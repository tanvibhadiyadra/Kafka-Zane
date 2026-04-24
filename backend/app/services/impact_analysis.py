"""
app/services/impact_analysis.py – Impact Analysis Service

Runs inside the Kafka consumer worker (NOT the webhook service).

Full pipeline per PR event:
  1. Create pr_analyses row (status='processing', idempotency_key)
  2. Fetch SQL file diffs from GitHub via PyGithub (installation token)
  3. Analyse each file  ← plug your real schema_detection_rag / dbt logic here
  4. Post formatted comment to GitHub PR
  5. Persist results + update status='done'

Tenant isolation:
  • tenant_id is injected at construction
  • All DB writes are scoped to tenant_id
  • GitHub token is scoped to the installation (GitHub enforces repo access)

Merging notes:
  • backend project:      run_impact_analysis() function stub (asyncpg-based)
  • queryguard-async:     ImpactAnalysisService class (full pipeline, SQLAlchemy)
  → We keep the full class from queryguard-async (it's the production-ready version)
    and wire it into the consumer from app/kafka/consumer.py.
    The asyncpg-based stub in the backend project is no longer needed.
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import text

from app.config import settings

logger = logging.getLogger("services.impact_analysis")


class ImpactAnalysisService:
    """
    Runs impact analysis for a single PR event in a tenant-isolated context.

    Usage (inside the Kafka consumer):
        service = ImpactAnalysisService(db=db_session, tenant_id=tenant_id)
        service.run(event)
    """

    def __init__(self, db: Session, tenant_id: str):
        self._db = db
        self._tenant_id = tenant_id

    # ── Public Entry Point ────────────────────────────────────────────────────

    def run(self, event: dict) -> None:
        """
        Full impact analysis pipeline for one PR event.

        Args:
            event: Kafka event envelope produced by the webhook handler.
                   Required keys: installation_id, pr_number, repo_full_name,
                   idempotency_key, pr_title, branch_name, author_name, pr_url
        """
        installation_id: str = str(event["installation_id"])
        pr_number: int = event["pr_number"]
        repo_full_name: str = event.get("repo_full_name", "")
        idempotency_key: str = event.get("idempotency_key", "")

        logger.info(
            "ImpactAnalysis START | tenant_id=%s installation_id=%s pr=%s repo=%s",
            self._tenant_id, installation_id, pr_number, repo_full_name,
        )

        # ── Step 1: Create tracking record ────────────────────────────────────
        analysis_db_id = self._create_analysis_record(
            installation_id=installation_id,
            pr_number=pr_number,
            repo_full_name=repo_full_name,
            pr_title=event.get("pr_title"),
            pr_url=event.get("pr_url"),
            branch_name=event.get("branch_name"),
            author_name=event.get("author_name"),
            idempotency_key=idempotency_key,
        )

        try:
            # ── Step 2: Fetch PR SQL files from GitHub ────────────────────────
            sql_files = self._fetch_pr_sql_files(
                installation_id, repo_full_name, pr_number
            )

            if not sql_files:
                logger.info(
                    "No SQL files in PR | tenant_id=%s pr=%s",
                    self._tenant_id, pr_number,
                )
                self._update_analysis_status(
                    analysis_db_id, "done", results=[], total=0
                )
                return

            # ── Step 3: Analyse each file ─────────────────────────────────────
            results = []
            for file_info in sql_files:
                result = self._analyse_file(file_info, installation_id)
                results.append(result)

            # ── Step 4: Post GitHub comment ───────────────────────────────────
            self._post_github_comment(
                installation_id=installation_id,
                repo_full_name=repo_full_name,
                pr_number=pr_number,
                results=results,
                sql_files=sql_files,
            )

            # ── Step 5: Persist final results ─────────────────────────────────
            all_query_ids = {
                qid for r in results for qid in r.get("affected_query_ids", [])
            }
            self._update_analysis_status(
                analysis_db_id,
                status="done",
                results=results,
                total=len(all_query_ids),
            )

            logger.info(
                "ImpactAnalysis DONE | tenant_id=%s pr=%s files=%d impacted_queries=%d",
                self._tenant_id, pr_number, len(results), len(all_query_ids),
            )

        except Exception as exc:
            logger.exception(
                "ImpactAnalysis FAILED | tenant_id=%s pr=%s: %s",
                self._tenant_id, pr_number, exc,
            )
            self._update_analysis_status(
                analysis_db_id, "failed", results=[], total=0
            )
            raise   # re-raise so consumer can retry / DLQ

    # ── GitHub: Installation Token ────────────────────────────────────────────

    def _get_installation_token(self, installation_id: str) -> Optional[str]:
        """
        Get a short-lived GitHub App installation access token.
        Falls back to None in dev mode (GITHUB_APP_ID not set).
        """
        if not settings.GITHUB_APP_ID or not settings.GITHUB_PRIVATE_KEY:
            logger.warning(
                "GITHUB_APP_ID / GITHUB_PRIVATE_KEY not set – "
                "skipping GitHub API calls (dev mode)"
            )
            return None
        try:
            from github import GithubIntegration
            app_id = str(settings.GITHUB_APP_ID).strip()
            # Support both literal newlines and escaped "\n" from env files.
            private_key = settings.GITHUB_PRIVATE_KEY.replace("\\n", "\n").strip()
            integration = GithubIntegration(
                app_id,
                private_key,
            )
            return integration.get_access_token(int(installation_id)).token
        except Exception as exc:
            logger.error(
                "Failed to get GitHub token | installation_id=%s: %s",
                installation_id, exc,
            )
            return None

    # ── GitHub: Fetch PR SQL Files ────────────────────────────────────────────

    def _fetch_pr_sql_files(
        self, installation_id: str, repo_full_name: str, pr_number: int
    ) -> list[dict]:
        """
        Return a list of changed .sql files in the PR.
        Each entry: {filename, status, patch, additions, deletions}
        """
        token = self._get_installation_token(installation_id)
        if not token:
            logger.warning("No GitHub token – cannot fetch PR files (dev mode)")
            return []
        try:
            from github import Github
            gh = Github(login_or_token=token)
            repo = gh.get_repo(repo_full_name)
            pr = repo.get_pull(pr_number)

            sql_files = [
                {
                    "filename": f.filename,
                    "status": f.status,
                    "patch": f.patch or "",
                    "additions": f.additions,
                    "deletions": f.deletions,
                }
                for f in pr.get_files()
                if getattr(f, "patch", None) and f.filename.lower().endswith(".sql")
            ]

            logger.info(
                "Fetched %d SQL file(s) | tenant_id=%s pr=%s",
                len(sql_files), self._tenant_id, pr_number,
            )
            return sql_files

        except Exception as exc:
            logger.error("Failed to fetch PR files: %s", exc)
            raise

    # ── GitHub: Post Analysis Comment ─────────────────────────────────────────

    def _post_github_comment(
        self,
        installation_id: str,
        repo_full_name: str,
        pr_number: int,
        results: list[dict],
        sql_files: list[dict],
    ) -> None:
        """Format and post the analysis comment to the GitHub PR."""
        token = self._get_installation_token(installation_id)
        if not token:
            return
        try:
            from github import Github
            gh = Github(login_or_token=token)
            repo = gh.get_repo(repo_full_name)

            file_sections = []
            for idx, result in enumerate(results):
                f = sql_files[idx]
                query_ids = ", ".join(result.get("affected_query_ids", [])) or "None"
                file_sections.append(
                    f"\n<details>\n"
                    f"<summary>📂 **{f['filename']}** ({f['status']}) "
                    f"[+{f['additions']}/-{f['deletions']}]</summary>\n\n"
                    f"**Impact Report:**\n{result.get('impact_analysis', 'N/A')}\n\n"
                    f"**Affected Query IDs:** {query_ids}\n\n"
                    f"</details>"
                )

            comment = (
                f"## 🤖 **QueryGuard Impact Analysis**\n\n"
                f"Analysed **{len(results)}** SQL file(s) for downstream impact.\n\n"
                f"*Generated at "
                f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC*\n\n"
                f"---\n{''.join(file_sections)}"
            )
            repo.get_issue(number=pr_number).create_comment(comment)
            logger.info("GitHub comment posted | pr=%s", pr_number)

        except Exception as exc:
            logger.error("Failed to post GitHub comment: %s", exc)
            # Don't raise – a failed comment should not trigger a retry

    # ── Core Analysis Logic ───────────────────────────────────────────────────

    def _analyse_file(self, file_info: dict, installation_id: str) -> dict:
        """
        Run impact analysis on a single SQL file change.

        ── HOW TO PLUG IN YOUR EXISTING LOGIC ──────────────────────────────────
        Replace the stub below with your real analysis functions.

        Example:
            from app.services.impact_analysis_core import (
                schema_detection_rag, dbt_model_detection_rag, fetch_queries
            )
            if "models/" in file_info["filename"]:
                result = dbt_model_detection_rag(
                    analysis_input, file_info["filename"],
                    self._tenant_id, self._db
                )
            else:
                result = schema_detection_rag(analysis_input, self._tenant_id)
            regression_queries = fetch_queries(result.get("affected_query_ids", []))
        ─────────────────────────────────────────────────────────────────────────
        """
        filename: str = file_info["filename"]
        patch: str = file_info.get("patch", "")

        logger.info(
            "Analysing file | tenant_id=%s file=%s", self._tenant_id, filename
        )

        added_lines = "\n".join(
            line[1:]
            for line in patch.splitlines()
            if line.startswith("+") and not line.startswith("+++")
        )
        analysis_input = (
            f"File: {filename} ({file_info['status']}) "
            f"[+{file_info['additions']}/-{file_info['deletions']}]\n"
            f"New changes:\n{added_lines}"
        )

        # ── REPLACE THIS STUB with schema_detection_rag / dbt_model_detection_rag
        impact_report = (
            f"[STUB] Analysis for {filename}. "
            f"Replace _analyse_file() with your real RAG / LLM calls."
        )
        affected_query_ids: list[str] = []
        # ─────────────────────────────────────────────────────────────────────

        return {
            "filename": filename,
            "sql_change": analysis_input,
            "impact_analysis": impact_report,
            "affected_query_ids": affected_query_ids,
        }

    # ── DB Persistence ────────────────────────────────────────────────────────

    def _create_analysis_record(
        self,
        installation_id: str,
        pr_number: int,
        repo_full_name: str,
        pr_title: Optional[str],
        pr_url: Optional[str],
        branch_name: Optional[str],
        author_name: Optional[str],
        idempotency_key: str,
    ) -> Optional[str]:
        """Insert pr_analyses row with status='processing'. Returns the UUID."""
        try:
            row = self._db.execute(
                text(
                    """
                    INSERT INTO pr_analyses
                        (tenant_id, installation_id, repo_full_name, pr_number,
                         pr_title, pr_url, branch_name, author_name,
                         idempotency_key, status, created_at)
                    VALUES
                        (:tenant_id, :installation_id, :repo_full_name, :pr_number,
                         :pr_title, :pr_url, :branch_name, :author_name,
                         :idempotency_key, 'processing', NOW())
                    ON CONFLICT (idempotency_key) DO UPDATE
                        SET status = 'processing', updated_at = NOW()
                    RETURNING id::TEXT
                    """
                ),
                {
                    "tenant_id": self._tenant_id,
                    "installation_id": installation_id,
                    "repo_full_name": repo_full_name,
                    "pr_number": pr_number,
                    "pr_title": pr_title,
                    "pr_url": pr_url,
                    "branch_name": branch_name,
                    "author_name": author_name,
                    "idempotency_key": idempotency_key,
                },
            ).fetchone()
            self._db.commit()
            return row[0] if row else None
        except Exception as exc:
            logger.error("Failed to create analysis record: %s", exc)
            self._db.rollback()
            raise

    def _update_analysis_status(
        self,
        analysis_id: Optional[str],
        status: str,
        results: list,
        total: int,
    ) -> None:
        """Update pr_analyses row with final status and JSONB result data."""
        if not analysis_id:
            return
        try:
            self._db.execute(
                text(
                    """
                    UPDATE pr_analyses
                    SET    status                 = :status,
                           analysis_data          = CAST(:data AS JSONB),
                           total_impacted_queries = :total,
                           processed_at           = NOW(),
                           updated_at             = NOW()
                    WHERE  id = CAST(:id AS UUID)
                    """
                ),
                {
                    "status": status,
                    "data": json.dumps({"files": results}),
                    "total": total,
                    "id": analysis_id,
                },
            )
            self._db.commit()
        except Exception as exc:
            logger.error("Failed to update analysis status: %s", exc)
            self._db.rollback()
