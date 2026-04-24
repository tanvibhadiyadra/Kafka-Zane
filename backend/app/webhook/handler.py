"""
app/webhook/handler.py – GitHub Webhook Receiver

RESPONSIBILITIES (and nothing more):
  1. Validate GitHub's HMAC-SHA256 signature          → reject fakes instantly
  2. Parse raw JSON payload
  3. Handle installation.created                      → onboard tenant (rare, fast)
  4. Handle pull_request events                       → publish envelope to Kafka
  5. Return 202 Accepted to GitHub within < 2 seconds

WHAT THIS FILE MUST NEVER DO:
  • Run impact analysis
  • Make external HTTP requests (except indirectly via onboarding)
  • Block on heavy I/O in the hot path

Merging notes:
  • backend project:      full signature check, installation.created onboarding,
                          publish to Kafka via aiokafka KafkaProducerService
  • queryguard-async:     signature check (commented out in dev), detailed
                          event envelope with PR metadata, confluent-kafka publish
  → We keep the backend's async DB onboarding on installation.created AND
    the queryguard-async's richer event envelope and confluent-kafka producer.
"""

import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from app.config import settings
from app.kafka.producer import publish_event

logger = logging.getLogger("webhook")
router = APIRouter()


# ── Signature Verification ────────────────────────────────────────────────────

def _verify_github_signature(body: bytes, signature_header: str | None) -> None:
    """
    Raise HTTP 401 if the HMAC-SHA256 signature is missing or invalid.
    Skipped when VERIFY_GITHUB_SIGNATURE=false (local dev only).
    """
    if not settings.VERIFY_GITHUB_SIGNATURE:
        return

    if not settings.GITHUB_WEBHOOK_SECRET:
        logger.error("GITHUB_WEBHOOK_SECRET is not set – rejecting all webhooks")
        raise HTTPException(status_code=401, detail="webhook_secret_not_configured")

    if not signature_header or not signature_header.startswith("sha256="):
        raise HTTPException(status_code=401, detail="missing_signature")

    received = signature_header[len("sha256="):]
    expected = hmac.new(
        key=settings.GITHUB_WEBHOOK_SECRET.encode("utf-8"),
        msg=body,
        digestmod=hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(expected, received):
        raise HTTPException(status_code=401, detail="bad_signature")


# ── Webhook Endpoint ──────────────────────────────────────────────────────────

@router.post("")
async def github_webhook(
    request: Request,
    x_hub_signature_256: str | None = Header(default=None, alias="X-Hub-Signature-256"),
    x_github_event: str | None = Header(default=None, alias="X-GitHub-Event"),
    x_github_delivery: str | None = Header(default=None, alias="X-GitHub-Delivery"),
):
    """
    Single endpoint for all GitHub App webhook events.

    GitHub App → POST /webhook → validate sig → route event → 202 Accepted
    """
    raw_body = await request.body()

    # ── 1. Validate signature ─────────────────────────────────────────────────
    _verify_github_signature(raw_body, x_hub_signature_256)

    # ── 2. Parse payload ──────────────────────────────────────────────────────
    try:
        payload: dict = json.loads(raw_body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.warning("Webhook rejected – malformed JSON: %s", exc)
        raise HTTPException(status_code=400, detail="invalid_json_payload")

    action: str = payload.get("action", "")
    installation_data: dict = payload.get("installation") or {}
    installation_id = installation_data.get("id")

    logger.info(
        "Webhook received | event=%s action=%s delivery=%s installation_id=%s",
        x_github_event, action, x_github_delivery, installation_id,
    )

    # ── 3. Handle installation.created (onboard new tenant) ───────────────────
    if x_github_event == "installation" and action == "created":
        return await _handle_installation_created(
            request, installation_data, x_github_delivery
        )

    # ── 4. Handle pull_request events ─────────────────────────────────────────
    if x_github_event == "pull_request":
        return await _handle_pull_request(
            request, payload, installation_id, x_github_event, x_github_delivery
        )

    # ── 5. Ignore everything else ─────────────────────────────────────────────
    logger.info("Ignoring event: %s / %s", x_github_event, action)
    return JSONResponse(
        status_code=202,
        content={"status": "ignored", "reason": f"event '{x_github_event}' not handled"},
    )


# ── Installation Onboarding ───────────────────────────────────────────────────

async def _handle_installation_created(
    request: Request,
    installation_data: dict,
    delivery_id: str | None,
) -> JSONResponse:
    """
    Create a tenant record and per-tenant schema when a GitHub App is installed.
    Uses the asyncpg PostgresService attached to app.state.
    """
    installation_id = installation_data.get("id")
    account = installation_data.get("account") or {}
    account_login: str | None = account.get("login")
    account_type: str | None = account.get("type")

    if not isinstance(installation_id, int):
        raise HTTPException(status_code=400, detail="invalid_installation_id")

    tenant_id = f"tenant_{installation_id}"
    schema_name = f"schema_{installation_id}"

    pg = request.app.state.pg
    await pg.onboard_installation(
        installation_id=installation_id,
        tenant_id=tenant_id,
        schema_name=schema_name,
        account_login=account_login,
        account_type=account_type,
    )

    logger.info(
        "Tenant onboarded | installation_id=%s tenant_id=%s schema=%s",
        installation_id, tenant_id, schema_name,
    )
    return JSONResponse(
        status_code=202,
        content={
            "status": "tenant_created",
            "installation_id": installation_id,
            "tenant_id": tenant_id,
        },
    )


# ── Pull Request Event ────────────────────────────────────────────────────────

async def _handle_pull_request(
    request: Request,
    payload: dict,
    installation_id,
    event_name: str | None,
    delivery_id: str | None,
) -> JSONResponse:
    """
    Validate and enqueue a pull_request event to Kafka.
    Only processes: opened, reopened, synchronize.
    """
    action: str = payload.get("action", "")

    if action not in {"opened", "reopened", "synchronize"}:
        logger.info("Ignoring PR action: %s", action)
        return JSONResponse(
            status_code=202,
            content={"status": "ignored", "reason": f"PR action '{action}' not handled"},
        )

    pr_data: dict = payload.get("pull_request") or {}
    repo_data: dict = payload.get("repository") or {}
    pr_number: int = pr_data.get("number", 0)

    if not installation_id or not pr_number:
        logger.warning(
            "Missing required fields | installation_id=%s pr_number=%s",
            installation_id, pr_number,
        )
        return JSONResponse(
            status_code=202,
            content={"status": "ignored", "reason": "missing installation_id or pr_number"},
        )

    # ── Build event envelope ──────────────────────────────────────────────────
    # Keep envelope small. Worker fetches full PR details from GitHub API.
    # Include enough for: tenant resolution, PR identification, deduplication.
    str_installation_id = str(installation_id)
    idempotency_key = f"{str_installation_id}:{pr_number}:{delivery_id}"

    envelope = {
        # Routing / deduplication
        "delivery_id": delivery_id,
        "event_type": event_name,
        "action": action,
        "received_at": datetime.now(timezone.utc).isoformat(),

        # Tenant resolution
        "installation_id": str_installation_id,

        # PR identification
        "pr_number": pr_number,
        "pr_title": pr_data.get("title"),
        "pr_url": pr_data.get("html_url"),
        "branch_name": pr_data.get("head", {}).get("ref"),
        "author_name": pr_data.get("user", {}).get("login"),
        "repo_full_name": repo_data.get("full_name"),

        # Idempotency key – prevents double-processing the same PR push
        "idempotency_key": idempotency_key,
    }

    # ── Publish to Kafka ──────────────────────────────────────────────────────
    # publish_event() is non-blocking (confluent-kafka produce + poll(0))
    # This keeps the GitHub webhook response well under 2 seconds.
    publish_event(
        topic=settings.KAFKA_TOPIC_GITHUB_EVENTS,
        key=str_installation_id,        # partition by installation → same tenant in order
        value=envelope,
    )

    logger.info(
        "PR event published | topic=%s installation_id=%s pr_number=%s delivery_id=%s",
        settings.KAFKA_TOPIC_GITHUB_EVENTS,
        str_installation_id,
        pr_number,
        delivery_id,
    )

    return JSONResponse(
        status_code=202,
        content={
            "status": "queued",
            "installation_id": str_installation_id,
            "pr_number": pr_number,
            "delivery_id": delivery_id,
        },
    )
