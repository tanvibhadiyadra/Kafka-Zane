"""
app/services/tenant_resolver.py – Tenant Resolver

Maps:  installation_id (GitHub numeric string)  →  tenant_id (UUID string)

Lookup order (cache-aside):
  1. Redis  O(1) ~0.1 ms
  2. PostgreSQL  O(log n) ~5 ms
  3. Auto-onboard if still not found (graceful recovery for missed install events)
  4. Cache result in Redis for subsequent calls

The webhook process does its own onboarding on the `installation.created` event.
This resolver is used exclusively by the Kafka worker and is synchronous
(confluent-kafka consumer runs sync).

Merging notes:
  • backend project:    TenantResolver(redis, pg) with auto-onboard
  • queryguard-async:   TenantResolver(db, redis) without auto-onboard
  → Merged version keeps auto-onboard as a safety net (same as backend project)
    but uses the queryguard-async cache helpers and SQLAlchemy DB session.
"""

import logging
import uuid
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db.redis_client import (
    get_cached_tenant_id,
    set_cached_tenant_id,
    invalidate_tenant_cache,
)

logger = logging.getLogger("services.tenant_resolver")


class TenantResolutionError(Exception):
    """Raised when a tenant cannot be resolved (non-retryable)."""


class TenantResolver:
    """
    Resolve a GitHub App installation_id to a tenant_id.

    Usage (inside the Kafka consumer):
        resolver = TenantResolver(db=db_session)
        tenant_id = resolver.resolve("12345678")
    """

    def __init__(self, db: Session) -> None:
        self._db = db

    def resolve(self, installation_id: str) -> Optional[str]:
        """
        Return tenant_id for the given installation_id.
        Returns None only if the installation is genuinely unknown and
        auto-onboarding is also unavailable (rare).
        """
        if not installation_id:
            logger.warning("resolve() called with empty installation_id")
            return None

        logger.info("Resolving tenant | installation_id=%s", installation_id)

        # ── 1. Redis cache ────────────────────────────────────────────────────
        tenant_id = get_cached_tenant_id(installation_id)
        if tenant_id:
            if not self._is_uuid(tenant_id):
                logger.warning(
                    "Tenant ID in cache is not UUID; invalidating cache | installation_id=%s tenant_id=%s",
                    installation_id, tenant_id,
                )
                invalidate_tenant_cache(installation_id)
                tenant_id = None
            else:
                logger.info(
                    "Tenant resolved from CACHE | installation_id=%s tenant_id=%s",
                    installation_id, tenant_id,
                )
                return tenant_id

        # ── 2. PostgreSQL ─────────────────────────────────────────────────────
        tenant_id = self._query_db(installation_id)
        if tenant_id:
            if not self._is_uuid(tenant_id):
                logger.warning(
                    "Tenant ID in DB is not UUID; repairing via auto-onboard | installation_id=%s tenant_id=%s",
                    installation_id, tenant_id,
                )
                tenant_id = self._auto_onboard(installation_id)
                if not tenant_id:
                    return None
            logger.info(
                "Tenant resolved from DB | installation_id=%s tenant_id=%s",
                installation_id, tenant_id,
            )
            set_cached_tenant_id(installation_id, tenant_id)
            return tenant_id

        # ── 3. Auto-onboard (missed installation.created event) ───────────────
        logger.warning(
            "Tenant NOT FOUND – auto-onboarding | installation_id=%s", installation_id
        )
        tenant_id = self._auto_onboard(installation_id)
        if tenant_id:
            set_cached_tenant_id(installation_id, tenant_id)
            logger.info(
                "Tenant auto-onboarded | installation_id=%s tenant_id=%s",
                installation_id, tenant_id,
            )
            return tenant_id

        logger.error(
            "Tenant resolution completely failed | installation_id=%s", installation_id
        )
        return None

    # ── Private helpers ───────────────────────────────────────────────────────

    def _query_db(self, installation_id: str) -> Optional[str]:
        """Query github_installations for an active installation."""
        try:
            row = self._db.execute(
                text(
                    """
                    SELECT tenant_id::TEXT
                    FROM   github_installations
                    WHERE  installation_id = :iid
                      AND  is_active = TRUE
                    LIMIT 1
                    """
                ),
                {"iid": installation_id},
            ).fetchone()
            return row[0] if row else None
        except Exception as exc:
            logger.error(
                "DB query failed in TenantResolver | installation_id=%s err=%s",
                installation_id, exc,
            )
            raise   # retryable

    def _auto_onboard(self, installation_id: str) -> Optional[str]:
        """
        Insert a minimal tenant + installation record so processing can continue.
        This is a safety net – real onboarding happens via the webhook handler.
        """
        # Use deterministic UUID so repeated auto-onboard is stable/idempotent.
        tenant_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"queryguard:{installation_id}")
        tenant_id = str(tenant_uuid)
        schema_name = f"schema_{installation_id}"
        try:
            self._db.execute(
                text(
                    """
                    INSERT INTO tenants
                        (id, name, plan, is_active)
                    VALUES
                        (CAST(:tenant_id AS UUID), :name, 'free', TRUE)
                    ON CONFLICT (id) DO UPDATE
                        SET is_active = TRUE, updated_at = NOW()
                    """
                ),
                {
                    "tenant_id": tenant_id,
                    "name": f"tenant_{installation_id}",
                },
            )
            self._db.execute(
                text(
                    """
                    INSERT INTO github_installations
                        (installation_id, tenant_id, schema_name, is_active)
                    VALUES (:iid, :tid, :schema, TRUE)
                    ON CONFLICT (installation_id) DO UPDATE
                        SET tenant_id = EXCLUDED.tenant_id,
                            schema_name = EXCLUDED.schema_name,
                            is_active = TRUE,
                            updated_at = NOW()
                    """
                ),
                {
                    "iid": installation_id,
                    "tid": tenant_id,
                    "schema": schema_name,
                },
            )
            self._db.commit()
            return tenant_id
        except Exception as exc:
            logger.error(
                "Auto-onboard failed | installation_id=%s err=%s", installation_id, exc
            )
            self._db.rollback()
            return None

    @staticmethod
    def _is_uuid(value: str) -> bool:
        try:
            uuid.UUID(str(value))
            return True
        except Exception:
            return False
