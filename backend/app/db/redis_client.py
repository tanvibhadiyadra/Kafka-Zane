"""
app/db/redis_client.py – Unified Redis Layer

Two interfaces live here, mirroring the dual DB approach:

1. RedisService  (async, redis.asyncio)
   ──────────────────────────────────────
   Used by the FastAPI webhook process and optionally by the async consumer.
   Supports tenant-mapping cache, idempotency keys.

2. Synchronous helpers  (redis.Redis)
   ──────────────────────────────────────
   get_cached_tenant_id() / set_cached_tenant_id() / invalidate_tenant_cache()
   Used by the Kafka worker (confluent-kafka consumer runs sync).

Cache schema
  inst:{installation_id}:tenant_mapping  → JSON {tenant_id, schema_name}
  inst:{installation_id}                 → tenant_id string  (worker fast path)
  idempotency:delivery:{delivery_id}     → "1"  (TTL = IDEMPOTENCY_TTL_SECONDS)
"""

import json
import logging
from typing import Optional

import redis
import redis.asyncio as aioredis

from app.config import settings

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Async  –  RedisService   (webhook / FastAPI)
# ─────────────────────────────────────────────────────────────────────────────

class RedisService:
    """Async Redis wrapper for the FastAPI webhook process."""

    def __init__(self) -> None:
        self._client: Optional[aioredis.Redis] = None

    async def connect(self) -> None:
        try:
            self._client = aioredis.from_url(
                settings.REDIS_URL, decode_responses=True
            )
            await self._client.ping()
            logger.info("Redis (async) connected | url=%s", settings.REDIS_URL)
        except Exception as exc:
            logger.warning("Redis not available (async): %s – continuing without cache", exc)
            if self._client:
                try:
                    await self._client.close()
                except Exception:
                    pass
            self._client = None

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None

    async def is_healthy(self) -> bool:
        if self._client is None:
            return False
        try:
            await self._client.ping()
            return True
        except Exception:
            logger.exception("redis_healthcheck_failed")
            return False

    # ── Tenant mapping (async) ────────────────────────────────────────────────

    @staticmethod
    def _mapping_key(installation_id: int | str) -> str:
        return f"inst:{installation_id}:tenant_mapping"

    async def get_tenant_mapping(
        self, installation_id: int | str
    ) -> Optional[tuple[str, str]]:
        if self._client is None:
            return None
        raw = await self._client.get(self._mapping_key(installation_id))
        if not raw:
            return None
        try:
            data = json.loads(raw)
            tid = data.get("tenant_id")
            sname = data.get("schema_name")
            if tid and sname:
                return tid, sname
        except (json.JSONDecodeError, AttributeError):
            logger.warning(
                "invalid_tenant_mapping_cache installation_id=%s", installation_id
            )
        return None

    async def set_tenant_mapping(
        self,
        installation_id: int | str,
        tenant_id: str,
        schema_name: str,
        ttl_seconds: int = 3600,
    ) -> None:
        if self._client is None:
            return
        payload = json.dumps({"tenant_id": tenant_id, "schema_name": schema_name})
        await self._client.set(
            self._mapping_key(installation_id), payload, ex=ttl_seconds
        )

    # ── Idempotency (async) ───────────────────────────────────────────────────

    async def is_processed(self, key: str) -> bool:
        if self._client is None:
            return False
        return await self._client.get(key) is not None

    async def mark_processed(self, key: str, ttl_seconds: int) -> None:
        if self._client is None:
            return
        await self._client.set(key, "1", ex=ttl_seconds)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Synchronous helpers  –  Kafka worker
# ─────────────────────────────────────────────────────────────────────────────

_sync_client: Optional[redis.Redis] = None


def _get_sync_client() -> redis.Redis:
    """Return the global synchronous Redis client, creating on first call."""
    global _sync_client
    if _sync_client is None:
        _sync_client = redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=2,
            retry_on_timeout=True,
        )
        _sync_client.ping()
        logger.info("Redis (sync) connected | url=%s", settings.REDIS_URL)
    return _sync_client


def _inst_key(installation_id: str) -> str:
    return f"inst:{installation_id}"


def get_cached_tenant_id(installation_id: str) -> Optional[str]:
    """
    O(1) Redis look-up: installation_id → tenant_id.
    Returns None on miss or if Redis is unavailable (fail-open).
    """
    try:
        client = _get_sync_client()
        value = client.get(_inst_key(installation_id))
        if value:
            logger.debug(
                "Redis CACHE HIT | installation_id=%s tenant_id=%s",
                installation_id, value,
            )
        else:
            logger.debug("Redis CACHE MISS | installation_id=%s", installation_id)
        return value
    except redis.RedisError as exc:
        logger.warning("Redis get failed – falling back to DB | err=%s", exc)
        return None


def set_cached_tenant_id(installation_id: str, tenant_id: str) -> None:
    """Cache installation_id → tenant_id with TTL. Fails silently."""
    try:
        client = _get_sync_client()
        client.setex(
            name=_inst_key(installation_id),
            time=settings.REDIS_TENANT_TTL_SECONDS,
            value=tenant_id,
        )
        logger.debug(
            "Redis SET | installation_id=%s tenant_id=%s ttl=%ds",
            installation_id, tenant_id, settings.REDIS_TENANT_TTL_SECONDS,
        )
    except redis.RedisError as exc:
        logger.warning("Redis set failed – continuing without cache | err=%s", exc)


def invalidate_tenant_cache(installation_id: str) -> None:
    """Delete cached tenant mapping. Call when an installation is removed."""
    try:
        client = _get_sync_client()
        deleted = client.delete(_inst_key(installation_id))
        logger.info(
            "Redis cache invalidated | installation_id=%s deleted=%s",
            installation_id, deleted,
        )
    except redis.RedisError as exc:
        logger.warning("Redis delete failed | err=%s", exc)


def sync_health_check() -> bool:
    """Return True if Redis is reachable (sync)."""
    try:
        _get_sync_client().ping()
        return True
    except Exception:
        return False
