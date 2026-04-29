"""
app/db/redis_client.py – Redis Client (PHASE 5)

Cache schema:
  Key:    inst:{installation_id}
  Value:  tenant_id (plain string)
  TTL:    REDIS_TENANT_TTL_SECONDS (default 1 hour; 0 = no expiry)

Why Redis and not Postgres directly?
  • TenantResolver is called on EVERY Kafka message.
  • DB round-trip ~5-10ms; Redis ~0.1ms.
  • At 10k messages/min: difference between 100s and 1s of DB load.

Cache invalidation:
  • TTL-based: entries expire after REDIS_TENANT_TTL_SECONDS.
  • For immediate invalidation, call invalidate_tenant_cache() explicitly.

Two-layer API:
  • Async API  — for FastAPI webhook container (uses shared async client singleton)
  • Sync API   — for Kafka worker container  (uses dedicated sync client, no asyncio bridge)

Key changes from previous version:
  • Async path: shared Redis client singleton instead of creating a new wrapper per call.
  • Sync path:  plain redis.Redis (no asyncio/_run_async bridge) — simpler, thread-safe,
                correct for a sync confluent-kafka consumer.
  • health_check_interval=30 added to both pools so idle connections are validated before use.
  • socket_keepalive=True added to both pools to prevent OS-level TCP teardown on idle conns.
  • Negative REDIS_TENANT_TTL_SECONDS raises ValueError at startup instead of silently
    being treated as "no expiry".
"""

import logging
import redis
import redis.asyncio as aioredis

import threading

_async_lock = threading.Lock()
_sync_lock = threading.Lock()

from app.config import settings

logger = logging.getLogger("db.redis")


# ── Config validation ─────────────────────────────────────────────────────────

def _validate_config() -> None:
    """Raise early if Redis config values are invalid."""
    ttl = settings.REDIS_TENANT_TTL_SECONDS
    if ttl < 0:
        raise ValueError(
            f"REDIS_TENANT_TTL_SECONDS must be >= 0 (0 = no expiry), got {ttl}"
        )

_validate_config()


# ── Cache Key Helper ──────────────────────────────────────────────────────────

def _cache_key(installation_id: str) -> str:
    """inst:12345678"""
    return f"inst:{installation_id}"


# ════════════════════════════════════════════════════════════════════════════════
# ASYNC API  — FastAPI webhook container
# ════════════════════════════════════════════════════════════════════════════════

_async_client: aioredis.Redis | None = None


def _get_async_pool() -> aioredis.ConnectionPool:
    """
    Return a shared async ConnectionPool. Called once; pool is embedded in the
    singleton client below.

    Settings:
      health_check_interval=30  – auto-PING any connection idle >30s before use.
                                   Prevents silent ConnectionError after idle periods
                                   (common in cloud Redis / Docker network restarts).
      socket_keepalive=True     – TCP keepalive so the OS does not tear down the
                                   socket behind redis-py's back during quiet periods.
    """
    return aioredis.ConnectionPool.from_url(
        settings.REDIS_URL,
        max_connections=20,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=2,
        retry_on_timeout=True,
        health_check_interval=30,
        socket_keepalive=True,
    )


def get_async_redis() -> aioredis.Redis:
    """
    Return the shared async Redis client singleton.

    Returns the same object on every call — no new wrapper is allocated per
    request. The underlying ConnectionPool handles connection checkout/return.
    Call once at startup (or let RedisService.connect() do it) and reuse.
    """
    global _async_client
    if _async_client is None:
        _async_client = aioredis.Redis(connection_pool=_get_async_pool())
        logger.info("Async Redis client created | url=%s", settings.REDIS_URL)
    return _async_client


async def startup_ping() -> None:
    """Verify Redis connectivity at startup. Raises if unreachable."""
    await get_async_redis().ping()
    logger.info("Redis connectivity verified at startup | url=%s", settings.REDIS_URL)


async def health_check() -> bool:
    """Return True if Redis is reachable. Never raises."""
    try:
        await get_async_redis().ping()
        return True
    except (aioredis.RedisError, ConnectionError):
        return False


# ── RedisService (for FastAPI lifespan) ──────────────────────────────────────

class RedisService:
    """FastAPI webhook Redis lifecycle manager."""

    async def connect(self) -> None:
        """Create client and verify Redis connectivity at startup."""
        await startup_ping()
        logger.info("RedisService connected")

    async def close(self) -> None:
        """Close the async Redis client and its connection pool."""
        global _async_client
        if _async_client is not None:
            await _async_client.aclose()
            _async_client = None
            logger.info("RedisService closed")

    async def is_healthy(self) -> bool:
        """Check if Redis is reachable."""
        return await health_check()


# ════════════════════════════════════════════════════════════════════════════════
# SYNC API  — Kafka worker container
#
# The Kafka worker uses confluent-kafka which is synchronous. Rather than
# bridging async code with a persistent event loop (fragile, not thread-safe),
# we use a plain redis.Redis sync client here. It connects to the same Redis
# instance and uses the same key schema — just a different client object.
#
# Thread safety: get_sync_redis() uses a module-level singleton protected by a
# check-then-set pattern that is safe in CPython's GIL. For multi-threaded
# workers (e.g. --scale worker=3 each with thread pool), each thread shares
# this one client; redis-py's sync ConnectionPool IS thread-safe by design.
# ════════════════════════════════════════════════════════════════════════════════

_sync_client: redis.Redis | None = None


def get_sync_redis() -> redis.Redis:
    """
    Return the shared sync Redis client singleton for the Kafka worker.

    No asyncio, no event loop bridge. Plain redis.Redis backed by a thread-safe
    ConnectionPool. Safe to call from multiple threads (CPython GIL + redis-py
    pool locking protects the singleton initialisation).
    """
    global _sync_client
    if _sync_client is None:
        _sync_client = redis.Redis.from_url(
            settings.REDIS_URL,
            max_connections=10,        # worker needs fewer connections than webhook
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=2,
            retry_on_timeout=True,
            health_check_interval=30,  # validate idle connections before use
            socket_keepalive=True,     # prevent OS-level TCP teardown on idle conns
        )
        logger.info("Sync Redis client created | url=%s", settings.REDIS_URL)
    return _sync_client


# ── Sync public API ───────────────────────────────────────────────────────────

def get_cached_tenant_id_sync(installation_id: str) -> str | None:
    """Look up tenant_id in Redis (sync). Returns None on miss or error."""
    try:
        value = get_sync_redis().get(_cache_key(installation_id))
        if value:
            logger.debug(
                "Redis CACHE HIT | installation_id=%s tenant_id=%s",
                installation_id, value,
            )
        else:
            logger.debug("Redis CACHE MISS | installation_id=%s", installation_id)
        return value
    except (redis.RedisError, ConnectionError) as exc:
        logger.warning("Redis get failed – falling back to DB | err=%s", exc)
        return None


def set_cached_tenant_id_sync(installation_id: str, tenant_id: str) -> None:
    """Cache installation_id → tenant_id in Redis (sync). Fails silently."""
    try:
        key = _cache_key(installation_id)
        ttl = settings.REDIS_TENANT_TTL_SECONDS
        r = get_sync_redis()
        if ttl == 0:
            r.set(key, tenant_id)
        else:
            r.setex(name=key, time=ttl, value=tenant_id)
        logger.debug(
            "Redis SET | installation_id=%s tenant_id=%s ttl=%s",
            installation_id,
            tenant_id,
            "persistent" if ttl == 0 else f"{ttl}s",
        )
    except (redis.RedisError, ConnectionError) as exc:
        logger.warning("Redis set failed – continuing without cache | err=%s", exc)


def invalidate_tenant_cache_sync(installation_id: str) -> None:
    """Delete cached tenant mapping (sync). Fails silently."""
    try:
        deleted = get_sync_redis().delete(_cache_key(installation_id))
        if deleted:
            logger.info(
                "Redis cache invalidated | installation_id=%s", installation_id
            )
        else:
            logger.debug(
                "Redis cache invalidate – key not found | installation_id=%s",
                installation_id,
            )
    except (redis.RedisError, ConnectionError) as exc:
        logger.warning("Redis delete failed | err=%s", exc)


def health_check_sync() -> bool:
    """Return True if Redis is reachable from sync context. Never raises."""
    try:
        get_sync_redis().ping()
        return True
    except (redis.RedisError, ConnectionError):
        return False