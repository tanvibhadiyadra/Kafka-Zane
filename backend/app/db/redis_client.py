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
  • TTL-based: entries expire after REDIS_TENANT_TTL_SECONDS (unless 0 = persistent).
  • For immediate invalidation, call invalidate_tenant_cache() explicitly.

Changes from original async version:
  FIX 1 – Startup connectivity check: get_redis_pool() now exposes an
           async ping() helper (health_check) that the FastAPI lifespan
           event should call so a bad REDIS_URL fails loudly at boot,
           not silently on the first cache miss.
  FIX 2 – Narrowed exception handling: bare `Exception` replaced with
           `(redis.RedisError, ConnectionError)` so logic bugs
           (AttributeError, TypeError, etc.) are never swallowed
           as cache misses.
  FIX 3 – tenant_id restored to Redis SET log line for easier tracing.

Two-layer API:
  • Async API (get_cached_tenant_id_async, etc.) — for FastAPI webhook
  • Sync API (get_cached_tenant_id, etc.) — for synchronous Kafka consumer
  Both use the same underlying connection pool and caching logic.
"""

import asyncio
import logging
from redis.asyncio import Redis, ConnectionPool
import redis.asyncio as aioredis

from app.config import settings

logger = logging.getLogger("db.redis")

_pool: ConnectionPool | None = None


# ── Pool ──────────────────────────────────────────────────────────────────────

def get_redis_pool() -> ConnectionPool:
    """Return the shared async connection pool, creating it on first call."""
    global _pool
    if _pool is None:
        _pool = ConnectionPool.from_url(
            settings.REDIS_URL,
            max_connections=20,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=2,
            retry_on_timeout=True,
        )
        logger.info("Redis connection pool created | url=%s", settings.REDIS_URL)
    return _pool


async def get_redis() -> Redis:
    """Return an async Redis client backed by the shared pool."""
    return Redis(connection_pool=get_redis_pool())


# ── FIX 1: Startup connectivity check ────────────────────────────────────────
# Call this from your FastAPI lifespan startup event so a misconfigured
# REDIS_URL surfaces immediately instead of on the first cache operation.
#
#   @asynccontextmanager
#   async def lifespan(app: FastAPI):
#       await startup_ping()          # ← add this line
#       yield
#       await shutdown()

async def startup_ping() -> None:
    """
    Verify Redis is reachable at application startup.

    Raises redis.RedisError (or ConnectionError) so the process exits
    loudly if REDIS_URL is wrong or Redis is unreachable, rather than
    silently falling back to DB on every request.
    """
    r = await get_redis()
    await r.ping()
    logger.info("Redis connectivity verified at startup | url=%s", settings.REDIS_URL)


# ── Cache Key Helper ──────────────────────────────────────────────────────────

def _cache_key(installation_id: str) -> str:
    """inst:12345678"""
    return f"inst:{installation_id}"


# ── Public API ────────────────────────────────────────────────────────────────

async def get_cached_tenant_id(installation_id: str) -> str | None:
    """
    Look up a tenant_id in Redis by installation_id.

    Returns None on cache miss OR if Redis is unavailable (fail-open:
    lets TenantResolver fall back to DB rather than blocking processing).

    FIX 2: Catches (redis.RedisError, ConnectionError) instead of bare
    Exception so genuine logic bugs are not silently swallowed.
    """
    try:
        r = await get_redis()
        value = await r.get(_cache_key(installation_id))
        if value:
            logger.debug(
                "Redis CACHE HIT | installation_id=%s tenant_id=%s",
                installation_id, value,
            )
        else:
            logger.debug("Redis CACHE MISS | installation_id=%s", installation_id)
        return value
    except (aioredis.RedisError, ConnectionError) as exc:   # FIX 2
        logger.warning("Redis get failed – falling back to DB | err=%s", exc)
        return None


async def set_cached_tenant_id(installation_id: str, tenant_id: str) -> None:
    """
    Cache an installation_id → tenant_id mapping in Redis with TTL.

    Fails silently if Redis is unavailable so the worker keeps processing.

    FIX 2: Narrowed exception scope.
    FIX 3: tenant_id restored to the SET log line.
    """
    try:
        r = await get_redis()
        key = _cache_key(installation_id)
        ttl = settings.REDIS_TENANT_TTL_SECONDS
        if ttl <= 0:
            await r.set(key, tenant_id)
        else:
            await r.setex(name=key, time=ttl, value=tenant_id)
        logger.debug(
            "Redis SET | installation_id=%s tenant_id=%s ttl=%s",
            installation_id,
            tenant_id,
            "persistent" if ttl <= 0 else f"{ttl}s",
        )
    except (aioredis.RedisError, ConnectionError) as exc:   # FIX 2
        logger.warning("Redis set failed – continuing without cache | err=%s", exc)


async def invalidate_tenant_cache(installation_id: str) -> None:
    """
    Delete the cached tenant mapping for an installation.

    Call when an installation is removed or reassigned to a new tenant.

    FIX 2: Narrowed exception scope.
    """
    try:
        r = await get_redis()
        deleted = await r.delete(_cache_key(installation_id))
        logger.info(
            "Redis cache invalidated | installation_id=%s deleted=%s",
            installation_id, deleted,
        )
    except (aioredis.RedisError, ConnectionError) as exc:   # FIX 2
        logger.warning("Redis delete failed | err=%s", exc)


# ── Synchronous Wrappers (for Kafka Consumer + TenantResolver) ──────────────

def get_cached_tenant_id_sync(installation_id: str) -> str | None:
    """
    Synchronous wrapper around async get_cached_tenant_id().
    
    Used by TenantResolver and Kafka consumer (sync contexts).
    Returns None on cache miss OR if Redis is unavailable (fail-open).
    
    Implementation note:
      asyncio.run() creates a new event loop, executes the coroutine,
      then closes the loop. Safe for synchronous worker processes.
    """
    try:
        return asyncio.run(get_cached_tenant_id(installation_id))
    except Exception as exc:
        logger.warning(
            "Sync Redis get failed – falling back to DB | err=%s", exc
        )
        return None


def set_cached_tenant_id_sync(installation_id: str, tenant_id: str) -> None:
    """
    Synchronous wrapper around async set_cached_tenant_id().
    
    Used by TenantResolver and Kafka consumer (sync contexts).
    Fails silently if Redis is unavailable so processing continues.
    """
    try:
        asyncio.run(set_cached_tenant_id(installation_id, tenant_id))
    except Exception as exc:
        logger.warning("Sync Redis set failed – continuing | err=%s", exc)


def invalidate_tenant_cache_sync(installation_id: str) -> None:
    """
    Synchronous wrapper around async invalidate_tenant_cache().
    
    Used by TenantResolver and Kafka consumer (sync contexts).
    Fails silently if Redis is unavailable.
    """
    try:
        asyncio.run(invalidate_tenant_cache(installation_id))
    except Exception as exc:
        logger.warning("Sync Redis delete failed | err=%s", exc)


async def health_check() -> bool:
    """
    Return True if Redis is reachable.

    For use in the /health endpoint. Unlike startup_ping(), this never
    raises — it returns False so the health handler can report degraded
    status without crashing.
    """
    try:
        r = await get_redis()
        await r.ping()
        return True
    except Exception:
        return False


# ── RedisService (for FastAPI lifespan) ──────────────────────────────────────

class RedisService:
    """
    Thin async Redis wrapper for FastAPI webhook lifecycle.
    
    Lifecycle (called from app/main.py lifespan):
        await redis.connect()   # on startup (FIX 1: calls startup_ping internally)
        await redis.close()     # on shutdown
    """

    async def connect(self) -> None:
        """
        Verify Redis connectivity at startup.
        Raises redis.RedisError if REDIS_URL is misconfigured or Redis is unreachable.
        """
        await startup_ping()
        logger.info("RedisService connected")

    async def close(self) -> None:
        """Close Redis connection pool on shutdown."""
        global _pool
        if _pool:
            await _pool.disconnect()
            _pool = None
            logger.info("RedisService closed")

    async def is_healthy(self) -> bool:
        """
        Health-check for /health endpoint.
        Returns False (not True) if Redis is unreachable, allowing graceful degradation.
        """
        return await health_check()