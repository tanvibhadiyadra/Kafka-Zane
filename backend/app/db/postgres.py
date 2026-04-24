"""
app/db/postgres.py – Dual PostgreSQL Layer

Two complementary interfaces live here:

1. PostgresService  (asyncpg connection pool)
   ──────────────────────────────────────────
   Used by the FastAPI webhook service for:
     • Tenant onboarding  (CREATE SCHEMA, INSERT github_installations)
     • Fast health-check
   asyncpg is faster than SQLAlchemy for simple parameterised queries and
   doesn't require an ORM mapping.

2. get_db_session()  (SQLAlchemy 2 + psycopg v3 context manager)
   ───────────────────────────────────────────────────────────────
   Used by the Kafka worker for:
     • TenantResolver DB queries
     • ImpactAnalysisService persistence
   SQLAlchemy gives us typed ORM models, clean transaction management,
   and compatibility with the existing QueryGuard codebase.

Why both?
   The webhook process must stay DB-free for speed (< 2 s response).
   The sole exception is installation onboarding which is rare and must be
   transactional.  asyncpg handles that with minimal overhead.
   The worker is a long-running process where SQLAlchemy's connection pool
   and ORM conveniences are worth the slight overhead.
"""

import json
import logging
import re
from contextlib import contextmanager
from typing import Generator, Optional

# ── asyncpg (webhook / onboarding) ───────────────────────────────────────────
import asyncpg
from asyncpg import UndefinedColumnError

# ── SQLAlchemy (worker / impact analysis) ────────────────────────────────────
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.config import settings

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 1. asyncpg  –  PostgresService
# ─────────────────────────────────────────────────────────────────────────────

def _asyncpg_dsn() -> str:
    """
    asyncpg needs plain postgresql:// (no +psycopg dialect marker).
    Strip the SQLAlchemy dialect suffix that config.py normalises to.
    """
    url = settings.DATABASE_URL
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


class PostgresService:
    """
    Thin asyncpg wrapper used exclusively by the FastAPI webhook process.

    Lifecycle (called from app/main.py lifespan):
        await pg.connect()   # on startup
        await pg.close()     # on shutdown
    """

    def __init__(self) -> None:
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            _asyncpg_dsn(), min_size=1, max_size=10
        )
        logger.info("asyncpg pool connected")

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def is_healthy(self) -> bool:
        if self._pool is None:
            return False
        try:
            async with self._pool.acquire() as conn:
                await conn.execute("SELECT 1;")
            return True
        except Exception:
            logger.exception("postgres_healthcheck_failed")
            return False

    # ── Tenant Management ─────────────────────────────────────────────────────

    async def fetch_tenant_mapping_for_installation(
        self, installation_id: int | str
    ) -> Optional[tuple[str, str]]:
        """Return (tenant_id, schema_name) or None if not found."""
        assert self._pool is not None
        try:
            row = await self._pool.fetchrow(
                "SELECT tenant_id::TEXT, schema_name "
                "FROM github_installations WHERE installation_id = $1",
                str(installation_id),
            )
        except UndefinedColumnError as exc:
            logger.error("missing column in github_installations: %s", exc)
            raise
        if not row:
            return None
        return row["tenant_id"], row["schema_name"]

    async def onboard_installation(
        self,
        installation_id: int | str,
        tenant_id: str,
        schema_name: str,
        account_login: Optional[str] = None,
        account_type: Optional[str] = None,
    ) -> None:
        """
        Idempotently register a new GitHub App installation as a tenant.

        Creates:
          • Row in github_installations
          • Per-tenant PostgreSQL schema  (schema_{installation_id})
        """
        assert self._pool is not None
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema_name):
            raise ValueError(f"invalid schema_name: {schema_name!r}")

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Ensure the global installations table exists
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS github_installations (
                        id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
                        installation_id TEXT        NOT NULL UNIQUE,
                        tenant_id       TEXT        NOT NULL,
                        schema_name     TEXT        NOT NULL,
                        account_login   TEXT,
                        account_type    TEXT,
                        is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
                        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at      TIMESTAMPTZ
                    );
                    """
                )
                # Create per-tenant schema
                await conn.execute(
                    f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'
                )
                # Upsert installation record
                await conn.execute(
                    """
                    INSERT INTO github_installations
                        (installation_id, tenant_id, schema_name, account_login, account_type)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (installation_id) DO UPDATE
                        SET tenant_id     = EXCLUDED.tenant_id,
                            schema_name   = EXCLUDED.schema_name,
                            account_login = EXCLUDED.account_login,
                            account_type  = EXCLUDED.account_type,
                            updated_at    = NOW();
                    """,
                    str(installation_id),
                    tenant_id,
                    schema_name,
                    account_login,
                    account_type,
                )

        logger.info(
            "tenant_onboarded installation_id=%s tenant_id=%s schema=%s",
            installation_id, tenant_id, schema_name,
        )

    async def ensure_analysis_tables(self, schema_name: str) -> None:
        """Create the impact_analysis_runs table inside a tenant schema."""
        assert self._pool is not None
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema_name):
            raise ValueError(f"invalid schema_name: {schema_name!r}")
        async with self._pool.acquire() as conn:
            await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema_name}".impact_analysis_runs (
                    id              BIGSERIAL   PRIMARY KEY,
                    tenant_id       TEXT        NOT NULL,
                    pr_number       BIGINT,
                    payload         JSONB       NOT NULL,
                    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

    async def insert_analysis_result(
        self,
        schema_name: str,
        tenant_id: str,
        pr_number: Optional[int],
        payload: dict,
    ) -> None:
        """Append a raw analysis payload into the tenant schema."""
        assert self._pool is not None
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema_name):
            raise ValueError(f"invalid schema_name: {schema_name!r}")
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO "{schema_name}".impact_analysis_runs
                    (tenant_id, pr_number, payload)
                VALUES ($1, $2, $3::jsonb);
                """,
                tenant_id,
                pr_number,
                json.dumps(payload),
            )


# ─────────────────────────────────────────────────────────────────────────────
# 2. SQLAlchemy  –  get_db_session()
# ─────────────────────────────────────────────────────────────────────────────

_engine = create_engine(
    settings.DATABASE_URL,   # already has +psycopg dialect
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    future=True,
)

_SessionFactory = sessionmaker(
    bind=_engine,
    autoflush=False,
    autocommit=False,
    future=True,
)

logger.info(
    "SQLAlchemy engine created | host=%s",
    settings.DATABASE_URL.split("@")[-1],
)


@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """
    Yield a SQLAlchemy Session, commit on success, rollback on exception.

    Usage (inside the Kafka worker):
        with get_db_session() as db:
            result = db.execute(text("SELECT 1")).scalar()
    """
    session: Session = _SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def sqlalchemy_health_check() -> bool:
    """Return True if the DB is reachable via SQLAlchemy."""
    try:
        with get_db_session() as db:
            db.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        logger.error("SQLAlchemy health check failed: %s", exc)
        return False
