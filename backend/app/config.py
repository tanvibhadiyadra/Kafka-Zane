"""
app/config.py – Unified Settings

Single source of truth for all environment variables across the merged system.
Reads from .env (docker-compose supplies overrides via `environment:` blocks).

Merging notes:
  • backend project used pydantic-settings with AliasChoices (asyncpg-based)
  • queryguard-async used pydantic-settings with psycopg / SQLAlchemy
  → We keep BOTH DB layers:
      asyncpg  → PostgresService  (used by webhook handler for fast onboarding)
      SQLAlchemy/psycopg → get_db_session() (used by the Kafka worker / impact analysis)
  • Kafka: unified to confluent-kafka (synchronous producer in webhook,
    sync consumer in worker).  aiokafka removed.
"""

from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent  # backend/


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── GitHub ────────────────────────────────────────────────────────────────
    GITHUB_WEBHOOK_SECRET: str = ""
    GITHUB_APP_ID: str = ""
    GITHUB_PRIVATE_KEY: str = ""
    # Set to False only for local smoke-testing without a real GitHub signature
    VERIFY_GITHUB_SIGNATURE: bool = True

    # ── PostgreSQL ─────────────────────────────────────────────────────────────
    # Accepted formats (both auto-normalised for SQLAlchemy):
    #   postgresql://user:pass@host/db
    #   postgresql+psycopg://user:pass@host/db
    DATABASE_URL: str 

    @field_validator("DATABASE_URL", mode="before")
    @classmethod
    def _normalise_db_url(cls, v: str) -> str:
        """
        SQLAlchemy 2 + psycopg v3 needs the +psycopg dialect.
        asyncpg (used in PostgresService for onboarding) needs the plain URL.
        We store the psycopg variant here; PostgresService strips the suffix.
        """
        if isinstance(v, str) and v.startswith("postgresql://"):
            return v.replace("postgresql://", "postgresql+psycopg://", 1)
        return v

    # ── Redis ─────────────────────────────────────────────────────────────────
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_TENANT_TTL_SECONDS: int = 3600

    # ── Kafka ─────────────────────────────────────────────────────────────────
    # Accept both the backend and queryguard env-var names
    KAFKA_BOOTSTRAP_SERVERS: str = Field(
        default="localhost:9092",
        validation_alias=AliasChoices(
            "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP", "kafka_bootstrap_servers"
        ),
    )
    KAFKA_TOPIC_GITHUB_EVENTS: str = Field(
        default="github-events",
        validation_alias=AliasChoices(
            "KAFKA_TOPIC_GITHUB_EVENTS", "KAFKA_TOPIC_GITHUB", "kafka_topic_github"
        ),
    )
    KAFKA_TOPIC_DLQ: str = Field(
        default="github-events-dlq",
        validation_alias=AliasChoices("KAFKA_TOPIC_DLQ", "kafka_topic_dlq"),
    )
    KAFKA_CONSUMER_GROUP: str = Field(
        default="queryguard-workers",
        validation_alias=AliasChoices(
            "KAFKA_CONSUMER_GROUP", "KAFKA_GROUP_ID", "kafka_group_id"
        ),
    )
    KAFKA_MAX_RETRIES: int = 3
    KAFKA_ENABLED: bool = True   # set False to skip Kafka entirely (smoke test)

    # ── Idempotency ───────────────────────────────────────────────────────────
    IDEMPOTENCY_TTL_SECONDS: int = 86400

    # ── Application ───────────────────────────────────────────────────────────
    LOG_LEVEL: str = "INFO"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


# Module-level singleton for convenient import
settings: Settings = get_settings()
