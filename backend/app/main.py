"""
app/main.py – FastAPI Webhook Service Entry Point

This is the ONLY process exposed to the internet (via ngrok or a reverse proxy).

Sole responsibilities:
  • Receive GitHub webhooks and validate HMAC signature
  • Handle installation.created (tenant onboarding)
  • Publish pull_request events to Kafka
  • Respond to GitHub in < 2 seconds

Heavy work (impact analysis, GitHub API calls) happens in the worker process.

Services wired up in lifespan:
  • PostgresService (asyncpg)  – tenant onboarding, health-check
  • RedisService (async)       – idempotency / tenant cache
  • Kafka Producer (confluent) – singleton initialised on startup via get_producer()
"""

import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings
from app.db.postgres import PostgresService
from app.db.redis_client import RedisService
from app.kafka.producer import get_producer, close_producer
from app.webhook.handler import router as webhook_router

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("app")


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Starting QueryGuard Webhook Service")

    # Initialise connections
    pg = PostgresService()
    redis = RedisService()

    await pg.connect()
    await redis.connect()

    if settings.KAFKA_ENABLED:
        get_producer()   # warm up the singleton
        logger.info("✅ Kafka producer ready | bootstrap=%s", settings.KAFKA_BOOTSTRAP_SERVERS)
    else:
        logger.warning("⚠️  Kafka disabled (KAFKA_ENABLED=false) – events will be dropped")

    # Attach to app state so handlers can access them via request.app.state
    app.state.pg = pg
    app.state.redis = redis
    app.state.settings = settings

    logger.info("✅ QueryGuard Webhook Service started")
    yield

    # Graceful shutdown
    logger.info("🛑 Shutting down …")
    close_producer()
    await redis.close()
    await pg.close()
    logger.info("✅ Shutdown complete")


# ── Application ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="QueryGuard – GitHub Webhook Service",
    description=(
        "Receives GitHub App webhook events, validates signatures, "
        "and publishes events to Kafka for async impact analysis."
    ),
    version="3.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routes ────────────────────────────────────────────────────────────────────
app.include_router(webhook_router, prefix="/webhook", tags=["Webhook"])


@app.get("/health", tags=["Health"])
async def health(request: Request):
    """
    Dependency health-check.
    Returns 200 if all services are reachable, 503 if any are degraded.
    """
    pg_ok = await request.app.state.pg.is_healthy()
    redis_ok = await request.app.state.redis.is_healthy()
    kafka_ok = settings.KAFKA_ENABLED  # producer is sync; OK if process started

    overall_ok = pg_ok and redis_ok and kafka_ok

    payload = {
        "status": "ok" if overall_ok else "degraded",
        "service": "webhook",
        "dependencies": {
            "postgres": pg_ok,
            "redis": redis_ok,
            "kafka": kafka_ok,
        },
    }
    return JSONResponse(
        status_code=200 if overall_ok else 503,
        content=payload,
    )


@app.get("/", tags=["Root"])
def root():
    return {"service": "QueryGuard Webhook", "docs": "/docs"}
