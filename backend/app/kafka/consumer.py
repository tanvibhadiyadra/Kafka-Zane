"""
app/kafka/consumer.py – Kafka Consumer Worker

Blocking event loop. Runs as the `worker` container / process.
Separate from the FastAPI webhook service.

Processing pipeline per message:
  1.  Deserialise the event envelope
  2.  Check idempotency (skip already-processed events via pr_analyses table)
  3.  Resolve tenant  (Redis → PostgreSQL → auto-onboard if missing)
  4.  Run impact analysis  (fetch PR files, analyse SQL, post GitHub comment)
  5.  Persist results to pr_analyses
  6.  Commit Kafka offset (only after success or DLQ routing)

Error handling:
  • ValueError / TenantResolutionError  →  non-retryable, route to DLQ immediately
  • Any other Exception                →  retry up to KAFKA_MAX_RETRIES with exponential back-off
  • After all retries exhausted        →  publish to DLQ and commit offset
"""

import json
import logging
import time
from typing import Optional

from confluent_kafka import Consumer, KafkaError

from app.config import settings
from app.db.postgres import get_db_session
from app.db.redis_client import get_cached_tenant_id, set_cached_tenant_id
from app.kafka.producer import publish_to_dlq
from app.services.tenant_resolver import TenantResolver, TenantResolutionError
from app.services.impact_analysis import ImpactAnalysisService

logger = logging.getLogger("kafka.consumer")


# ── Consumer Factory ──────────────────────────────────────────────────────────

def _make_consumer() -> Consumer:
    config = {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": settings.KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,     # manual commit – only after processing
        "max.poll.interval.ms": 300000,  # 5 min (impact analysis can be slow)
        "session.timeout.ms": 30000,
        "heartbeat.interval.ms": 10000,
    }
    consumer = Consumer(config)
    logger.info(
        "Kafka Consumer created | bootstrap=%s group=%s",
        settings.KAFKA_BOOTSTRAP_SERVERS,
        settings.KAFKA_CONSUMER_GROUP,
    )
    return consumer


# ── Idempotency ───────────────────────────────────────────────────────────────

def _is_already_processed(idempotency_key: str, db_session) -> bool:
    """
    Check pr_analyses table (status='done') as the canonical idempotency store.
    Redis is the fast path; DB is the source of truth.
    """
    from sqlalchemy import text
    if not idempotency_key:
        return False
    result = db_session.execute(
        text(
            "SELECT id FROM pr_analyses "
            "WHERE idempotency_key = :key AND status = 'done' "
            "LIMIT 1"
        ),
        {"key": idempotency_key},
    ).fetchone()
    return result is not None


# ── Per-Message Processing ────────────────────────────────────────────────────

def _process_event(event: dict) -> None:
    """
    Full processing pipeline for one GitHub PR event. Must be idempotent.

    Raises:
        TenantResolutionError  – non-retryable
        ValueError             – non-retryable
        Exception              – retryable (network, DB timeout, etc.)
    """
    installation_id: str = str(event.get("installation_id", ""))
    pr_number: int = event.get("pr_number", 0)
    idempotency_key: str = event.get("idempotency_key", "")

    logger.info(
        "Processing event | installation_id=%s pr_number=%s idempotency_key=%s",
        installation_id, pr_number, idempotency_key,
    )

    with get_db_session() as db:

        # ── Idempotency check ─────────────────────────────────────────────────
        if _is_already_processed(idempotency_key, db):
            logger.info(
                "Skipping duplicate event | idempotency_key=%s", idempotency_key
            )
            return

        # ── Tenant resolution ─────────────────────────────────────────────────
        resolver = TenantResolver(db=db)
        tenant_id: Optional[str] = resolver.resolve(installation_id)

        if not tenant_id:
            raise TenantResolutionError(
                f"Tenant not found for installation_id={installation_id}"
            )

        logger.info(
            "Tenant resolved | installation_id=%s tenant_id=%s",
            installation_id, tenant_id,
        )

        # ── Impact analysis ───────────────────────────────────────────────────
        service = ImpactAnalysisService(db=db, tenant_id=tenant_id)
        service.run(event)

        logger.info(
            "Impact analysis complete | installation_id=%s pr_number=%s tenant_id=%s",
            installation_id, pr_number, tenant_id,
        )


# ── Main Consumer Loop ────────────────────────────────────────────────────────

def run_consumer() -> None:
    """
    Blocking event loop. Run in the worker container.
    Subscribe → poll → process → commit.
    """
    consumer = _make_consumer()
    consumer.subscribe([settings.KAFKA_TOPIC_GITHUB_EVENTS])

    logger.info(
        "Worker started | topic=%s group=%s",
        settings.KAFKA_TOPIC_GITHUB_EVENTS,
        settings.KAFKA_CONSUMER_GROUP,
    )

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue   # normal idle poll

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(
                        "End of partition | %s [%d]", msg.topic(), msg.partition()
                    )
                else:
                    logger.error("Kafka consumer error: %s", msg.error())
                continue

            # ── Deserialise ───────────────────────────────────────────────────
            try:
                event: dict = json.loads(msg.value().decode("utf-8"))
            except Exception as exc:
                logger.error("Cannot deserialise message – routing to DLQ: %s", exc)
                publish_to_dlq({}, f"deserialisation_error: {exc}")
                consumer.commit(message=msg)
                continue

            # ── Retry loop ────────────────────────────────────────────────────
            last_error: Optional[Exception] = None

            for attempt in range(1, settings.KAFKA_MAX_RETRIES + 1):
                try:
                    _process_event(event)
                    last_error = None
                    break   # success
                except (TenantResolutionError, ValueError) as exc:
                    # Non-retryable
                    last_error = exc
                    logger.warning(
                        "Non-retryable error (attempt %d/%d) | installation_id=%s: %s",
                        attempt, settings.KAFKA_MAX_RETRIES,
                        event.get("installation_id"), exc,
                    )
                    break
                except Exception as exc:
                    last_error = exc
                    logger.warning(
                        "Retryable error (attempt %d/%d) | installation_id=%s: %s",
                        attempt, settings.KAFKA_MAX_RETRIES,
                        event.get("installation_id"), exc,
                    )
                    if attempt < settings.KAFKA_MAX_RETRIES:
                        backoff = min(2 ** attempt, 30)
                        logger.info("Retrying in %ds …", backoff)
                        time.sleep(backoff)

            # ── DLQ routing ───────────────────────────────────────────────────
            if last_error is not None:
                publish_to_dlq(event, str(last_error))
                logger.error(
                    "Event routed to DLQ | installation_id=%s pr_number=%s reason=%s",
                    event.get("installation_id"),
                    event.get("pr_number"),
                    last_error,
                )

            # ── Commit offset (always) ────────────────────────────────────────
            consumer.commit(message=msg)

    except KeyboardInterrupt:
        logger.info("Worker interrupted by user")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed")
