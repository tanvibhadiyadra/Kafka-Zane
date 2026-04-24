"""
app/kafka/producer.py – Kafka Producer

Uses confluent-kafka (librdkafka) for high-throughput, low-latency delivery.
The producer is a module-level singleton shared across all webhook requests.

Design:
  • produce() is non-blocking → webhook response stays < 2 s
  • Delivery callback logs every success / failure
  • Idempotent producer prevents duplicates on retry
  • publish_to_dlq() routes poison-pill envelopes to the dead-letter topic

Why confluent-kafka instead of aiokafka?
  The queryguard-async project already uses confluent-kafka in the consumer.
  Unifying on one Kafka library reduces dependency surface and makes the
  Dockerfiles simpler (librdkafka is already present).
  The webhook process is sync-friendly (uvicorn runs an event loop, but
  producer.produce() is non-blocking and poll() is O(µs)).
"""

import json
import logging
import time
from typing import Any

from confluent_kafka import Producer, KafkaException

from app.config import settings

logger = logging.getLogger("kafka.producer")

# ── Singleton ─────────────────────────────────────────────────────────────────
_producer: Producer | None = None


def get_producer() -> Producer:
    """Return the global Kafka Producer, creating on first call."""
    global _producer
    if _producer is None:
        config = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "retries": 5,
            "linger.ms": 5,
            "acks": "all",
            "enable.idempotence": True,
        }
        _producer = Producer(config)
        logger.info(
            "Kafka Producer initialised | bootstrap=%s",
            settings.KAFKA_BOOTSTRAP_SERVERS,
        )
    return _producer


def close_producer() -> None:
    """Flush remaining messages and destroy the producer. Call on shutdown."""
    global _producer
    if _producer is not None:
        remaining = _producer.flush(timeout=30)
        if remaining > 0:
            logger.warning(
                "Kafka flush: %d message(s) may not have been delivered", remaining
            )
        else:
            logger.info("Kafka Producer flushed cleanly")
        _producer = None


# ── Delivery Callback ─────────────────────────────────────────────────────────

def _on_delivery(err, msg):
    if err:
        logger.error(
            "Kafka delivery FAILED | topic=%s partition=%s offset=%s err=%s key=%s",
            msg.topic(), msg.partition(), msg.offset(), err,
            msg.key().decode() if msg.key() else None,
        )
    else:
        logger.info(
            "Kafka delivery OK | topic=%s partition=%s offset=%s key=%s",
            msg.topic(), msg.partition(), msg.offset(),
            msg.key().decode() if msg.key() else None,
        )


# ── Public API ────────────────────────────────────────────────────────────────

def publish_event(topic: str, key: str, value: dict[str, Any]) -> None:
    """
    Non-blocking publish of a JSON event to a Kafka topic.

    Args:
        topic:  Target Kafka topic
        key:    Partition key (use installation_id to route same tenant together)
        value:  Python dict serialised to JSON

    Raises:
        KafkaException: if the local queue is full (Kafka broker unreachable)
        BufferError:    if the local producer queue is full
    """
    if not settings.KAFKA_ENABLED:
        logger.debug("Kafka disabled – skipping publish to %s", topic)
        return

    producer = get_producer()
    try:
        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(value, default=str).encode("utf-8"),
            on_delivery=_on_delivery,
        )
        producer.poll(0)   # dispatch delivery callbacks without blocking

    except KafkaException as exc:
        logger.error(
            "Failed to enqueue Kafka message | topic=%s key=%s err=%s",
            topic, key, exc,
        )
        raise
    except BufferError:
        logger.error(
            "Kafka producer local queue full – message dropped | topic=%s key=%s",
            topic, key,
        )
        raise


def publish_to_dlq(envelope: dict[str, Any], reason: str) -> None:
    """
    Route a failed event to the Dead-Letter Queue with failure metadata.
    Swallows exceptions so a DLQ failure never blocks processing.
    """
    dlq_payload = {
        **envelope,
        "_dlq_reason": reason,
        "_dlq_timestamp": time.time(),
    }
    try:
        publish_event(
            topic=settings.KAFKA_TOPIC_DLQ,
            key=str(envelope.get("installation_id", "unknown")),
            value=dlq_payload,
        )
        logger.warning(
            "Message sent to DLQ | installation_id=%s pr_number=%s reason=%s",
            envelope.get("installation_id"),
            envelope.get("pr_number"),
            reason,
        )
    except Exception as exc:
        logger.error("Failed to publish to DLQ: %s", exc)
