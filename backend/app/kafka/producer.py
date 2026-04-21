import json
import logging

from aiokafka import AIOKafkaProducer

from app.config import Settings

logger = logging.getLogger(__name__)


class KafkaProducerService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        if not self._settings.kafka_enabled:
            logger.info("Kafka disabled")
            return
        try:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._settings.kafka_bootstrap,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            )
            await self._producer.start()
        except Exception as exc:
            logger.warning("Kafka unavailable: %s", exc)
            p, self._producer = self._producer, None
            if p:
                try:
                    await p.stop()
                except Exception:
                    logger.exception("kafka_producer_cleanup_after_failed_start")
            return
        logger.info("Kafka connected")

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None

    async def send_github_event(self, envelope: dict) -> None:
        if self._producer is None:
            logger.warning("Kafka not available, skipping send")
            return
        inst = envelope.get("installation_id")
        pr = envelope.get("pr_number")
        key = f"{inst}:{pr}".encode("utf-8")
        await self._producer.send_and_wait(
            self._settings.kafka_topic_github,
            value=envelope,
            key=key,
        )

    async def send_dlq(self, envelope: dict, error: str) -> None:
        if self._producer is None:
            logger.warning("Kafka not available, skipping send")
            return
        dlq_payload = {**envelope, "dlq_error": error}
        await self._producer.send_and_wait(
            self._settings.kafka_topic_dlq,
            value=dlq_payload,
            key=str(envelope.get("delivery_id") or "").encode("utf-8"),
        )
