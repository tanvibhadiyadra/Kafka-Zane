import asyncio
import json
import logging
import traceback

from aiokafka import AIOKafkaConsumer

from app.config import get_settings
from app.db.postgres import PostgresService
from app.db.redis_client import RedisService
from app.kafka.producer import KafkaProducerService
from app.services.impact_analysis import run_impact_analysis
from app.services.tenant_resolver import TenantResolver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

MAX_RETRIES = 4
BASE_DELAY_SEC = 0.5


def _delivery_idempotency_key(delivery_id: object) -> str:
    return f"idemp:delivery:{delivery_id}"


async def _process_message(
    value: dict,
    resolver: TenantResolver,
    redis: RedisService,
    ttl: int,
) -> None:
    installation_id = value.get("installation_id")
    delivery_id = value.get("delivery_id")
    pr_number = value.get("pr_number")
    payload = value.get("payload") or {}

    if delivery_id:
        if await redis.is_processed(_delivery_idempotency_key(delivery_id)):
            logger.info(
                "skip_already_processed delivery_id=%s installation_id=%s",
                delivery_id,
                installation_id,
            )
            return

    tenant_id = await resolver.resolve_tenant(installation_id)
    if tenant_id is None:
        logger.error(
            "tenant_not_found installation_id=%s delivery_id=%s",
            installation_id,
            delivery_id,
        )
        return

    logger.info(
        "processing_event installation_id=%s tenant_id=%s delivery_id=%s pr_number=%s",
        installation_id,
        tenant_id,
        delivery_id,
        pr_number,
    )
    await run_impact_analysis(tenant_id, payload)

    if delivery_id:
        await redis.mark_processed(_delivery_idempotency_key(delivery_id), ttl)


async def _run_consumer() -> None:
    settings = get_settings()
    redis = RedisService(settings.redis_url)
    await redis.connect()
    pg = PostgresService(settings.database_url)
    await pg.connect()
    resolver = TenantResolver(redis=redis, pg=pg)

    dlq_producer = KafkaProducerService(settings)
    await dlq_producer.start()

    consumer = AIOKafkaConsumer(
        settings.kafka_topic_github,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id=settings.kafka_group_id,
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    await consumer.start()
    logger.info(
        "consumer_started topic=%s group=%s",
        settings.kafka_topic_github,
        settings.kafka_group_id,
    )

    try:
        async for msg in consumer:
            value = msg.value
            last_error: str | None = None
            succeeded = False
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    await _process_message(
                        value,
                        resolver,
                        redis,
                        settings.idempotency_ttl_seconds,
                    )
                    succeeded = True
                    break
                except Exception:
                    last_error = traceback.format_exc()
                    logger.exception(
                        "consumer_retry attempt=%s partition=%s offset=%s",
                        attempt,
                        msg.partition,
                        msg.offset,
                    )
                    if attempt < MAX_RETRIES:
                        delay = BASE_DELAY_SEC * (2 ** (attempt - 1))
                        await asyncio.sleep(delay)

            if not succeeded and last_error is not None:
                err_short = last_error[:4000]
                logger.error(
                    "consumer_permanent_failure sending_dlq partition=%s offset=%s",
                    msg.partition,
                    msg.offset,
                )
                try:
                    await dlq_producer.send_dlq(value, err_short)
                except Exception:
                    logger.exception("dlq_send_failed")

            await consumer.commit()
    finally:
        await consumer.stop()
        await dlq_producer.stop()
        await redis.close()
        await pg.close()
        logger.info("consumer_stopped")


def main() -> None:
    asyncio.run(_run_consumer())


if __name__ == "__main__":
    main()
