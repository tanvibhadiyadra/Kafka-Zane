"""
worker_main.py – Kafka Consumer Worker Entry Point

Runs as the `worker` container in docker-compose.
Completely separate process from the FastAPI webhook service.

Start locally:
    python worker_main.py

Via Docker Compose:
    docker compose up worker

Scale horizontally (each instance gets its own Kafka partition assignment):
    docker compose up --scale worker=3
"""

import logging
import sys

from app.config import settings
from app.kafka.consumer import run_consumer

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger("worker")

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("  QueryGuard Worker Starting")
    logger.info("  Kafka Bootstrap : %s", settings.KAFKA_BOOTSTRAP_SERVERS)
    logger.info("  Topic           : %s", settings.KAFKA_TOPIC_GITHUB_EVENTS)
    logger.info("  Consumer Group  : %s", settings.KAFKA_CONSUMER_GROUP)
    logger.info("  DLQ Topic       : %s", settings.KAFKA_TOPIC_DLQ)
    logger.info("=" * 60)
    run_consumer()
