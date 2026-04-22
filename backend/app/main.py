import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.config import get_settings
from app.db.redis_client import RedisService
from app.kafka.producer import KafkaProducerService
from app.webhook.handler import router as webhook_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    producer = KafkaProducerService(settings)
    await producer.start()
    redis_service = RedisService(settings.redis_url)
    await redis_service.connect()
    app.state.settings = settings
    app.state.producer = producer
    app.state.redis = redis_service
    logger.info("app_startup_complete")
    yield
    await producer.stop()
    await redis_service.close()
    logger.info("app_shutdown_complete")


app = FastAPI(title="GitHub Webhook POC", lifespan=lifespan)
app.include_router(webhook_router, prefix="/webhook", tags=["webhook"])


@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/webhook")
async def webhook(request: Request):
    data = await request.json()
    print("🔥 Received event!")
    print(data)
    return {"status": "ok"}
