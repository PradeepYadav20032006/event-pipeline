import logging
from contextlib import asynccontextmanager
from typing import Dict, Any
from fastapi import FastAPI, status, Response
from fastapi.responses import JSONResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from src.pipeline.config import get_settings
from src.pipeline.broker import broker_manager
from src.pipeline.routes import router as event_router
from src.pipeline.metrics import PipelineMetricsMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("event_pipeline.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages startup and shutdown lifecycles:
    Establishes RabbitMQ robust connection on boot, cleans up on shutdown.
    """
    settings = get_settings()
    logger.info("Initializing %s in %s environment...", settings.APP_NAME, settings.APP_ENV)
    broker_manager.init(settings.RABBITMQ_URL)

    yield

    logger.info("Closing RabbitMQ connections...")
    await broker_manager.close()
    logger.info("Shutdown complete.")


app = FastAPI(
    title="Resilient Event Streaming Pipeline",
    description="Asynchronous event pipeline with RabbitMQ publisher confirms, Dead Letter Exchange (DLX), and retry ring topology.",
    version="1.0.0",
    lifespan=lifespan,
)

# Register Metrics Middleware
app.add_middleware(PipelineMetricsMiddleware)

# Register Event Ingress API Routes
app.include_router(event_router)


@app.get("/metrics", tags=["Observability"])
async def metrics_endpoint():
    """Prometheus metrics scrape target."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health", tags=["Observability"])
async def health_check() -> JSONResponse:
    """
    Liveness probe verifying RabbitMQ connection status.
    """
    is_broker_alive = await broker_manager.ping()
    overall_status = "healthy" if is_broker_alive else "degraded"
    status_code = status.HTTP_200_OK if is_broker_alive else status.HTTP_503_SERVICE_UNAVAILABLE

    payload: Dict[str, Any] = {
        "status": overall_status,
        "dependencies": {
            "rabbitmq": "connected" if is_broker_alive else "disconnected",
        }
    }
    return JSONResponse(status_code=status_code, content=payload)


@app.get("/", tags=["Root"])
async def root():
    settings = get_settings()
    return {
        "service": settings.APP_NAME,
        "version": "1.0.0",
        "docs_url": "/docs",
        "health_url": "/health"
    }
