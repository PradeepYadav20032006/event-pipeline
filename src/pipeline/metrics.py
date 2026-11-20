import time
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

# Prometheus Metrics for Event Streaming Pipeline

EVENTS_PUBLISHED = Counter(
    "pipeline_events_published_total",
    "Total events published to the message broker",
    ["event_type", "status"]
)

CONSUMER_MESSAGES_PROCESSED = Counter(
    "pipeline_consumer_messages_total",
    "Total consumer messages evaluated by outcome",
    ["outcome"]  # success, retry, dlq
)

CONSUMER_DURATION_SECONDS = Histogram(
    "pipeline_consumer_duration_seconds",
    "Time taken to execute business logic per event",
    ["event_type"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)
)

HTTP_REQUEST_COUNT = Counter(
    "pipeline_http_requests_total",
    "Total HTTP requests to the pipeline API",
    ["method", "endpoint", "http_status"]
)

HTTP_REQUEST_DURATION = Histogram(
    "pipeline_http_request_duration_seconds",
    "HTTP endpoint response latency in seconds",
    ["method", "endpoint"]
)

DLQ_QUARANTINED_COUNT = Counter(
    "pipeline_dlq_quarantined_total",
    "Total messages diverted into Dead Letter Queue",
    ["reason"]
)


class PipelineMetricsMiddleware(BaseHTTPMiddleware):
    """Measures HTTP ingress throughput, status codes, and latency."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if request.url.path == "/metrics":
            return await call_next(request)

        start_time = time.perf_counter()
        try:
            response = await call_next(request)
            status_code = str(response.status_code)
            return response
        except Exception:
            status_code = "500"
            raise
        finally:
            duration = time.perf_counter() - start_time
            HTTP_REQUEST_COUNT.labels(
                method=request.method,
                endpoint=request.url.path,
                http_status=status_code
            ).inc()
            HTTP_REQUEST_DURATION.labels(
                method=request.method,
                endpoint=request.url.path
            ).observe(duration)
