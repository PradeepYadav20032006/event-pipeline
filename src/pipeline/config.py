import os
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Event Streaming Pipeline Configuration."""
    APP_NAME: str = "Resilient Event Pipeline"
    APP_ENV: str = "development"
    DEBUG: bool = True

    # RabbitMQ Connection Parameters
    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "pipeline_user"
    RABBITMQ_PASSWORD: str = "pipeline_pass"
    RABBITMQ_VHOST: str = "/"
    RABBITMQ_URL: str = "amqp://pipeline_user:pipeline_pass@localhost:5672/"

    # Resilience & Retry Configuration
    MAX_RETRIES: int = 3
    RETRY_BASE_TTL_MS: int = 5000  # 5 seconds initial backoff

    # Topology Names
    MAIN_EXCHANGE: str = "events.direct"
    RETRY_EXCHANGE: str = "events.retry"
    DLX_EXCHANGE: str = "events.dlx"

    MAIN_QUEUE: str = "orders.process"
    RETRY_QUEUE: str = "orders.retry.queue"
    DLQ_QUEUE: str = "orders.dlq"

    ROUTING_KEY_ORDER_CREATED: str = "order.created"
    ROUTING_KEY_ORDER_CANCELLED: str = "order.cancelled"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


@lru_cache()
def get_settings() -> Settings:
    return Settings()
