import asyncio
import logging
from typing import Optional
import aio_pika
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel
from src.pipeline.config import get_settings

logger = logging.getLogger("event_pipeline.broker")


class RabbitMQBrokerManager:
    """
    Manages asynchronous connection and channel lifecycles to RabbitMQ.
    Uses aio-pika robust connections with automated reconnection and health probes.
    """

    def __init__(self):
        self._connection: Optional[AbstractRobustConnection] = None
        self._channel: Optional[AbstractRobustChannel] = None
        self._url: Optional[str] = None

    def init(self, url: str):
        self._url = url

    @property
    def is_connected(self) -> bool:
        return self._connection is not None and not self._connection.is_closed

    async def connect(self) -> AbstractRobustConnection:
        """Establishes or returns the active robust AMQP connection."""
        if not self._url:
            settings = get_settings()
            self._url = settings.RABBITMQ_URL

        if self._connection is None or self._connection.is_closed:
            logger.info("Connecting to RabbitMQ at %s...", self._url.split("@")[-1])
            self._connection = await aio_pika.connect_robust(self._url)
            logger.info("RabbitMQ robust connection established.")
        return self._connection

    async def get_channel(self) -> AbstractRobustChannel:
        """Creates or returns an active robust channel with publisher confirms enabled."""
        if self._channel is None or self._channel.is_closed:
            connection = await self.connect()
            # publisher_confirms=True guarantees that publish() awaits broker ACK
            self._channel = await connection.channel(publisher_confirms=True)
            logger.info("RabbitMQ robust channel opened with publisher confirms.")
        return self._channel

    async def ping(self) -> bool:
        """
        Lightweight health probe verifying broker liveness and channel capability.
        """
        if self._connection is None or self._connection.is_closed:
            return False
        try:
            channel = await self.get_channel()
            return not channel.is_closed
        except Exception as exc:
            logger.error("RabbitMQ health probe failed: %s", exc)
            return False

    async def close(self):
        """Cleanly closes open channels and connection pools."""
        if self._channel is not None and not self._channel.is_closed:
            await self._channel.close()
            self._channel = None
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()
            self._connection = None
        logger.info("RabbitMQ broker connections closed.")


broker_manager = RabbitMQBrokerManager()
