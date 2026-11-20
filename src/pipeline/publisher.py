import json
import logging
from typing import Optional
import aio_pika
from aio_pika import Message, DeliveryMode
from src.pipeline.broker import broker_manager
from src.pipeline.config import get_settings
from src.pipeline.schemas import EventEnvelope

logger = logging.getLogger("event_pipeline.publisher")


class EventPublisher:
    """
    Asynchronous event publisher with Publisher Confirms and persistent delivery.
    Ensures events are safely buffered on the RabbitMQ broker before acknowledging HTTP callers.
    """

    def __init__(self):
        self.settings = get_settings()

    async def publish(
        self,
        event: EventEnvelope,
        exchange_name: Optional[str] = None,
        routing_key: Optional[str] = None
    ) -> bool:
        """
        Publishes a message to the direct exchange with delivery_mode=PERSISTENT.
        Awaits broker confirmation to prevent silent message loss.
        """
        target_exchange = exchange_name or self.settings.MAIN_EXCHANGE
        target_routing_key = routing_key or event.event_type

        channel = await broker_manager.get_channel()

        # Declare or get direct exchange
        exchange = await channel.declare_exchange(
            target_exchange,
            aio_pika.ExchangeType.DIRECT,
            durable=True
        )

        message_body = event.model_dump_json().encode("utf-8")

        message = Message(
            body=message_body,
            delivery_mode=DeliveryMode.PERSISTENT,  # Write to disk on broker
            content_type="application/json",
            content_encoding="utf-8",
            message_id=event.event_id,
            correlation_id=event.correlation_id,
            headers={
                "x-retry-count": event.retry_count,
                "x-source": event.source,
                "x-event-type": event.event_type,
            }
        )

        logger.info(
            "Publishing event %s (%s) to exchange '%s' with key '%s'",
            event.event_id,
            event.event_type,
            target_exchange,
            target_routing_key
        )

        # publisher_confirms ensures this call awaits broker acknowledgment
        await exchange.publish(
            message,
            routing_key=target_routing_key
        )

        return True


event_publisher = EventPublisher()
