import json
import logging
from typing import Callable, Awaitable, Optional, Dict, Any
import aio_pika
from aio_pika import IncomingMessage, Message, DeliveryMode
from src.pipeline.config import get_settings
from src.pipeline.broker import broker_manager
from src.pipeline.schemas import EventEnvelope

logger = logging.getLogger("event_pipeline.consumer")


class PoisonPillException(Exception):
    """Raised when an unrecoverable payload error is encountered, triggering immediate DLQ routing."""
    pass


class TransientException(Exception):
    """Raised when a temporary downstream outage occurs (e.g. database timeout), triggering retry."""
    pass


class EventConsumer:
    """
    Resilient Event Consumer implementing:
    - Manual Acknowledgment (ACK) on success
    - Automated Exponential Retry routing to TTL backoff queue when attempts < MAX_RETRIES
    - Automated Dead-Letter routing to DLX when attempts >= MAX_RETRIES or on PoisonPillException
    """

    def __init__(self, message_handler: Optional[Callable[[EventEnvelope], Awaitable[None]]] = None):
        self.settings = get_settings()
        self.handler = message_handler or self.default_handler

    async def default_handler(self, event: EventEnvelope):
        """Default business logic handler for order processing."""
        logger.info("Processing order event: %s for customer %s", event.event_id, event.payload.get("customer_id"))
        # Simulated poison-pill check
        if event.payload.get("trigger_poison_pill"):
            raise PoisonPillException("Malicious or corrupt payload detected; unrecoverable.")
        if event.payload.get("trigger_transient_error"):
            raise TransientException("Simulated downstream payment gateway timeout.")

    async def process_message(self, message: IncomingMessage):
        """
        Processes a single incoming message within a manual ACK context.
        """
        async with message.process(requeue=False, ignore_processed=True):
            try:
                # 1. Parse JSON body into EventEnvelope
                payload_dict = json.loads(message.body.decode("utf-8"))
                event = EventEnvelope(**payload_dict)

                # Extract retry count from header or envelope
                retry_count = int(message.headers.get("x-retry-count", event.retry_count))
                event.retry_count = retry_count

                logger.info(
                    "Received event %s [Attempt %d/%d]",
                    event.event_id,
                    event.retry_count + 1,
                    self.settings.MAX_RETRIES + 1
                )

                # 2. Execute business handler
                await self.handler(event)

                # 3. Message successfully processed -> ACK implicitly on block exit
                logger.info("Event %s successfully processed and ACKed.", event.event_id)

            except PoisonPillException as p_err:
                logger.error(
                    "Poison pill detected for event %s: %s. Quarantining immediately to DLQ.",
                    message.message_id,
                    p_err
                )
                await self._forward_to_dlq(message, reason=str(p_err))

            except Exception as exc:
                # 4. Handle transient or unforeseen failure
                current_retries = int(message.headers.get("x-retry-count", 0))

                if current_retries < self.settings.MAX_RETRIES:
                    next_retry = current_retries + 1
                    logger.warning(
                        "Processing error for event %s (%s). Routing to Retry Exchange [Retry %d/%d].",
                        message.message_id,
                        exc,
                        next_retry,
                        self.settings.MAX_RETRIES
                    )
                    await self._forward_to_retry_ring(message, next_retry_count=next_retry, error_msg=str(exc))
                else:
                    logger.critical(
                        "Max retries (%d) exhausted for event %s. Forwarding to Dead Letter Queue (DLQ).",
                        self.settings.MAX_RETRIES,
                        message.message_id
                    )
                    await self._forward_to_dlq(message, reason=f"Max retries exhausted: {str(exc)}")

    async def _forward_to_retry_ring(self, orig_msg: IncomingMessage, next_retry_count: int, error_msg: str):
        """Publishes the message into the retry exchange with an incremented retry counter."""
        channel = await broker_manager.get_channel()
        retry_exchange = await channel.get_exchange(self.settings.RETRY_EXCHANGE)

        # Update envelope retry_count in body if possible
        try:
            body_dict = json.loads(orig_msg.body.decode("utf-8"))
            body_dict["retry_count"] = next_retry_count
            new_body = json.dumps(body_dict).encode("utf-8")
        except Exception:
            new_body = orig_msg.body

        headers = dict(orig_msg.headers or {})
        headers["x-retry-count"] = next_retry_count
        headers["x-last-error"] = error_msg[:200]

        retry_message = Message(
            body=new_body,
            delivery_mode=DeliveryMode.PERSISTENT,
            content_type=orig_msg.content_type,
            content_encoding=orig_msg.content_encoding,
            message_id=orig_msg.message_id,
            correlation_id=orig_msg.correlation_id,
            headers=headers
        )

        await retry_exchange.publish(
            retry_message,
            routing_key=orig_msg.routing_key or self.settings.ROUTING_KEY_ORDER_CREATED
        )

    async def _forward_to_dlq(self, orig_msg: IncomingMessage, reason: str):
        """Publishes the failed message directly to the Dead Letter Exchange (DLX)."""
        channel = await broker_manager.get_channel()
        dlx_exchange = await channel.get_exchange(self.settings.DLX_EXCHANGE)

        headers = dict(orig_msg.headers or {})
        headers["x-death-reason"] = reason[:500]
        headers["x-original-queue"] = self.settings.MAIN_QUEUE

        dlq_message = Message(
            body=orig_msg.body,
            delivery_mode=DeliveryMode.PERSISTENT,
            content_type=orig_msg.content_type,
            content_encoding=orig_msg.content_encoding,
            message_id=orig_msg.message_id,
            correlation_id=orig_msg.correlation_id,
            headers=headers
        )

        await dlx_exchange.publish(
            dlq_message,
            routing_key=orig_msg.routing_key or self.settings.ROUTING_KEY_ORDER_CREATED
        )
