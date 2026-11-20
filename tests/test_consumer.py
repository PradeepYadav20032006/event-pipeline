import json
import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from aio_pika import IncomingMessage, DeliveryMode
from src.pipeline.consumer import EventConsumer, PoisonPillException, TransientException
from src.pipeline.schemas import EventEnvelope, EventType
from src.pipeline.config import get_settings


def create_fake_incoming_message(payload_dict: dict, retry_count: int = 0) -> IncomingMessage:
    """Helper to construct a mock IncomingMessage wrapping an EventEnvelope."""
    event = EventEnvelope(
        event_id=str(uuid.uuid4()),
        correlation_id=str(uuid.uuid4()),
        event_type=EventType.ORDER_CREATED,
        retry_count=retry_count,
        payload=payload_dict
    )
    raw_body = event.model_dump_json().encode("utf-8")

    msg = MagicMock(spec=IncomingMessage)
    msg.body = raw_body
    msg.message_id = event.event_id
    msg.correlation_id = event.correlation_id
    msg.routing_key = "order.created"
    msg.content_type = "application/json"
    msg.content_encoding = "utf-8"
    msg.headers = {"x-retry-count": retry_count}

    # In aio-pika, message.process(...) is a synchronous function returning an asynchronous context manager
    class ProcessContext:
        async def __aenter__(self):
            return msg
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return False

    msg.process = MagicMock(return_value=ProcessContext())
    return msg


@pytest.mark.asyncio
async def test_consumer_successful_processing():
    """Verifies that a valid message is processed cleanly without triggering retries or DLQ."""
    processed_events = []

    async def custom_handler(event: EventEnvelope):
        processed_events.append(event.event_id)

    consumer = EventConsumer(message_handler=custom_handler)
    consumer._forward_to_retry_ring = AsyncMock()
    consumer._forward_to_dlq = AsyncMock()

    msg = create_fake_incoming_message({"customer_id": "cust-1", "amount": 100})
    await consumer.process_message(msg)

    assert len(processed_events) == 1
    consumer._forward_to_retry_ring.assert_not_called()
    consumer._forward_to_dlq.assert_not_called()


@pytest.mark.asyncio
async def test_consumer_transient_error_routes_to_retry_ring():
    """Verifies that a transient error routes the message to the retry exchange with incremented count."""
    async def failing_handler(event: EventEnvelope):
        raise TransientException("Downstream timeout")

    consumer = EventConsumer(message_handler=failing_handler)
    consumer._forward_to_retry_ring = AsyncMock()
    consumer._forward_to_dlq = AsyncMock()

    # Initial attempt (retry_count = 0)
    msg = create_fake_incoming_message({"customer_id": "cust-2"}, retry_count=0)
    await consumer.process_message(msg)

    consumer._forward_to_retry_ring.assert_awaited_once()
    assert consumer._forward_to_retry_ring.await_args.kwargs["next_retry_count"] == 1
    consumer._forward_to_dlq.assert_not_called()


@pytest.mark.asyncio
async def test_consumer_max_retries_exhausted_routes_to_dlq():
    """Verifies that once retry_count reaches MAX_RETRIES (3), it is quarantined to DLQ."""
    settings = get_settings()

    async def failing_handler(event: EventEnvelope):
        raise TransientException("Persistent database error")

    consumer = EventConsumer(message_handler=failing_handler)
    consumer._forward_to_retry_ring = AsyncMock()
    consumer._forward_to_dlq = AsyncMock()

    # Message at max retries (3)
    msg = create_fake_incoming_message({"customer_id": "cust-3"}, retry_count=settings.MAX_RETRIES)
    await consumer.process_message(msg)

    consumer._forward_to_retry_ring.assert_not_called()
    consumer._forward_to_dlq.assert_awaited_once()
    assert "Max retries exhausted" in consumer._forward_to_dlq.await_args.kwargs["reason"]


@pytest.mark.asyncio
async def test_consumer_poison_pill_immediate_dlq():
    """Verifies that a poison pill error skips retries and is quarantined directly to DLQ."""
    async def poison_handler(event: EventEnvelope):
        raise PoisonPillException("Corrupt invalid payload format")

    consumer = EventConsumer(message_handler=poison_handler)
    consumer._forward_to_retry_ring = AsyncMock()
    consumer._forward_to_dlq = AsyncMock()

    msg = create_fake_incoming_message({"customer_id": "cust-bad"}, retry_count=0)
    await consumer.process_message(msg)

    consumer._forward_to_retry_ring.assert_not_called()
    consumer._forward_to_dlq.assert_awaited_once()
    assert "Corrupt invalid payload format" in consumer._forward_to_dlq.await_args.kwargs["reason"]
