import json
import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock
from aio_pika import IncomingMessage
from src.pipeline.dlq_admin import DLQAdminManager
from src.pipeline.broker import broker_manager
from src.pipeline.schemas import EventEnvelope, EventType


def create_mock_dlq_message(payload_dict: dict, reason: str = "Downstream timeout") -> IncomingMessage:
    """Helper creating a mock IncomingMessage in the DLQ."""
    event = EventEnvelope(
        event_id=str(uuid.uuid4()),
        correlation_id=str(uuid.uuid4()),
        event_type=EventType.ORDER_CREATED,
        retry_count=3,
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
    msg.headers = {
        "x-retry-count": 3,
        "x-death-reason": reason,
        "x-original-queue": "orders.process"
    }
    msg.reject = AsyncMock()
    msg.ack = AsyncMock()
    return msg


@pytest.mark.asyncio
async def test_dlq_inspect_non_destructive():
    """Verifies that inspecting DLQ peeks without removing or ACKing messages."""
    admin = DLQAdminManager()

    mock_msg1 = create_mock_dlq_message({"order_id": "ord-1"}, reason="DB Timeout")
    mock_msg2 = create_mock_dlq_message({"order_id": "ord-2"}, reason="Invalid schema")

    mock_queue = AsyncMock()
    # Return two messages then None (empty queue)
    mock_queue.get = AsyncMock(side_effect=[mock_msg1, mock_msg2, None])

    mock_channel = AsyncMock()
    mock_channel.get_queue = AsyncMock(return_value=mock_queue)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(broker_manager, "get_channel", AsyncMock(return_value=mock_channel))
        inspected = await admin.inspect_dlq(limit=10)

        assert len(inspected) == 2
        assert inspected[0]["message_id"] == mock_msg1.message_id
        assert inspected[0]["death_reason"] == "DB Timeout"
        assert inspected[1]["message_id"] == mock_msg2.message_id
        assert inspected[1]["death_reason"] == "Invalid schema"

        # Crucial: verify messages were rejected with requeue=True (non-destructive)
        mock_msg1.reject.assert_awaited_once_with(requeue=True)
        mock_msg2.reject.assert_awaited_once_with(requeue=True)
        mock_msg1.ack.assert_not_called()
        mock_msg2.ack.assert_not_called()


@pytest.mark.asyncio
async def test_dlq_replay_republishes_and_acks():
    """Verifies that replaying DLQ messages republishes to main exchange and ACKs from DLQ."""
    admin = DLQAdminManager()

    mock_msg = create_mock_dlq_message({"order_id": "ord-replay-1"})

    mock_queue = AsyncMock()
    mock_queue.get = AsyncMock(side_effect=[mock_msg, None])

    mock_exchange = AsyncMock()
    mock_exchange.publish = AsyncMock()

    mock_channel = AsyncMock()
    mock_channel.get_queue = AsyncMock(return_value=mock_queue)
    mock_channel.get_exchange = AsyncMock(return_value=mock_exchange)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(broker_manager, "get_channel", AsyncMock(return_value=mock_channel))
        replayed = await admin.replay_dlq(limit=5)

        assert replayed == 1
        # Verify message was republished to main exchange
        mock_exchange.publish.assert_awaited_once()
        published_msg = mock_exchange.publish.await_args.args[0]
        assert published_msg.headers["x-retry-count"] == 0
        assert published_msg.headers["x-replayed-from-dlq"] == "true"
        assert "x-death-reason" not in published_msg.headers

        # Verify message was acknowledged from DLQ
        mock_msg.ack.assert_awaited_once()
        mock_msg.reject.assert_not_called()


@pytest.mark.asyncio
async def test_dlq_purge():
    """Verifies DLQ purge operation."""
    admin = DLQAdminManager()

    mock_purge_result = MagicMock()
    mock_purge_result.message_count = 15

    mock_queue = AsyncMock()
    mock_queue.purge = AsyncMock(return_value=mock_purge_result)

    mock_channel = AsyncMock()
    mock_channel.get_queue = AsyncMock(return_value=mock_queue)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(broker_manager, "get_channel", AsyncMock(return_value=mock_channel))
        purged = await admin.purge_dlq()
        assert purged == 15
        mock_queue.purge.assert_awaited_once()
