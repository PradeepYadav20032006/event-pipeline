import pytest
import uuid
from httpx import ASGITransport, AsyncClient
from unittest.mock import AsyncMock, patch, MagicMock
from src.pipeline.main import app
from src.pipeline.schemas import EventEnvelope, EventType
from src.pipeline.publisher import event_publisher


@pytest.mark.asyncio
async def test_event_envelope_validation_and_serialization():
    """Verifies CloudEvents envelope schema generation and validation."""
    event = EventEnvelope(
        event_type=EventType.ORDER_CREATED,
        payload={"order_id": "ord-123", "amount": 99.95}
    )
    assert event.event_id is not None
    assert event.correlation_id is not None
    assert event.event_type == "order.created"
    assert event.retry_count == 0
    assert event.payload["amount"] == 99.95

    # Serialization test
    serialized = event.model_dump_json()
    assert "order.created" in serialized
    assert "ord-123" in serialized


@pytest.mark.asyncio
async def test_publish_order_event_success(monkeypatch):
    """
    Verifies that POST /api/v1/orders returns HTTP 202 Accepted
    and dispatches a persistent message to the message broker.
    """
    mock_publish = AsyncMock(return_value=True)
    monkeypatch.setattr(event_publisher, "publish", mock_publish)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        custom_trace = "trace-client-order-999"
        response = await client.post(
            "/api/v1/orders",
            headers={"X-Correlation-ID": custom_trace},
            json={
                "customer_id": "cust-8821",
                "customer_email": "shopper@example.com",
                "items": [{"item_id": "prod-1", "quantity": 2, "price": 45.00}],
                "total_amount": 90.00
            }
        )

        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "accepted"
        assert data["correlation_id"] == custom_trace
        assert data["event_type"] == "order.created"
        assert data["event_id"] is not None

        # Verify publisher was called with correct envelope
        mock_publish.assert_awaited_once()
        published_event = mock_publish.await_args.kwargs["event"]
        assert published_event.correlation_id == custom_trace
        assert published_event.payload["customer_id"] == "cust-8821"
        assert published_event.payload["total_amount"] == 90.00


@pytest.mark.asyncio
async def test_publish_order_event_broker_failure(monkeypatch):
    """Verifies that broker failure returns HTTP 500 cleanly without unhandled crash."""
    mock_publish = AsyncMock(side_effect=RuntimeError("RabbitMQ broker connection dropped"))
    monkeypatch.setattr(event_publisher, "publish", mock_publish)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/orders",
            json={
                "customer_id": "cust-8821",
                "customer_email": "shopper@example.com",
                "items": [{"item_id": "prod-1", "quantity": 1, "price": 10.00}],
                "total_amount": 10.00
            }
        )
        assert response.status_code == 500
        assert "Message broker publishing failed" in response.json()["detail"]
