import pytest
from httpx import ASGITransport, AsyncClient
from unittest.mock import AsyncMock, patch
from src.pipeline.main import app
from src.pipeline.broker import broker_manager, RabbitMQBrokerManager
from src.pipeline.config import get_settings


@pytest.mark.asyncio
async def test_health_check_healthy(monkeypatch):
    """Verifies that /health returns 200 when RabbitMQ ping succeeds."""
    monkeypatch.setattr(broker_manager, "ping", AsyncMock(return_value=True))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["dependencies"]["rabbitmq"] == "connected"


@pytest.mark.asyncio
async def test_health_check_degraded_when_rabbitmq_down(monkeypatch):
    """Verifies that /health returns 503 when RabbitMQ ping fails."""
    monkeypatch.setattr(broker_manager, "ping", AsyncMock(return_value=False))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "degraded"
        assert data["dependencies"]["rabbitmq"] == "disconnected"


@pytest.mark.asyncio
async def test_root_endpoint():
    """Verifies that root metadata returns service name and health endpoint."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "Resilient Event Pipeline" in data["service"]
        assert data["health_url"] == "/health"


@pytest.mark.asyncio
async def test_broker_manager_lifecycle():
    """Verifies initialization and close lifecycle of RabbitMQBrokerManager."""
    mgr = RabbitMQBrokerManager()
    mgr.init("amqp://guest:guest@localhost:5672/")
    assert mgr.is_connected is False
    assert await mgr.ping() is False

    mock_conn = AsyncMock()
    mock_conn.is_closed = False
    mock_channel = AsyncMock()
    mock_channel.is_closed = False
    mock_conn.channel = AsyncMock(return_value=mock_channel)

    with patch("aio_pika.connect_robust", AsyncMock(return_value=mock_conn)):
        conn = await mgr.connect()
        assert conn is mock_conn
        assert mgr.is_connected is True

        channel = await mgr.get_channel()
        assert channel is mock_channel
        assert await mgr.ping() is True

        await mgr.close()
        assert mgr.is_connected is False
