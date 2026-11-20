import pytest
from unittest.mock import AsyncMock, MagicMock
from src.pipeline.topology import TopologyManager
from src.pipeline.config import get_settings


@pytest.mark.asyncio
async def test_topology_declaration_creates_exchanges_and_queues():
    """Verifies that all three rings (main, retry, dlx) are declared with correct parameters."""
    settings = get_settings()
    manager = TopologyManager()

    mock_channel = AsyncMock()

    # Mock exchanges and queues
    mock_exchange = AsyncMock()
    mock_queue = AsyncMock()
    mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
    mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

    topology = await manager.setup_topology(mock_channel)

    assert mock_channel.declare_exchange.await_count == 3
    assert mock_channel.declare_queue.await_count == 3

    # Check that retry queue has x-message-ttl and x-dead-letter-exchange
    queue_calls = mock_channel.declare_queue.await_args_list
    retry_call = [c for c in queue_calls if c.args[0] == settings.RETRY_QUEUE][0]
    assert retry_call.kwargs["arguments"]["x-dead-letter-exchange"] == settings.MAIN_EXCHANGE
    assert retry_call.kwargs["arguments"]["x-message-ttl"] == settings.RETRY_BASE_TTL_MS

    # Check that main queue has x-dead-letter-exchange set to DLX
    main_call = [c for c in queue_calls if c.args[0] == settings.MAIN_QUEUE][0]
    assert main_call.kwargs["arguments"]["x-dead-letter-exchange"] == settings.DLX_EXCHANGE
