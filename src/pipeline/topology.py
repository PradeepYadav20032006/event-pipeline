import logging
import aio_pika
from aio_pika.abc import AbstractRobustChannel
from src.pipeline.config import get_settings

logger = logging.getLogger("event_pipeline.topology")


class TopologyManager:
    """
    Declares the resilient RabbitMQ exchange-queue topology:
    1. Main Ring: events.direct -> orders.process (active processing queue)
    2. Retry Ring: events.retry -> orders.retry.queue (TTL queue that dead-letters back to events.direct)
    3. Dead Letter Ring: events.dlx -> orders.dlq (quarantine queue for unrecoverable errors)
    """

    def __init__(self):
        self.settings = get_settings()

    async def setup_topology(self, channel: AbstractRobustChannel):
        """Idempotently declares all exchanges, queues, bindings, and dead-letter routing arguments."""
        logger.info("Declaring resilient RabbitMQ topology...")

        # -------------------------------------------------------------
        # 1. Dead Letter Exchange (DLX) & Dead Letter Queue (DLQ)
        # -------------------------------------------------------------
        dlx_exchange = await channel.declare_exchange(
            self.settings.DLX_EXCHANGE,
            aio_pika.ExchangeType.DIRECT,
            durable=True
        )

        dlq_queue = await channel.declare_queue(
            self.settings.DLQ_QUEUE,
            durable=True
        )
        # Bind DLQ to DLX for all order routing keys
        await dlq_queue.bind(dlx_exchange, routing_key=self.settings.ROUTING_KEY_ORDER_CREATED)
        await dlq_queue.bind(dlx_exchange, routing_key=self.settings.ROUTING_KEY_ORDER_CANCELLED)
        logger.info("Dead Letter Ring configured: %s -> %s", self.settings.DLX_EXCHANGE, self.settings.DLQ_QUEUE)

        # -------------------------------------------------------------
        # 2. Main Direct Exchange & Main Processing Queue
        # -------------------------------------------------------------
        main_exchange = await channel.declare_exchange(
            self.settings.MAIN_EXCHANGE,
            aio_pika.ExchangeType.DIRECT,
            durable=True
        )

        main_queue = await channel.declare_queue(
            self.settings.MAIN_QUEUE,
            durable=True,
            arguments={
                # If a message is explicitly rejected with requeue=False, RabbitMQ automatically routes to DLX
                "x-dead-letter-exchange": self.settings.DLX_EXCHANGE,
            }
        )
        await main_queue.bind(main_exchange, routing_key=self.settings.ROUTING_KEY_ORDER_CREATED)
        await main_queue.bind(main_exchange, routing_key=self.settings.ROUTING_KEY_ORDER_CANCELLED)
        logger.info("Main Processing Ring configured: %s -> %s", self.settings.MAIN_EXCHANGE, self.settings.MAIN_QUEUE)

        # -------------------------------------------------------------
        # 3. Retry Exchange & TTL Backoff Queue
        # -------------------------------------------------------------
        retry_exchange = await channel.declare_exchange(
            self.settings.RETRY_EXCHANGE,
            aio_pika.ExchangeType.DIRECT,
            durable=True
        )

        retry_queue = await channel.declare_queue(
            self.settings.RETRY_QUEUE,
            durable=True,
            arguments={
                # Messages resting in the retry queue expire after TTL and are routed BACK to main exchange
                "x-dead-letter-exchange": self.settings.MAIN_EXCHANGE,
                "x-message-ttl": self.settings.RETRY_BASE_TTL_MS,
            }
        )
        await retry_queue.bind(retry_exchange, routing_key=self.settings.ROUTING_KEY_ORDER_CREATED)
        await retry_queue.bind(retry_exchange, routing_key=self.settings.ROUTING_KEY_ORDER_CANCELLED)
        logger.info("Retry Ring configured: %s -> %s (TTL: %d ms)", self.settings.RETRY_EXCHANGE, self.settings.RETRY_QUEUE, self.settings.RETRY_BASE_TTL_MS)

        return {
            "main_exchange": main_exchange,
            "main_queue": main_queue,
            "retry_exchange": retry_exchange,
            "retry_queue": retry_queue,
            "dlx_exchange": dlx_exchange,
            "dlq_queue": dlq_queue,
        }


topology_manager = TopologyManager()
