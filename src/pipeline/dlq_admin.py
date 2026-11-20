import asyncio
import json
import logging
import sys
from typing import List, Dict, Any, Optional
import aio_pika
from aio_pika import Message, DeliveryMode
from src.pipeline.broker import broker_manager
from src.pipeline.config import get_settings

logger = logging.getLogger("event_pipeline.dlq_admin")


class DLQAdminManager:
    """
    Operational tooling for Dead Letter Queue (DLQ) triage:
    - Queue Depth Status: Live count of messages in Main, Retry, and DLQ.
    - Inspect: Non-destructively peek into quarantined messages and error diagnostics.
    - Replay: Re-queue quarantined messages back into the main pipeline with cleared retry counters.
    - Purge: Discard known unrecoverable poison pills.
    """

    def __init__(self):
        self.settings = get_settings()

    async def get_queue_status(self) -> Dict[str, int]:
        """Returns message counts for all pipeline queues."""
        channel = await broker_manager.get_channel()
        main_q = await channel.declare_queue(self.settings.MAIN_QUEUE, passive=True)
        retry_q = await channel.declare_queue(self.settings.RETRY_QUEUE, passive=True)
        dlq_q = await channel.declare_queue(self.settings.DLQ_QUEUE, passive=True)

        return {
            "main_queue_depth": main_q.declaration_result.message_count,
            "retry_queue_depth": retry_q.declaration_result.message_count,
            "dlq_depth": dlq_q.declaration_result.message_count,
        }

    async def inspect_dlq(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Non-destructively peeks into quarantined messages in the DLQ.
        Messages are retrieved and rejected with requeue=True so they remain safely in the DLQ.
        """
        channel = await broker_manager.get_channel()
        dlq = await channel.get_queue(self.settings.DLQ_QUEUE)
        inspected = []

        for _ in range(limit):
            message = await dlq.get(no_ack=False, fail=False)
            if not message:
                break

            try:
                payload = json.loads(message.body.decode("utf-8"))
            except Exception:
                payload = message.body.decode("utf-8", errors="replace")

            info = {
                "message_id": message.message_id,
                "correlation_id": message.correlation_id,
                "routing_key": message.routing_key,
                "death_reason": message.headers.get("x-death-reason", "unknown"),
                "retry_count": message.headers.get("x-retry-count", 0),
                "payload": payload,
            }
            inspected.append(info)

            # Requeue back into DLQ so inspection is read-only
            await message.reject(requeue=True)

        return inspected

    async def replay_dlq(self, limit: int = 100) -> int:
        """
        Replays quarantined messages from the DLQ back into the Main Exchange.
        Resets retry metadata to allow clean downstream reprocessing.
        """
        channel = await broker_manager.get_channel()
        dlq = await channel.get_queue(self.settings.DLQ_QUEUE)
        main_exchange = await channel.get_exchange(self.settings.MAIN_EXCHANGE)

        replayed_count = 0

        for _ in range(limit):
            message = await dlq.get(no_ack=False, fail=False)
            if not message:
                break

            try:
                # Reset retry count in payload if JSON
                try:
                    body_dict = json.loads(message.body.decode("utf-8"))
                    body_dict["retry_count"] = 0
                    replayed_body = json.dumps(body_dict).encode("utf-8")
                except Exception:
                    replayed_body = message.body

                headers = dict(message.headers or {})
                headers["x-retry-count"] = 0
                headers["x-replayed-from-dlq"] = "true"
                # Strip death reason on replay
                headers.pop("x-death-reason", None)

                replayed_msg = Message(
                    body=replayed_body,
                    delivery_mode=DeliveryMode.PERSISTENT,
                    content_type=message.content_type,
                    content_encoding=message.content_encoding,
                    message_id=message.message_id,
                    correlation_id=message.correlation_id,
                    headers=headers
                )

                routing_key = message.routing_key or self.settings.ROUTING_KEY_ORDER_CREATED

                # Publish back to main exchange
                await main_exchange.publish(replayed_msg, routing_key=routing_key)

                # ACK the message from DLQ once confirmed published
                await message.ack()
                replayed_count += 1
                logger.info("Replayed message %s from DLQ to %s", message.message_id, routing_key)

            except Exception as exc:
                logger.error("Failed to replay message %s: %s", message.message_id, exc)
                # Requeue back to DLQ if replay publish failed
                await message.reject(requeue=True)
                break

        return replayed_count

    async def purge_dlq(self) -> int:
        """Purges all messages from the Dead Letter Queue."""
        channel = await broker_manager.get_channel()
        dlq = await channel.get_queue(self.settings.DLQ_QUEUE)
        result = await dlq.purge()
        logger.info("Purged %d messages from %s", result.message_count, self.settings.DLQ_QUEUE)
        return result.message_count


dlq_admin = DLQAdminManager()


async def main():
    """Command-line entrypoint for operator triage."""
    if len(sys.argv) < 2:
        print("Usage: python -m src.pipeline.dlq_admin [status|inspect|replay|purge]")
        sys.exit(1)

    cmd = sys.argv[1].lower()
    settings = get_settings()
    broker_manager.init(settings.RABBITMQ_URL)

    try:
        if cmd == "status":
            status_data = await dlq_admin.get_queue_status()
            print("\n=== RabbitMQ Pipeline Queue Depths ===")
            for k, v in status_data.items():
                print(f"  {k}: {v}")
        elif cmd == "inspect":
            messages = await dlq_admin.inspect_dlq(limit=10)
            print(f"\n=== Quarantined DLQ Messages (Found: {len(messages)}) ===")
            for m in messages:
                print(json.dumps(m, indent=2))
        elif cmd == "replay":
            count = await dlq_admin.replay_dlq(limit=100)
            print(f"\nSuccessfully replayed {count} messages from DLQ to {settings.MAIN_EXCHANGE}.")
        elif cmd == "purge":
            count = await dlq_admin.purge_dlq()
            print(f"\nPurged {count} messages from DLQ.")
        else:
            print(f"Unknown command: {cmd}")
    finally:
        await broker_manager.close()


if __name__ == "__main__":
    asyncio.run(main())
