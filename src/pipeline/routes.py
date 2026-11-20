import uuid
import logging
from fastapi import APIRouter, status, HTTPException, Header
from src.pipeline.schemas import OrderCreateRequest, OrderEventResponse, EventEnvelope, EventType
from src.pipeline.publisher import event_publisher

logger = logging.getLogger("event_pipeline.routes")

router = APIRouter(prefix="/api/v1", tags=["Events"])


@router.post(
    "/orders",
    response_model=OrderEventResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Publish an Order Created event"
)
async def create_order_event(
    order: OrderCreateRequest,
    x_correlation_id: str | None = Header(default=None)
):
    """
    Ingests an order and dispatches an asynchronous event to RabbitMQ.
    Returns HTTP 202 Accepted upon broker confirmation.
    """
    correlation_id = x_correlation_id or str(uuid.uuid4())

    event = EventEnvelope(
        event_id=str(uuid.uuid4()),
        correlation_id=correlation_id,
        event_type=EventType.ORDER_CREATED,
        source="order-ingress-api",
        payload={
            "customer_id": order.customer_id,
            "customer_email": order.customer_email,
            "items": order.items,
            "total_amount": order.total_amount,
        }
    )

    try:
        await event_publisher.publish(event=event)
    except Exception as exc:
        logger.error("Failed to publish order event to message broker: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Message broker publishing failed: {str(exc)}"
        )

    return OrderEventResponse(
        status="accepted",
        event_id=event.event_id,
        correlation_id=event.correlation_id,
        event_type=event.event_type
    )
