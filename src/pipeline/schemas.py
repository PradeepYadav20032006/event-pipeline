import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field, ConfigDict


class EventType(str, Enum):
    """Standardized event domain types across the event stream."""
    ORDER_CREATED = "order.created"
    ORDER_CANCELLED = "order.cancelled"
    PAYMENT_PROCESSED = "payment.processed"
    INVENTORY_RESERVED = "inventory.reserved"


class EventEnvelope(BaseModel):
    """
    Standardized CloudEvents-compliant event envelope.
    Guarantees consistent metadata for tracing, retries, and dead-letter quarantine.
    """
    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Globally unique identifier for this specific event."
    )
    correlation_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Distributed correlation ID linking user request to downstream processing."
    )
    event_type: EventType = Field(
        ...,
        description="Domain routing event type."
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC generation timestamp."
    )
    source: str = Field(
        default="order-service",
        description="Originating microservice identifier."
    )
    retry_count: int = Field(
        default=0,
        ge=0,
        description="Current retry attempts experienced by this message."
    )
    payload: Dict[str, Any] = Field(
        ...,
        description="Domain-specific event data payload."
    )

    model_config = ConfigDict(
        use_enum_values=True
    )


class OrderCreateRequest(BaseModel):
    """Customer order creation payload schema."""
    customer_id: str = Field(..., description="Unique customer ID")
    customer_email: str = Field(..., description="Customer contact email")
    items: list[Dict[str, Any]] = Field(..., min_length=1, description="List of purchased line items")
    total_amount: float = Field(..., gt=0, description="Total order monetary value")


class OrderEventResponse(BaseModel):
    """API response acknowledging published event receipt."""
    status: str = "accepted"
    event_id: str
    correlation_id: str
    event_type: str
