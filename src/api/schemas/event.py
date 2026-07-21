"""
Pydantic v2 request/response schemas.

POST /api/v1/events  — ingest raw shop stream events
POST /api/v1/infer   — on-demand inference for a known user
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

# ─── Incoming event payloads ──────────────────────────────────────────────────


class ActivityDataPayload(BaseModel):
    """Flexible activity data — fields vary by event type."""

    product_id: Optional[str] = None
    action: Optional[str] = None
    quantity: Optional[int] = Field(default=1, ge=0)
    price: Optional[float] = None
    taxon_id: Optional[str] = None
    session_id: Optional[str] = None
    timestamp: Optional[datetime] = None

    model_config = {"extra": "allow"}  # Accept any additional fields from shops


class StreamEvent(BaseModel):
    """A single engagement event from a shop's data stream."""

    event_id: Optional[str] = None
    account_id: str = Field(..., min_length=1, description="User account ID")
    session_id: Optional[str] = None
    activity_name: str = Field(
        ...,
        description=(
            "One of: order-events, limit-events, cart-events, "
            "wishlist-events, view_product, taxon_click"
        ),
    )
    # activity_data can be a dict OR a raw string (Oracle format)
    activity_data: Any = Field(
        default=None,
        description="Event payload — dict or JSON/Python-literal string",
    )
    user_agent: Optional[str] = None
    p_date: Optional[str] = None
    timestamp: Optional[datetime] = None

    @field_validator("activity_name")
    @classmethod
    def validate_activity_name(cls, v: str) -> str:
        allowed = {
            "order-events",
            "limit-events",
            "cart-events",
            "wishlist-events",
            "view_product",
            "taxon_click",
        }
        if v not in allowed:
            # Accept but normalise unknown types instead of hard-rejecting
            pass
        return v


class EventsBatchRequest(BaseModel):
    """Batch of events posted by a shop."""

    shop_id: Optional[str] = None
    events: list[StreamEvent] = Field(..., min_length=1, max_length=500)


# ─── Inference request ────────────────────────────────────────────────────────


class InferContext(BaseModel):
    """Optional context provided with an on-demand inference call."""

    current_taxon_id: Optional[str] = None
    cart_product_ids: list[str] = Field(default_factory=list)
    limit_checked: bool = False
    device_type: str = Field(
        default="mobile",
        description="mobile | desktop | miniprogram | unknown",
    )


class InferRequest(BaseModel):
    """On-demand per-user inference request."""

    account_id: str = Field(..., min_length=1)
    session_id: Optional[str] = None
    context: InferContext = Field(default_factory=InferContext)
    top_n: int = Field(default=10, ge=1, le=50)
    exclude_product_ids: list[str] = Field(default_factory=list)


# ─── Recommendation response ──────────────────────────────────────────────────


class RecommendationResult(BaseModel):
    """Recommendation result for a single user."""

    id: str = Field(..., description="account_id")
    taxon_id: Optional[str] = None
    recommendations: list[str] = Field(description="Ordered list of product_ids")
    strategy: str = Field(
        description="hybrid | cbf | taxon_cbf | popular | catalog_not_ready"
    )
    intent_score: float = 0.0
    device: str = "unknown"
    count: int = 0
    served_at: datetime = Field(default_factory=datetime.utcnow)


class EventsResponse(BaseModel):
    """Response to POST /api/v1/events."""

    status: str = "accepted"
    processed: int
    failed: int = 0
    recommendations: list[RecommendationResult] = Field(default_factory=list)


class InferResponse(RecommendationResult):
    """Response to POST /api/v1/infer (extends RecommendationResult)."""

    pass
