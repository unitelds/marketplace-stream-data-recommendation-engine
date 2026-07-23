"""
Pydantic v2 request/response schemas.

Endpoints:
  POST /api/v1/events          -- ingest customer_activities stream events
  POST /api/v1/consumer-events -- ingest consumer_events rows (Oracle format)
  POST /api/v1/infer           -- on-demand single-taxon inference
  POST /api/v1/feed            -- multi-taxon feed for a user
  POST /api/v1/feed/push       -- generate feed AND push to shop API
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Incoming event payloads
# ---------------------------------------------------------------------------

class ActivityDataPayload(BaseModel):
    """Flexible activity data; fields vary by event type."""
    product_id: Optional[str] = None
    action: Optional[str] = None
    quantity: Optional[int] = Field(default=1, ge=0)
    price: Optional[float] = None
    taxon_id: Optional[str] = None
    session_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    model_config = {"extra": "allow"}


class StreamEvent(BaseModel):
    """Single engagement event -- customer_activities format (shop stream)."""
    event_id: Optional[str] = None
    account_id: str = Field(..., min_length=1)
    session_id: Optional[str] = None
    activity_name: str = Field(
        ...,
        description=(
            "order-events | limit-events | cart-events | "
            "wishlist-events | view_product | taxon_click | product_click"
        ),
    )
    activity_data: Any = Field(
        default=None,
        description="Event payload -- dict or JSON/Python-literal string",
    )
    user_agent: Optional[str] = None
    p_date: Optional[str] = None
    timestamp: Optional[datetime] = None

    @field_validator("activity_name")
    @classmethod
    def validate_activity_name(cls, v: str) -> str:
        # Accept all; unknown types yield 0 weight but won't error
        return v


class EventsBatchRequest(BaseModel):
    """Batch of activity events (customer_activities format)."""
    shop_id: Optional[str] = None
    events: list[StreamEvent] = Field(..., min_length=1, max_length=500)


class ConsumerEventRow(BaseModel):
    """
    Single row from Oracle consumer_events table.

    Supports product_click (productIds + taxon) and taxon_click (taxon label).
    Field names match Oracle column names; aliases allow both cases.
    """
    event_id: Optional[str] = Field(None, alias="ID_")
    event_name: str = Field(..., alias="EVENTNAME")
    event_value: Any = Field(None, alias="EVENTVALUE")
    account_id: str = Field(..., alias="ACCOUNTID", min_length=1)
    session_id: Optional[str] = Field(None, alias="SESSIONID")
    user_agent: Optional[str] = Field(None, alias="USERAGENT")
    timestamp: Optional[datetime] = Field(None, alias="TIMESTAMP_")
    p_date: Optional[str] = Field(None, alias="P_DATE")
    model_config = {"populate_by_name": True, "extra": "allow"}


class ConsumerEventsBatchRequest(BaseModel):
    """Batch of consumer_events rows."""
    shop_id: Optional[str] = None
    events: list[ConsumerEventRow] = Field(..., min_length=1, max_length=500)


# ---------------------------------------------------------------------------
# Inference request
# ---------------------------------------------------------------------------

class InferContext(BaseModel):
    current_taxon_id: Optional[str] = None
    cart_product_ids: list[str] = Field(default_factory=list)
    limit_checked: bool = False
    device_type: str = Field(default="mobile")


class InferRequest(BaseModel):
    account_id: str = Field(..., min_length=1)
    session_id: Optional[str] = None
    context: InferContext = Field(default_factory=InferContext)
    top_n: int = Field(default=10, ge=1, le=50)
    exclude_product_ids: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Feed (multi-taxon) requests
# ---------------------------------------------------------------------------

class FeedRequest(BaseModel):
    """Request multi-taxon recommendations for a user."""
    account_id: str = Field(..., min_length=1)
    top_taxons: int = Field(default=3, ge=1, le=10)
    top_n_per_taxon: int = Field(default=10, ge=1, le=30)
    extra_taxon_ids: list[str] = Field(
        default_factory=list,
        description="Force-include these taxons even if not in user history",
    )
    exclude_product_ids: list[str] = Field(default_factory=list)


class FeedPushRequest(FeedRequest):
    """Generate multi-taxon feed AND POST it back to the shop's feed endpoint."""
    shop_feed_url: Optional[str] = Field(
        None,
        description="Override shop feed endpoint. Default: MARKETPLACE_API_BASE_URL/{account_id}",
    )
    push_timeout_seconds: float = Field(default=3.0, ge=0.5, le=30.0)


# ---------------------------------------------------------------------------
# Recommendation responses
# ---------------------------------------------------------------------------

class RecommendationResult(BaseModel):
    """Single-taxon recommendation result."""
    id: str = Field(..., description="account_id")
    taxon_id: Optional[str] = None
    recommendations: list[str]
    strategy: str
    intent_score: float = 0.0
    device: str = "unknown"
    count: int = 0
    served_at: datetime = Field(default_factory=datetime.utcnow)


class TaxonFeedItem(BaseModel):
    """Recommendations for a single taxon."""
    taxon_id: str
    taxon_name: Optional[str] = None
    recommendations: list[str]
    count: int = 0
    score: float = 0.0


class MultiTaxonResponse(BaseModel):
    """Multi-taxon feed response."""
    id: str = Field(..., description="account_id")
    taxon_feeds: list[TaxonFeedItem]
    total_products: int
    strategy: str
    intent_score: float = 0.0
    device: str = "unknown"
    served_at: datetime = Field(default_factory=datetime.utcnow)


class FeedPushResponse(MultiTaxonResponse):
    """Multi-taxon feed response with shop-push delivery status."""
    push_status: str = "not_attempted"  # "ok" | "failed" | "not_attempted"
    push_url: Optional[str] = None
    push_error: Optional[str] = None


class EventsResponse(BaseModel):
    """Response to POST /events or /consumer-events."""
    status: str = "accepted"
    processed: int
    failed: int = 0
    recommendations: list[RecommendationResult] = Field(default_factory=list)


class InferResponse(RecommendationResult):
    """Response to POST /infer."""
    pass
