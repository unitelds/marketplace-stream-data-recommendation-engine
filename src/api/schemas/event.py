"""
Pydantic v2 request/response schemas.

Endpoints:
  POST /api/v1/events                    -- ingest customer_activities stream events
  POST /api/v1/consumer-events           -- ingest consumer_events rows (Oracle format)
  POST /api/v1/infer                     -- on-demand single-taxon inference
  POST /api/v1/feed                      -- multi-taxon feed for a user
  POST /api/v1/feed/push                 -- generate feed AND push to shop API
  POST /api/v1/recommendations/taxon     -- taxon/category page product grid
  POST /api/v1/recommendations/product   -- product detail page "similar products"
  POST /api/v1/recommendations/basket    -- basket/cart page cross-sell panel
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


# ---------------------------------------------------------------------------
# Placement-specific recommendation schemas
# ---------------------------------------------------------------------------


class TaxonPageRequest(BaseModel):
    """
    Taxon/category page product grid.

    The shop sends this when a user opens or scrolls a category page.
    Returns a personalized list of products within that taxon ordered by
    CBF + CF + popularity signals for the requesting user.
    """

    account_id: str = Field(..., min_length=1, description="User account ID")
    taxon_id: str = Field(
        ..., min_length=1, description="The taxon/category being viewed"
    )
    top_n: int = Field(default=20, ge=1, le=60, description="Max products to return")
    exclude_product_ids: list[str] = Field(
        default_factory=list,
        description="Product IDs already visible on the page (avoid duplicates)",
    )
    require_in_stock: bool = Field(default=True)


class ProductPageRequest(BaseModel):
    """
    Product detail page (PDP) — "You may also like" panel.

    The shop sends this when a user opens a product page.
    Returns similar products driven by TF-IDF cosine similarity from the
    anchor product's content vector, blended with CF co-interaction signals.
    """

    account_id: str = Field(..., min_length=1, description="User account ID")
    product_id: str = Field(
        ..., min_length=1, description="The product currently being viewed"
    )
    top_n: int = Field(
        default=10, ge=1, le=30, description="Max similar products to return"
    )
    exclude_product_ids: list[str] = Field(
        default_factory=list,
        description="Optional additional products to exclude from results",
    )
    require_in_stock: bool = Field(default=True)


class BasketPageRequest(BaseModel):
    """
    Basket/cart page — "Complete your purchase" cross-sell panel.

    The shop sends this when a user views their cart.
    Returns complementary products from categories not already in the basket,
    re-ranked by basket-aware cross-sell logic and the user's intent level.
    """

    account_id: str = Field(..., min_length=1, description="User account ID")
    basket_product_ids: list[str] = Field(
        ...,
        min_length=1,
        description="Product IDs currently in the user's basket",
    )
    top_n: int = Field(
        default=10, ge=1, le=30, description="Max cross-sell products to return"
    )
    exclude_product_ids: list[str] = Field(
        default_factory=list,
        description="Additional products to exclude beyond the basket contents",
    )
    require_in_stock: bool = Field(default=True)


class PlacementRecommendationResponse(BaseModel):
    """Unified response for all three placement recommendation endpoints."""

    account_id: str
    placement: str = Field(description="taxon_page | product_page | basket_page")
    recommendations: list[str]
    strategy: str
    intent_score: float = 0.0
    device: str = "unknown"
    count: int = 0
    # Context echoed back for easy correlation
    context_taxon_id: Optional[str] = None
    context_product_id: Optional[str] = None
    served_at: datetime = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Handset / device recommendation schemas
# ---------------------------------------------------------------------------


class HandsetAccessoriesRequest(BaseModel):
    """
    Handset product page — 'Compatible accessories' panel.

    Sent when a user opens a specific phone, tablet, or wearable product page.
    Returns accessories (cases, chargers, earphones, bands) that are compatible
    with or commonly bought alongside the anchor handset product.
    Blends the external Marketplace catalogue API feed with internal CBF + CF.
    """

    account_id: str = Field(..., min_length=1)
    handset_product_id: str = Field(
        ..., min_length=1, description="The handset/device product being viewed"
    )
    top_n: int = Field(default=10, ge=1, le=30)
    exclude_product_ids: list[str] = Field(default_factory=list)
    require_in_stock: bool = Field(default=True)
    accessory_taxon_ids: list[str] = Field(
        default_factory=list,
        description=(
            "Restrict results to these catalog taxon IDs. "
            "Empty = auto-resolve from accessory taxon slugs."
        ),
    )


class HandsetFeedRequest(BaseModel):
    """
    Per-user multi-taxon handset/device feed.

    Returns personalised recommendations across all device categories:
    phones, tablets, wearables, earphones, and accessories.
    Blends the external Marketplace catalogue API feed with internal
    CBF + popularity per taxon.
    """

    account_id: str = Field(..., min_length=1)
    taxon_slugs: list[str] = Field(
        default_factory=list,
        description=(
            "Device-category slugs to include "
            "(e.g. 'handset-cellphone', 'handset-accessory'). "
            "Empty = all categories from HANDSET_FEED_MAP."
        ),
    )
    top_n_per_taxon: int = Field(default=10, ge=1, le=30)
    exclude_product_ids: list[str] = Field(default_factory=list)
    require_in_stock: bool = Field(default=True)


class HandsetFeedTaxonItem(BaseModel):
    """Recommendations for a single device category taxon."""

    taxon_slug: str
    taxon_id: Optional[str] = None
    recommendations: list[str]
    count: int = 0
    source: str = "internal"  # "marketplace_api" | "internal" | "mixed"


class HandsetFeedResponse(BaseModel):
    """Multi-taxon handset/device feed response."""

    account_id: str
    taxon_feeds: list[HandsetFeedTaxonItem]
    total_products: int
    strategy: str
    intent_score: float = 0.0
    device: str = "unknown"
    served_at: datetime = Field(default_factory=datetime.utcnow)
