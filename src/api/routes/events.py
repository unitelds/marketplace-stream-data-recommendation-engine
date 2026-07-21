"""
POST /api/v1/events  — ingest shop engagement stream events
POST /api/v1/infer   — on-demand per-user inference

Event processing pipeline per request:
  1. Parse & validate with Pydantic
  2. Normalize each event (event_processor)
  3. Update user session state (FeatureStore)
  4. Update user–item interaction scores
  5. Run hybrid inference for unique account_ids
  6. Return recommendations synchronously
  7. Log delivered recommendations in background (fire-and-forget)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, status
from loguru import logger

from src.api.schemas.event import (
    EventsBatchRequest,
    EventsResponse,
    InferRequest,
    InferResponse,
    RecommendationResult,
    StreamEvent,
)
from src.module.event_processor import (
    detect_device_type,
    normalize_consumer_event,
    normalize_event,
)
from src.module.feature_store import store
from src.module.hybrid_ranker import recommend

router = APIRouter(prefix="/api/v1", tags=["recommendations"])


# ─── Helper: process one normalized event into the feature store ──────────────


async def _apply_event(norm: dict) -> None:
    """Update session state and user-item scores from a normalized event dict."""
    account_id = norm.get("account_id")
    product_id = norm.get("product_id")
    taxon_id = norm.get("taxon_id")
    intent_weight = norm.get("intent_weight", 0.0)
    device_type = norm.get("device_type") or detect_device_type(norm.get("user_agent"))

    # If product has a taxon in catalog, resolve it
    if not taxon_id and product_id:
        feat = store.product_features.get(product_id, {})
        taxon_id = feat.get("taxon_id")

    await store.update_session(
        account_id,
        taxon_id=taxon_id,
        product_id=product_id,
        intent_weight=intent_weight,
        basket_add=product_id if norm.get("is_basket_add") else None,
        basket_remove=product_id if norm.get("is_basket_remove") else None,
        limit_checked=bool(norm.get("is_limit_check")),
        device_type=device_type,
    )

    # Accumulate user-item interaction score (skip purely taxonomic events)
    if account_id and product_id and intent_weight != 0:
        await store.increment_user_item_score(account_id, product_id, intent_weight)


_DELIVERY_TABLE_READY = False


def _ensure_delivery_table() -> bool:
    """Create delivery log table once; returns True on success."""
    global _DELIVERY_TABLE_READY
    if _DELIVERY_TABLE_READY:
        return True
    try:
        from sqlalchemy import create_engine, text

        from config import WRITE_DATABASE_URL

        engine = create_engine(WRITE_DATABASE_URL, connect_args={"connect_timeout": 5})
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS rec_engine_delivery_log (
                    id          BIGSERIAL PRIMARY KEY,
                    account_id  TEXT NOT NULL,
                    product_id  TEXT NOT NULL,
                    strategy    TEXT,
                    served_at   TIMESTAMPTZ DEFAULT now()
                )
            """))
        engine.dispose()
        _DELIVERY_TABLE_READY = True
        return True
    except Exception as exc:
        logger.debug(f"Delivery table create failed (non-critical): {exc}")
        return False


def _log_delivered(account_id: str, product_ids: list[str], strategy: str) -> None:
    """Background task: persist delivered recommendations to PG (best-effort)."""
    if not product_ids:
        return
    try:
        if not _ensure_delivery_table():
            return
        import pandas as pd
        from sqlalchemy import create_engine

        from config import WRITE_DATABASE_URL

        now = datetime.now(timezone.utc).isoformat()
        rows = [
            {
                "account_id": account_id,
                "product_id": pid,
                "strategy": strategy,
                "served_at": now,
            }
            for pid in product_ids
        ]
        df = pd.DataFrame(rows)
        engine = create_engine(WRITE_DATABASE_URL, connect_args={"connect_timeout": 5})
        with engine.begin() as conn:
            df.to_sql(
                "rec_engine_delivery_log",
                con=conn,
                if_exists="append",
                index=False,
                chunksize=200,
            )
        engine.dispose()
        logger.debug(f"Logged {len(product_ids)} delivered recs for {account_id}")
    except Exception as exc:
        logger.debug(f"Log delivered recs failed (non-critical): {exc}")


# ─── POST /api/v1/events ──────────────────────────────────────────────────────


@router.post(
    "/events",
    response_model=EventsResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest shop engagement stream events",
    description=(
        "Receives a batch of engagement events from a shop stream. "
        "Normalizes, scores, updates user sessions, and returns "
        "real-time recommendations per user."
    ),
)
async def ingest_events(
    payload: EventsBatchRequest,
    background_tasks: BackgroundTasks,
) -> EventsResponse:
    processed = 0
    failed = 0
    # Collect unique account_ids that need inference
    affected: dict[str, str] = {}  # account_id → context_taxon_id

    for event in payload.events:
        try:
            norm = _normalize_stream_event(event)
            if norm is None:
                failed += 1
                continue
            await _apply_event(norm)
            processed += 1
            acc = norm.get("account_id")
            if acc:
                # Track the last seen taxon for context in inference
                if norm.get("taxon_id"):
                    affected[acc] = norm["taxon_id"]
                elif acc not in affected:
                    affected[acc] = None
        except Exception as exc:
            logger.warning(f"Event processing error [{event.activity_name}]: {exc}")
            failed += 1

    # Run inference for each affected user
    recommendations: list[RecommendationResult] = []
    for account_id, context_taxon in affected.items():
        result = recommend(account_id, context_taxon_id=context_taxon)
        recommendations.append(RecommendationResult(**result))
        if result["recommendations"]:
            background_tasks.add_task(
                _log_delivered,
                account_id,
                result["recommendations"],
                result["strategy"],
            )

    return EventsResponse(
        status="accepted",
        processed=processed,
        failed=failed,
        recommendations=recommendations,
    )


# ─── POST /api/v1/infer ───────────────────────────────────────────────────────


@router.post(
    "/infer",
    response_model=InferResponse,
    status_code=status.HTTP_200_OK,
    summary="On-demand per-user recommendation inference",
    description=(
        "Run the full hybrid recommendation pipeline for a known user. "
        "Accepts optional session context (basket contents, current taxon, device type)."
    ),
)
async def infer(
    req: InferRequest,
    background_tasks: BackgroundTasks,
) -> InferResponse:
    ctx = req.context

    # Seed cart items and device type into the session before inference
    if (
        ctx.cart_product_ids
        or ctx.device_type
        or ctx.current_taxon_id
        or ctx.limit_checked
    ):
        for pid in ctx.cart_product_ids:
            await store.update_session(
                req.account_id,
                product_id=pid,
                basket_add=pid,
                limit_checked=ctx.limit_checked,
                device_type=ctx.device_type,
                taxon_id=ctx.current_taxon_id,
            )

    result = recommend(
        req.account_id,
        context_taxon_id=ctx.current_taxon_id,
        top_n=req.top_n,
        exclude_product_ids=req.exclude_product_ids,
    )

    if result["recommendations"]:
        background_tasks.add_task(
            _log_delivered,
            req.account_id,
            result["recommendations"],
            result["strategy"],
        )

    return InferResponse(**result)


# ─── Helper: normalize a StreamEvent ─────────────────────────────────────────


def _normalize_stream_event(event: StreamEvent) -> Optional[dict]:
    """
    Route normalization based on activity_name.

    taxon_click → normalize_consumer_event
    all others  → normalize_event (parses ACTIVITYDATA)
    """
    taxon_label_map = store.taxon_label_map

    if event.activity_name == "taxon_click":
        return normalize_consumer_event(
            event_name=event.activity_name,
            event_value_raw=event.activity_data,
            account_id=event.account_id,
            session_id=event.session_id,
            user_agent=event.user_agent,
            taxon_label_map=taxon_label_map,
            event_timestamp=event.timestamp,
        )
    else:
        # For structured activity events, account_id is in ACTIVITYDATA usually
        # but also accept it from the top-level field
        norm = normalize_event(
            activity_name=event.activity_name,
            activity_data_raw=event.activity_data,
            taxon_label_map=taxon_label_map,
            event_id=event.event_id,
            event_timestamp=event.timestamp,
        )
        # Override account_id with top-level if ACTIVITYDATA had none
        if norm and not norm.get("account_id") and event.account_id:
            norm["account_id"] = event.account_id
        if norm and not norm.get("session_id") and event.session_id:
            norm["session_id"] = event.session_id
        if norm and not norm.get("device_type") and event.user_agent:
            norm["device_type"] = detect_device_type(event.user_agent)
        return norm
