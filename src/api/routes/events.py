"""
API routes: event ingestion, inference, multi-taxon feed, and shop push-back.

Endpoints
---------
POST /api/v1/events           -- ingest customer_activities stream events
POST /api/v1/consumer-events  -- ingest Oracle consumer_events rows
POST /api/v1/infer            -- on-demand single-taxon inference
POST /api/v1/feed             -- multi-taxon feed for a user
POST /api/v1/feed/push        -- generate feed AND POST it to shop's API
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, BackgroundTasks, status
from loguru import logger

from src.api.schemas.event import (
    ConsumerEventRow,
    ConsumerEventsBatchRequest,
    EventsBatchRequest,
    EventsResponse,
    FeedPushRequest,
    FeedPushResponse,
    FeedRequest,
    InferRequest,
    InferResponse,
    MultiTaxonResponse,
    RecommendationResult,
    StreamEvent,
    TaxonFeedItem,
)
from src.module.event_processor import (
    detect_device_type,
    normalize_consumer_event,
    normalize_event,
)
from src.module.feature_store import store
from src.module.hybrid_ranker import recommend, recommend_multi_taxon
from src.module.user_history_loader import ensure_user_history, is_loaded

router = APIRouter(prefix="/api/v1", tags=["recommendations"])


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


async def _apply_event(norm: dict) -> None:
    """Persist one normalized event into the in-memory feature store."""
    account_id = norm.get("account_id")
    product_id = norm.get("product_id")
    taxon_id = norm.get("taxon_id")
    intent_weight = norm.get("intent_weight", 0.0)
    device_type = norm.get("device_type") or detect_device_type(norm.get("user_agent"))

    if not taxon_id and product_id:
        taxon_id = store.product_features.get(product_id, {}).get("taxon_id")

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
    if account_id and product_id and intent_weight != 0:
        await store.increment_user_item_score(account_id, product_id, intent_weight)


def _normalize_stream_event(event: StreamEvent) -> list[dict]:
    """Route normalization by activity_name; always returns a list."""
    taxon_label_map = store.taxon_label_map

    if event.activity_name in ("taxon_click", "product_click"):
        return normalize_consumer_event(
            event_name=event.activity_name,
            event_value_raw=event.activity_data,
            account_id=event.account_id,
            session_id=event.session_id,
            user_agent=event.user_agent,
            taxon_label_map=taxon_label_map,
            event_timestamp=event.timestamp,
        )

    norm = normalize_event(
        activity_name=event.activity_name,
        activity_data_raw=event.activity_data,
        taxon_label_map=taxon_label_map,
        event_id=event.event_id,
        event_timestamp=event.timestamp,
    )
    if not norm:
        return []
    if not norm.get("account_id") and event.account_id:
        norm["account_id"] = event.account_id
    if not norm.get("session_id") and event.session_id:
        norm["session_id"] = event.session_id
    if not norm.get("device_type") and event.user_agent:
        norm["device_type"] = detect_device_type(event.user_agent)
    return [norm]


def _normalize_consumer_row(row: ConsumerEventRow) -> list[dict]:
    return normalize_consumer_event(
        event_name=row.event_name,
        event_value_raw=row.event_value,
        account_id=row.account_id,
        session_id=row.session_id,
        user_agent=row.user_agent,
        taxon_label_map=store.taxon_label_map,
        event_timestamp=row.timestamp,
    )


# ---------------------------------------------------------------------------
# Delivery log — async queue writer (avoids thread-pool saturation)
# ---------------------------------------------------------------------------
# All recommendation handlers enqueue records here; a single background
# asyncio task drains the queue in batches.  This decouples DB writes from
# the request hot path and eliminates p95 latency spikes caused by per-request
# SQLAlchemy engine creation inside BackgroundTasks thread workers.

import asyncio as _asyncio
from collections import deque as _deque

_DELIVERY_QUEUE: _deque[dict] = _deque(maxlen=50_000)  # ~50k records in-RAM buffer
_DELIVERY_TABLE_READY = False
_DELIVERY_WRITER_RUNNING = False
_DELIVERY_BATCH_SIZE = 200
_DELIVERY_FLUSH_INTERVAL = 5.0  # seconds between flushes


def _enqueue_delivery(account_id: str, product_ids: list[str], strategy: str) -> None:
    """Non-blocking: append delivery records to in-memory queue."""
    now = datetime.now(timezone.utc).isoformat()
    for pid in product_ids:
        _DELIVERY_QUEUE.append(
            {
                "account_id": account_id,
                "product_id": pid,
                "strategy": strategy,
                "served_at": now,
            }
        )


# Keep the synchronous version as a fallback for external callers
def _log_delivered(account_id: str, product_ids: list[str], strategy: str) -> None:
    """BackgroundTask shim — just enqueues; the async writer handles the DB write."""
    _enqueue_delivery(account_id, product_ids, strategy)


async def _delivery_writer_loop() -> None:
    """Single coroutine that flushes _DELIVERY_QUEUE to PostgreSQL in batches."""
    global _DELIVERY_TABLE_READY
    import asyncio

    while True:
        await asyncio.sleep(_DELIVERY_FLUSH_INTERVAL)
        if not _DELIVERY_QUEUE:
            continue
        # Drain up to _DELIVERY_BATCH_SIZE records
        batch: list[dict] = []
        while _DELIVERY_QUEUE and len(batch) < _DELIVERY_BATCH_SIZE:
            batch.append(_DELIVERY_QUEUE.popleft())
        if not batch:
            continue
        # Run blocking DB write in executor so we don't block the event loop
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, _flush_batch_to_db, batch
            )
        except Exception as exc:
            logger.debug(f"Delivery flush failed (non-critical): {exc}")


def _flush_batch_to_db(batch: list[dict]) -> None:
    """Synchronous batch write — called in a thread-pool executor."""
    global _DELIVERY_TABLE_READY
    try:
        import pandas as pd
        from sqlalchemy import create_engine, text

        from config import WRITE_DATABASE_URL

        engine = create_engine(
            WRITE_DATABASE_URL,
            pool_size=1,
            max_overflow=0,
            connect_args={"connect_timeout": 5},
        )
        with engine.begin() as conn:
            if not _DELIVERY_TABLE_READY:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS rec_engine_delivery_log (
                        id         BIGSERIAL PRIMARY KEY,
                        account_id TEXT NOT NULL,
                        product_id TEXT NOT NULL,
                        strategy   TEXT,
                        served_at  TIMESTAMPTZ DEFAULT now()
                    )
                """))
                _DELIVERY_TABLE_READY = True
            pd.DataFrame(batch).to_sql(
                "rec_engine_delivery_log",
                con=conn,
                if_exists="append",
                index=False,
                chunksize=500,
            )
        engine.dispose()
        logger.debug(f"Delivery log: flushed {len(batch)} records")
    except Exception as exc:
        logger.debug(f"Delivery flush DB error (non-critical): {exc}")


# ---------------------------------------------------------------------------
# POST /api/v1/events
# ---------------------------------------------------------------------------


@router.post(
    "/events",
    response_model=EventsResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest customer_activities engagement events",
    description=(
        "Batch of order-events, cart-events, limit-events, wishlist-events, "
        "view_product, taxon_click, product_click. "
        "Returns single-taxon recommendations per affected user."
    ),
)
async def ingest_events(
    payload: EventsBatchRequest,
    background_tasks: BackgroundTasks,
) -> EventsResponse:
    processed, failed = 0, 0
    affected: dict[str, Optional[str]] = {}

    for event in payload.events:
        try:
            norms = _normalize_stream_event(event)
            if not norms:
                failed += 1
                continue
            for norm in norms:
                await _apply_event(norm)
                acc = norm.get("account_id")
                if acc:
                    affected[acc] = norm.get("taxon_id") or affected.get(acc)
            processed += 1
        except Exception as exc:
            logger.warning(f"Event error [{event.activity_name}]: {exc}")
            failed += 1

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


# ---------------------------------------------------------------------------
# POST /api/v1/consumer-events
# ---------------------------------------------------------------------------


@router.post(
    "/consumer-events",
    response_model=EventsResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest Oracle consumer_events rows",
    description=(
        "Accepts rows directly from the Oracle consumer_events table. "
        "Handles product_click (productIds[] + Mongolian taxon label) "
        "and taxon_click. Field aliases: EVENTNAME, EVENTVALUE, ACCOUNTID, "
        "SESSIONID, USERAGENT, TIMESTAMP_."
    ),
)
async def ingest_consumer_events(
    payload: ConsumerEventsBatchRequest,
    background_tasks: BackgroundTasks,
) -> EventsResponse:
    processed, failed = 0, 0
    affected: dict[str, Optional[str]] = {}

    for row in payload.events:
        try:
            norms = _normalize_consumer_row(row)
            if not norms:
                failed += 1
                continue
            for norm in norms:
                await _apply_event(norm)
                acc = norm.get("account_id")
                if acc:
                    affected[acc] = norm.get("taxon_id") or affected.get(acc)
            processed += 1
        except Exception as exc:
            logger.warning(f"Consumer event error [{row.event_name}]: {exc}")
            failed += 1

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


# ---------------------------------------------------------------------------
# POST /api/v1/infer
# ---------------------------------------------------------------------------


@router.post(
    "/infer",
    response_model=InferResponse,
    status_code=status.HTTP_200_OK,
    summary="On-demand single-taxon inference",
)
async def infer(
    req: InferRequest,
    background_tasks: BackgroundTasks,
) -> InferResponse:
    ctx = req.context
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


# ---------------------------------------------------------------------------
# POST /api/v1/feed
# ---------------------------------------------------------------------------


@router.post(
    "/feed",
    response_model=MultiTaxonResponse,
    status_code=status.HTTP_200_OK,
    summary="Multi-taxon recommendation feed",
    description=(
        "Returns per-taxon recommendations for a user. On first request for a "
        "user with no in-memory history, Oracle consumer_events is queried in "
        "real-time (up to 3 s) to load their engagement profile before ranking. "
        "After generating the feed, recommendations are pushed to the shop's "
        "configured feed endpoint in the background."
    ),
)
async def feed(
    req: FeedRequest,
    background_tasks: BackgroundTasks,
) -> MultiTaxonResponse:
    # Trigger Oracle history load in background (non-blocking).
    # First call returns hash-diversified cold-start results (<100ms).
    # Subsequent calls will be fully personalized once Oracle load completes.
    if not is_loaded(req.account_id) and not store.get_user_top_products(
        req.account_id, top_n=1
    ):
        background_tasks.add_task(_bg_load_oracle, req.account_id)

    result = recommend_multi_taxon(
        req.account_id,
        top_taxons=req.top_taxons,
        top_n_per_taxon=req.top_n_per_taxon,
        extra_taxon_ids=req.extra_taxon_ids or [],
        exclude_product_ids=req.exclude_product_ids or [],
    )
    all_pids = [
        pid
        for tf in result.get("taxon_feeds", [])
        for pid in tf.get("recommendations", [])
    ]
    if all_pids:
        background_tasks.add_task(
            _log_delivered, req.account_id, all_pids, result.get("strategy", "feed")
        )

    taxon_feeds = [TaxonFeedItem(**tf) for tf in result.get("taxon_feeds", [])]

    # Auto-push to shop feed endpoint in background (fire-and-forget)
    if taxon_feeds:
        background_tasks.add_task(_auto_push_feed, req.account_id, taxon_feeds)

    return MultiTaxonResponse(
        id=result["id"],
        taxon_feeds=taxon_feeds,
        total_products=result["total_products"],
        strategy=result["strategy"],
        intent_score=result["intent_score"],
        device=result["device"],
    )


# ---------------------------------------------------------------------------
# POST /api/v1/feed/push
# ---------------------------------------------------------------------------


def _bg_load_oracle(account_id: str) -> None:
    """
    Background task: load Oracle engagement history for a user.

    Runs in FastAPI's thread pool so it never blocks the event loop.
    Subsequent /feed requests after this completes will use real Oracle data.
    """
    import asyncio as _asyncio

    loop = _asyncio.new_event_loop()
    try:
        loop.run_until_complete(ensure_user_history(account_id, timeout=20.0))
    finally:
        loop.close()


def _auto_push_feed(account_id: str, taxon_feeds: list[TaxonFeedItem]) -> None:
    """Background task: push feed to shop's configured API endpoint."""
    try:
        from config import MARKETPLACE_API_BASE_URL, MARKETPLACE_API_TIMEOUT

        push_url = f"{MARKETPLACE_API_BASE_URL}/{account_id}"
        payload = {
            "id": account_id,
            "taxon_feeds": [
                {
                    "taxon_id": tf.taxon_id,
                    "taxon_name": tf.taxon_name,
                    "recommendations": tf.recommendations,
                }
                for tf in taxon_feeds
            ],
        }
        import httpx as _httpx

        with _httpx.Client(timeout=MARKETPLACE_API_TIMEOUT) as client:
            resp = client.post(push_url, json=payload)
            resp.raise_for_status()
        total = sum(len(tf.recommendations) for tf in taxon_feeds)
        logger.info(
            f"Feed auto-pushed to {push_url}: {total} products [{resp.status_code}]"
        )
    except Exception as exc:
        logger.debug(f"Feed auto-push failed (non-critical): {exc}")


@router.post(
    "/feed/push",
    response_model=FeedPushResponse,
    status_code=status.HTTP_200_OK,
    summary="Generate feed and push to shop API",
    description=(
        "Generates multi-taxon recommendations (loading Oracle history if needed) "
        "and synchronously POSTs them to the shop's configured feed endpoint. "
        "The response always contains recommendations regardless of push outcome."
    ),
)
async def feed_push(
    req: FeedPushRequest,
    background_tasks: BackgroundTasks,
) -> FeedPushResponse:
    # Trigger Oracle history load in background (non-blocking)
    if not is_loaded(req.account_id) and not store.get_user_top_products(
        req.account_id, top_n=1
    ):
        background_tasks.add_task(_bg_load_oracle, req.account_id)

    result = recommend_multi_taxon(
        req.account_id,
        top_taxons=req.top_taxons,
        top_n_per_taxon=req.top_n_per_taxon,
        extra_taxon_ids=req.extra_taxon_ids or [],
        exclude_product_ids=req.exclude_product_ids or [],
    )
    taxon_feeds = [TaxonFeedItem(**tf) for tf in result.get("taxon_feeds", [])]
    all_pids = [pid for tf in taxon_feeds for pid in tf.recommendations]
    if all_pids:
        background_tasks.add_task(
            _log_delivered, req.account_id, all_pids, result.get("strategy", "feed")
        )

    push_url = req.shop_feed_url
    if not push_url:
        try:
            from config import MARKETPLACE_API_BASE_URL

            push_url = f"{MARKETPLACE_API_BASE_URL}/{req.account_id}"
        except ImportError:
            push_url = None

    push_status, push_error = "not_attempted", None

    if push_url and taxon_feeds:
        push_payload = {
            "id": req.account_id,
            "taxon_feeds": [
                {
                    "taxon_id": tf.taxon_id,
                    "taxon_name": tf.taxon_name,
                    "recommendations": tf.recommendations,
                }
                for tf in taxon_feeds
            ],
        }
        try:
            async with httpx.AsyncClient(timeout=req.push_timeout_seconds) as client:
                resp = await client.post(push_url, json=push_payload)
                resp.raise_for_status()
            push_status = "ok"
            logger.info(f"Feed pushed to {push_url} [{resp.status_code}]")
        except Exception as exc:
            push_status = "failed"
            push_error = str(exc)[:200]
            logger.warning(f"Feed push failed [{push_url}]: {exc}")

    return FeedPushResponse(
        id=result["id"],
        taxon_feeds=taxon_feeds,
        total_products=result["total_products"],
        strategy=result["strategy"],
        intent_score=result["intent_score"],
        device=result["device"],
        push_status=push_status,
        push_url=push_url,
        push_error=push_error,
    )
