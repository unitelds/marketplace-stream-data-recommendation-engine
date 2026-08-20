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

import asyncio
from datetime import datetime, timezone
from typing import Optional

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
from src.module.metrics import metrics
from src.module.user_history_loader import ensure_user_history, is_loaded

try:
    from config import EVENTS_INFER_TIMEOUT
except ImportError:
    EVENTS_INFER_TIMEOUT = 8.0

router = APIRouter(prefix="/api/v1", tags=["recommendations"])


# ---------------------------------------------------------------------------
# Concurrent recommendation helper
# ---------------------------------------------------------------------------


async def _run_recs_concurrent(
    affected: dict[str, Optional[str]],
    timeout: float = EVENTS_INFER_TIMEOUT,
) -> list[dict]:
    """Run recommend() for every affected user concurrently in the thread pool.

    Returns partial results for users whose recs completed within `timeout`.
    Users that time out are silently dropped — their events are still stored.
    scipy/numpy TF-IDF ops release the GIL so executor threads run in true parallel.
    """
    if not affected:
        return []
    loop = asyncio.get_event_loop()
    futures: dict[asyncio.Future, str] = {
        loop.run_in_executor(
            None,
            lambda acc=acc, tx=tx: recommend(acc, context_taxon_id=tx),
        ): acc
        for acc, tx in affected.items()
    }
    done, pending = await asyncio.wait(set(futures), timeout=timeout)
    if pending:
        metrics.record_infer_timeout(len(pending))
        logger.warning(
            f"Events infer timeout ({timeout}s): "
            f"{len(pending)}/{len(futures)} users skipped inline recs"
        )
    results = []
    for fut in done:
        try:
            results.append(fut.result())
        except Exception as exc:
            logger.debug(f"Recommendation error (non-critical): {exc}")
    return results


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

# Per-worker JSONL log files — same pattern as metrics worker snapshots so the
# GET /logs/* endpoints can aggregate across all gunicorn workers.
import glob as _glob
import json as _logjson
import re as _re

_MONGO_ID_RE = _re.compile(r'^[a-f0-9]{24}$')


def _is_valid_account_id(account_id: str) -> bool:
    """Shop API requires a 24-char hex MongoDB ObjectId."""
    return bool(account_id and _MONGO_ID_RE.match(account_id))


_LOG_TMP = "/tmp"
_INGEST_PFX = "toki_ingest_log_"
_PUSH_PFX = "toki_push_log_"
_LOG_MAX_LINES = 600  # rotate file above this many lines

_EVENT_LOG_DIR = "logs"
_EVENT_LOG_PFX = "toki_event_log_"
_EVENT_LOG_MAX_LINES = 400  # keep last N lines per worker file


def _write_event_log(entry: dict) -> None:
    """Append one normalised event record to logs/toki_event_log_{pid}.jsonl."""
    import os as _os

    path = f"{_EVENT_LOG_DIR}/{_EVENT_LOG_PFX}{_os.getpid()}.jsonl"
    try:
        _os.makedirs(_EVENT_LOG_DIR, exist_ok=True)
        with open(path, "a") as fh:
            fh.write(_logjson.dumps(entry, ensure_ascii=False) + "\n")
        if _os.path.getsize(path) > 160_000:
            with open(path) as fh:
                lines = fh.readlines()
            with open(path, "w") as fh:
                fh.writelines(lines[-_EVENT_LOG_MAX_LINES:])
    except Exception:
        pass


def _read_event_logs(limit: int) -> tuple[list[dict], int]:
    entries: list[dict] = []
    for path in _glob.glob(f"{_EVENT_LOG_DIR}/{_EVENT_LOG_PFX}*.jsonl"):
        try:
            with open(path) as fh:
                for raw in fh:
                    raw = raw.strip()
                    if raw:
                        try:
                            entries.append(_logjson.loads(raw))
                        except Exception:
                            pass
        except Exception:
            pass
    entries.sort(key=lambda x: x.get("ts", ""), reverse=True)
    return entries[:limit], len(entries)


def _write_log(prefix: str, entry: dict) -> None:
    import os as _os

    path = f"{_LOG_TMP}/{prefix}{_os.getpid()}.jsonl"
    try:
        with open(path, "a") as fh:
            fh.write(_logjson.dumps(entry) + "\n")
        # Rotate: keep only last _LOG_MAX_LINES when file grows large
        if _os.path.getsize(path) > 120_000:
            with open(path) as fh:
                lines = fh.readlines()
            with open(path, "w") as fh:
                fh.writelines(lines[-_LOG_MAX_LINES:])
    except Exception:
        pass


def _read_logs(prefix: str, limit: int) -> tuple[list[dict], int]:
    """Aggregate from all worker JSONL files, sort newest-first."""
    entries: list[dict] = []
    for path in _glob.glob(f"{_LOG_TMP}/{prefix}*.jsonl"):
        try:
            with open(path) as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        try:
                            entries.append(_logjson.loads(line))
                        except Exception:
                            pass
        except Exception:
            pass
    entries.sort(key=lambda x: x.get("ts", ""), reverse=True)
    return entries[:limit], len(entries)


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
    activity_counts: dict[str, int] = {}

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
            activity_counts[event.activity_name] = (
                activity_counts.get(event.activity_name, 0) + 1
            )
            _write_event_log(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "source": "events",
                    "account_id": event.account_id,
                    "activity_name": event.activity_name,
                    "activity_data": event.activity_data,
                    "session_id": event.session_id,
                    "user_agent": (event.user_agent or "")[:120],
                }
            )
        except Exception as exc:
            logger.warning(f"Event error [{event.activity_name}]: {exc}")
            failed += 1

    metrics.record_ingestion(
        processed=processed, failed=failed, activity_counts=activity_counts
    )
    _write_log(
        _INGEST_PFX,
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "source": "events",
            "processed": processed,
            "failed": failed,
            "users": list(affected.keys())[:20],
            "event_types": activity_counts,
        },
    )
    recommendations: list[RecommendationResult] = []
    for result in await _run_recs_concurrent(affected):
        recommendations.append(RecommendationResult(**result))
        if result["recommendations"]:
            metrics.record_recommendations(
                count=len(result["recommendations"]),
                strategy=result["strategy"],
                endpoint="events",
                device=result.get("device", "unknown"),
            )
            background_tasks.add_task(
                _log_delivered,
                result["id"],
                result["recommendations"],
                result["strategy"],
            )
    # Push fresh multi-taxon feed to marketplace for every affected valid account
    for acc in affected:
        background_tasks.add_task(_bg_push_feed_for_user, acc)
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
    activity_counts: dict[str, int] = {}

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
            activity_counts[row.event_name] = activity_counts.get(row.event_name, 0) + 1
            _write_event_log(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "source": "consumer-events",
                    "account_id": row.account_id,
                    "event_name": row.event_name,
                    "event_value": row.event_value,
                    "session_id": row.session_id,
                    "user_agent": (row.user_agent or "")[:120],
                }
            )
        except Exception as exc:
            logger.warning(f"Consumer event error [{row.event_name}]: {exc}")
            failed += 1

    metrics.record_ingestion(
        processed=processed, failed=failed, activity_counts=activity_counts
    )
    _write_log(
        _INGEST_PFX,
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "source": "consumer-events",
            "processed": processed,
            "failed": failed,
            "users": list(affected.keys())[:20],
            "event_types": activity_counts,
        },
    )
    recommendations: list[RecommendationResult] = []
    for result in await _run_recs_concurrent(affected):
        recommendations.append(RecommendationResult(**result))
        if result["recommendations"]:
            metrics.record_recommendations(
                count=len(result["recommendations"]),
                strategy=result["strategy"],
                endpoint="consumer_events",
                device=result.get("device", "unknown"),
            )
            background_tasks.add_task(
                _log_delivered,
                result["id"],
                result["recommendations"],
                result["strategy"],
            )
    # Push fresh multi-taxon feed to marketplace for every affected valid account
    for acc in affected:
        background_tasks.add_task(_bg_push_feed_for_user, acc)
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
        background_tasks.add_task(
            _auto_push_feed, req.account_id, taxon_feeds, result.get("strategy", "")
        )

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


def _auto_push_feed(
    account_id: str, taxon_feeds: list[TaxonFeedItem], strategy: str = ""
) -> None:
    """
    Background task: deliver a feed to the marketplace recommendation API.

    Gated on PUSH_ENABLED (TOKI_PUSH_ENABLED), which also gates the scheduled
    top-users loop — so one switch stops all *automatic* delivery. The explicit
    POST /api/v1/feed/push endpoint is unaffected and always attempts delivery.
    """
    try:
        from config import PUSH_ENABLED
    except ImportError:
        PUSH_ENABLED = False
    if not PUSH_ENABLED:
        logger.debug(f"Auto-push disabled (TOKI_PUSH_ENABLED=false) [{account_id}]")
        return
    if not _is_valid_account_id(account_id):
        logger.debug(
            f"Auto-push skipped: accountId '{account_id}' is not a 24-char hex ObjectId"
        )
        return

    import asyncio as _asyncio

    from src.module import marketplace_push

    push_url = marketplace_push.target_url()
    loop = _asyncio.new_event_loop()
    try:
        status_, error, count = loop.run_until_complete(
            marketplace_push.push(account_id, taxon_feeds)
        )
    except Exception as exc:  # never let a push failure escape a background task
        status_, error, count = "failed", str(exc)[:200], 0
    finally:
        loop.run_until_complete(marketplace_push.aclose())
        loop.close()

    if status_ == "ok":
        logger.info(f"Feed auto-pushed to {push_url}: {count} products")
    _write_log(
        _PUSH_PFX,
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "account_id": account_id,
            "products_count": count,
            "strategy": strategy,
            "push_url": push_url,
            "push_status": status_,
            "push_error": error,
        },
    )


def _bg_push_feed_for_user(account_id: str) -> None:
    """Background: regenerate multi-taxon feed and push to marketplace after event ingestion."""
    ts = datetime.now(timezone.utc).isoformat()
    if not _is_valid_account_id(account_id):
        _write_log(
            _PUSH_PFX,
            {
                "ts": ts,
                "account_id": account_id,
                "products_count": 0,
                "strategy": "",
                "push_url": None,
                "push_status": "skipped",
                "push_error": f"invalid account_id (not a 24-char hex ObjectId): {account_id!r}",
            },
        )
        logger.debug(f"Push skipped — invalid account_id: {account_id!r}")
        return
    result = recommend_multi_taxon(account_id, top_taxons=3, top_n_per_taxon=10)
    taxon_feeds = [TaxonFeedItem(**tf) for tf in result.get("taxon_feeds", [])]
    strategy = result.get("strategy", "")
    if not taxon_feeds:
        # catalog_not_ready is the only legitimate empty-feed case
        reason = (
            "catalog not ready — retry after sync"
            if strategy == "catalog_not_ready"
            else f"no products returned (strategy={strategy!r})"
        )
        _write_log(
            _PUSH_PFX,
            {
                "ts": ts,
                "account_id": account_id,
                "products_count": 0,
                "strategy": strategy,
                "push_url": None,
                "push_status": "skipped",
                "push_error": reason,
            },
        )
        logger.debug(f"Push skipped [{account_id}] — {reason}")
        return
    # cold-start users get hash-rotated popular products (strategy="popular"); push those too
    _auto_push_feed(account_id, taxon_feeds, strategy)


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

    from src.module import marketplace_push

    push_url = req.shop_feed_url or marketplace_push.target_url()
    push_status, push_error = "not_attempted", None

    if push_url and taxon_feeds:
        if not _is_valid_account_id(req.account_id):
            push_status = "skipped"
            push_error = (
                f"accountId '{req.account_id}' must be a 24-char hex string "
                "(MongoDB ObjectId)"
            )
            logger.warning(push_error)
        else:
            push_status, push_error, pushed_count = await marketplace_push.push(
                req.account_id,
                taxon_feeds,
                url=push_url,
                timeout=req.push_timeout_seconds,
            )
            if push_status == "ok":
                logger.info(f"Feed pushed to {push_url}: {pushed_count} products")
            _write_log(
                _PUSH_PFX,
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "account_id": req.account_id,
                    "products_count": pushed_count,
                    "strategy": result.get("strategy"),
                    "push_url": push_url,
                    "push_status": push_status,
                    "push_error": push_error,
                },
            )

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


# ---------------------------------------------------------------------------
# GET /api/v1/logs/ingest  &  GET /api/v1/logs/push
# ---------------------------------------------------------------------------


@router.get("/logs/ingest", summary="Recent ingest log", response_model=dict)
async def get_ingest_log(limit: int = 50) -> dict:
    entries, total = _read_logs(_INGEST_PFX, limit)
    return {"entries": entries, "total_stored": total}


@router.get("/logs/push", summary="Recent push log", response_model=dict)
async def get_push_log(limit: int = 50) -> dict:
    entries, total = _read_logs(_PUSH_PFX, limit)
    return {"entries": entries, "total_stored": total}


@router.get("/logs/events", summary="Latest shop events log", response_model=dict)
async def get_events_log(limit: int = 100) -> dict:
    """Latest individual events received from the marketplace, newest first."""
    entries, total = _read_event_logs(limit)
    return {"entries": entries, "total_stored": total}
