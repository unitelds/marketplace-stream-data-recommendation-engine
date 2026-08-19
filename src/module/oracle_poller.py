"""
Oracle event poller — background task that pulls new consumer_events rows from
toki.marketplace_consumer_EVENTS every ORACLE_POLL_INTERVAL_SECONDS and ingests
them into the recommendation engine so the staging app does not need to call our
endpoints directly.

Checkpoint (logs/.oracle_poll_checkpoint.json) tracks the last-seen TIMESTAMP_
so only genuinely new rows are processed on each cycle.  Multiple gunicorn workers
share the same checkpoint file; the first worker to finish a cycle writes the new
watermark, so occasional near-simultaneous polls may re-process the same row but
that is idempotent (intent-score updates converge).
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone

from loguru import logger

_CHECKPOINT_FILE = "logs/.oracle_poll_checkpoint.json"
_BATCH_SIZE = 500


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------


def _load_checkpoint() -> dict:
    try:
        with open(_CHECKPOINT_FILE) as fh:
            return json.load(fh)
    except Exception:
        return {}


def _save_checkpoint(data: dict) -> None:
    try:
        os.makedirs("logs", exist_ok=True)
        with open(_CHECKPOINT_FILE, "w") as fh:
            json.dump(data, fh)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Oracle fetch (blocking — called in thread-pool executor)
# ---------------------------------------------------------------------------


def _fetch_consumer_events_sync(since_pdate: str, since_ts: str) -> list[dict]:
    """Filter by P_DATE + TIMESTAMP_ in Oracle so FETCH FIRST only scans new rows."""
    try:
        from src.module.database import oracle_import

        # TIMESTAMP_ is VARCHAR2 in ISO format ('2026-08-17T00:39:27…').
        # String comparison works correctly on ISO-sorted timestamps.
        # Normalise the checkpoint to the same string shape Oracle stores.
        ts_str = since_ts[:19].replace(" ", "T")  # e.g. '2026-08-16T16:08:52'
        # Strip Z suffix from Oracle values before comparing
        ts_str = ts_str.rstrip("Z")

        df = oracle_import(
            "SELECT ID_, EVENTNAME, EVENTVALUE, ACCOUNTID, SESSIONID, "
            "USERAGENT, TIMESTAMP_, P_DATE "
            "FROM toki.marketplace_consumer_EVENTS "
            f"WHERE P_DATE >= '{since_pdate}' "
            f"AND SUBSTR(TIMESTAMP_, 1, 19) > '{ts_str}' "
            f"ORDER BY TIMESTAMP_ ASC "
            f"FETCH FIRST {_BATCH_SIZE} ROWS ONLY"
        )
        return df.to_dict(orient="records") if not df.empty else []
    except Exception as exc:
        logger.debug(f"Oracle consumer_events fetch failed: {exc}")
        return []


# ---------------------------------------------------------------------------
# Async poll cycle
# ---------------------------------------------------------------------------


async def _poll_cycle() -> None:
    loop = asyncio.get_event_loop()

    checkpoint = _load_checkpoint()
    now = datetime.now(timezone.utc)
    # Mongolia is UTC+8; use today's local date as the default partition filter
    # to avoid fetching thousands of rows from older partitions on first run
    mongolia_today = (now + timedelta(hours=8)).strftime("%Y%m%d")
    since_pdate = checkpoint.get("consumer_events_pdate", mongolia_today)
    # TIMESTAMP_ stored as UTC ISO strings ending in Z; default lookback = 24 h
    # to cover the full Mongolia business day (UTC+8 means today starts at 16:00 UTC yesterday)
    since_ts = checkpoint.get(
        "consumer_events_ts",
        (now - timedelta(hours=24)).isoformat(),
    )

    rows = await loop.run_in_executor(
        None, _fetch_consumer_events_sync, since_pdate, since_ts
    )
    if not rows:
        return

    logger.info(f"Oracle poll: {len(rows)} new consumer_events (pdate>={since_pdate})")

    # Advance watermark before processing so a crash doesn't re-replay the batch
    from src.api.routes.events import (
        _apply_event,
        _normalize_consumer_row,
        _write_event_log,
    )
    from src.api.schemas.event import ConsumerEventRow
    from src.module.metrics import metrics

    # Advance watermark — use newest TIMESTAMP_ from batch
    latest_ts_vals = [
        r.get("TIMESTAMP_") for r in rows if r.get("TIMESTAMP_") is not None
    ]
    if latest_ts_vals:
        latest = max(str(v) for v in latest_ts_vals)
        checkpoint["consumer_events_ts"] = latest
        # P_DATE is the Mongolia local date (UTC+8) derived from the UTC timestamp
        try:
            ts_utc = datetime.fromisoformat(latest.rstrip("Z").replace("Z", "+00:00"))
            if ts_utc.tzinfo is None:
                ts_utc = ts_utc.replace(tzinfo=timezone.utc)
            mongolia_date = (ts_utc + timedelta(hours=8)).strftime("%Y%m%d")
        except Exception:
            mongolia_date = latest[:10].replace("-", "")
        checkpoint["consumer_events_pdate"] = mongolia_date
        _save_checkpoint(checkpoint)

    processed = failed = 0
    affected: dict[str, str | None] = {}
    activity_counts: dict[str, int] = {}

    for r in rows:
        try:
            row = ConsumerEventRow.model_validate(r, strict=False)
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
                    "ts": now.isoformat(),
                    "source": "oracle-poll",
                    "account_id": row.account_id,
                    "event_name": row.event_name,
                    "event_value": row.event_value,
                    "session_id": row.session_id,
                    "user_agent": (row.user_agent or "")[:120],
                }
            )
        except Exception as exc:
            logger.debug(f"Oracle poll row processing error: {exc}")
            failed += 1

    metrics.record_ingestion(
        processed=processed, failed=failed, activity_counts=activity_counts
    )

    # Push fresh recommendations to marketplace for every valid affected account
    from src.api.routes.events import _bg_push_feed_for_user

    for acc in affected:
        loop.run_in_executor(None, _bg_push_feed_for_user, acc)

    logger.info(
        f"Oracle poll done: processed={processed} failed={failed} "
        f"users={len(affected)} activity={activity_counts}"
    )


# ---------------------------------------------------------------------------
# Background task entry point
# ---------------------------------------------------------------------------


async def run_oracle_poll_loop(interval_seconds: int = 60) -> None:
    """Async loop registered as a FastAPI lifespan background task."""
    logger.info(f"Oracle event poller started (interval={interval_seconds}s)")
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            await _poll_cycle()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug(f"Oracle poll cycle failed (non-critical): {exc}")
