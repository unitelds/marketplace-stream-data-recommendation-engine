"""
Scheduled top-users recommendation push.

Every ``PUSH_TOP_USERS_INTERVAL_SECONDS`` this loop:
  1. takes the top ``PUSH_TOP_USERS_COUNT`` most active users from the feature store
  2. generates a personalised multi-taxon feed for each, in the thread pool
  3. delivers them to the marketplace via :mod:`src.module.marketplace_push`

Only ONE gunicorn worker executes the cycle; the others skip via a shared
file-lock timestamp (no Redis needed — same host, same filesystem).

Config (all overridable via env vars):
  TOKI_PUSH_ENABLED             true | false  (default: true)
  TOKI_MARKETPLACE_PUSH_URL     full push endpoint (auto-selected by TOKI_ENV)
  TOKI_MARKETPLACE_PUSH_TOKEN   bearer token — delivery 401s without it
  TOKI_PUSH_TOP_USERS           1000          (users per cycle)
  TOKI_PUSH_INTERVAL            600           (seconds between cycles)
  TOKI_PUSH_BATCH_SIZE          50            (concurrent generations per wave)
"""

from __future__ import annotations

import asyncio
import os
import re
import time
from datetime import datetime, timezone

from loguru import logger

from src.module import marketplace_push

try:
    from config import (
        PUSH_ENABLED,
        PUSH_TOP_USERS_BATCH_SIZE,
        PUSH_TOP_USERS_COUNT,
        PUSH_TOP_USERS_INTERVAL_SECONDS,
    )
except ImportError:  # pragma: no cover
    PUSH_ENABLED = True
    PUSH_TOP_USERS_COUNT = 1000
    PUSH_TOP_USERS_INTERVAL_SECONDS = 600
    PUSH_TOP_USERS_BATCH_SIZE = 50

# File used to coordinate which gunicorn worker runs the push cycle
_LOCK_FILE = "/tmp/toki_push_leader.lock"
_MONGO_ID_RE = re.compile(r"^[a-f0-9]{24}$")


def _claim_leader() -> bool:
    """Return True if this worker wins the leader election for this cycle."""
    try:
        now = time.time()
        if os.path.exists(_LOCK_FILE):
            age = now - os.path.getmtime(_LOCK_FILE)
            # Another worker ran recently — skip this cycle
            if age < PUSH_TOP_USERS_INTERVAL_SECONDS - 30:
                return False
        with open(_LOCK_FILE, "w") as fh:
            fh.write(str(os.getpid()))
        os.utime(_LOCK_FILE, (now, now))
        return True
    except Exception:
        return False


async def _push_cycle(top_n: int, push_url: str, batch_size: int) -> None:
    """Generate and deliver feeds for the top N active users."""
    from src.api.routes.events import _PUSH_PFX, _write_log
    from src.module.feature_store import store
    from src.module.hybrid_ranker import recommend_multi_taxon

    if not store.catalog_ready:
        logger.warning("Scheduled push skipped — catalog not ready")
        return

    users = [u for u in store.get_top_users(top_n) if _MONGO_ID_RE.match(u)]
    if not users:
        logger.info("Scheduled push: no active users yet")
        return

    logger.info(f"Scheduled push start — {len(users)} users → {push_url}")
    ts_start = time.monotonic()
    ok_count = fail_count = skip_count = 0
    loop = asyncio.get_running_loop()

    for i in range(0, len(users), batch_size):
        batch = users[i : i + batch_size]

        # Generate feeds concurrently in the thread pool
        feeds = await asyncio.gather(
            *(
                loop.run_in_executor(
                    None,
                    lambda uid=uid: recommend_multi_taxon(
                        uid, top_taxons=3, top_n_per_taxon=10
                    ),
                )
                for uid in batch
            ),
            return_exceptions=True,
        )

        items: list[tuple[str, list[dict]]] = []
        for uid, feed in zip(batch, feeds):
            if isinstance(feed, BaseException) or not feed:
                skip_count += 1
                continue
            taxon_feeds = feed.get("taxon_feeds", [])
            if not taxon_feeds:
                skip_count += 1
                continue
            items.append((uid, taxon_feeds))

        if not items:
            continue

        results = await marketplace_push.push_many(
            items, url=push_url, concurrency=batch_size
        )
        now_iso = datetime.now(timezone.utc).isoformat()
        for uid, status, error, count in results:
            if status == "ok":
                ok_count += 1
            else:
                fail_count += 1
            _write_log(
                _PUSH_PFX,
                {
                    "ts": now_iso,
                    "account_id": uid,
                    "products_count": count,
                    "strategy": "scheduled_top_users",
                    "push_url": push_url,
                    "push_status": status,
                    "push_error": error,
                },
            )

    elapsed = time.monotonic() - ts_start
    logger.info(
        f"Scheduled push done — ok={ok_count} fail={fail_count} skip={skip_count} "
        f"in {elapsed:.1f}s  ({len(users)} users, {push_url})"
    )


async def run_scheduled_push_loop(
    interval_seconds: int = PUSH_TOP_USERS_INTERVAL_SECONDS,
    top_n: int = PUSH_TOP_USERS_COUNT,
    push_url: str | None = None,
    batch_size: int = PUSH_TOP_USERS_BATCH_SIZE,
) -> None:
    """
    Background coroutine: push feeds for top N users on every interval.
    Only the worker that wins ``_claim_leader()`` runs the cycle; the others
    sleep and check again next interval.
    """
    if not PUSH_ENABLED:
        logger.info("Scheduled push disabled (TOKI_PUSH_ENABLED=false)")
        return

    push_url = push_url or marketplace_push.target_url()
    if not marketplace_push.is_configured():
        logger.warning(
            "Scheduled push: TOKI_MARKETPLACE_PUSH_TOKEN is not set — "
            f"{push_url} will reject every delivery with 401"
        )

    logger.info(
        f"Scheduled push loop started — every {interval_seconds}s, "
        f"top {top_n} users → {push_url}"
    )
    # Initial stagger so workers don't all fire on startup
    await asyncio.sleep(interval_seconds)

    while True:
        if _claim_leader():
            try:
                await _push_cycle(top_n, push_url, batch_size)
            except Exception as exc:
                logger.error(f"Scheduled push cycle error: {exc}")
        else:
            logger.debug("Scheduled push: another worker is leader this cycle")
        await asyncio.sleep(interval_seconds)
