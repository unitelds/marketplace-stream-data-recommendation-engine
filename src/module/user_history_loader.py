"""
Real-time user history loader from Oracle.

When a user's feed is requested but they have no in-memory interaction history,
this module queries Oracle consumer_events by ACCOUNTID (indexed column, fast)
to build their interaction profile before the ranker runs.

Pattern: lazy-load on first /feed or /feed/push request per user.
Subsequent requests are served from the in-memory feature store.

Security: account_id is validated as a 24-char hex MongoDB ObjectID before
being interpolated into SQL. Any non-conforming ID skips the Oracle query.
"""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta
from typing import Optional

from loguru import logger

from src.module.event_processor import normalize_consumer_event
from src.module.feature_store import store

# ── State ──────────────────────────────────────────────────────────────────────
_loaded_users: set[str] = set()  # accounts whose Oracle history has been fetched
_LOOKBACK_DAYS = 30
# Validate MongoDB ObjectID format (24 hex chars) before SQL interpolation
_OBJECT_ID_RE = re.compile(r"^[0-9a-fA-F]{24}$")


# ── Public API ─────────────────────────────────────────────────────────────────


async def ensure_user_history(account_id: str, timeout: float = 3.0) -> bool:
    """
    Guarantee that a user's Oracle engagement history is loaded into the
    in-memory feature store before the ranker runs.

    Returns True if the user already had history or data was loaded.
    Returns False if Oracle is unavailable, timed out, or no data exists.
    """
    # Already loaded or has interaction data from live stream events
    if account_id in _loaded_users:
        return True
    if store.get_user_top_products(account_id, top_n=1):
        _loaded_users.add(account_id)
        return True
    if not store.catalog_ready:
        return False

    try:
        norms = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, _query_oracle, account_id),
            timeout=timeout,
        )
        for norm in norms:
            await _apply(norm)
        _loaded_users.add(account_id)
        if norms:
            logger.info(
                f"Oracle history loaded: {len(norms)} events for {account_id[:12]}... "
                f"→ {len(store.get_user_top_products(account_id, top_n=50))} product signals"
            )
        return bool(norms)
    except asyncio.TimeoutError:
        logger.debug(
            f"Oracle history load timed out ({timeout}s) for {account_id[:12]}..."
        )
        return False
    except Exception as exc:
        logger.debug(f"Oracle history load skipped for {account_id[:12]}...: {exc}")
        return False


async def _apply(norm: dict) -> None:
    """Push a single normalized event into the feature store."""
    account_id = norm.get("account_id")
    product_id = norm.get("product_id")
    taxon_id = norm.get("taxon_id")
    intent_weight = norm.get("intent_weight", 0.0)

    # Resolve taxon from catalog if missing from event
    if not taxon_id and product_id:
        taxon_id = store.product_features.get(product_id, {}).get("taxon_id")

    await store.update_session(
        account_id,
        taxon_id=taxon_id,
        product_id=product_id,
        intent_weight=intent_weight,
    )
    if account_id and product_id and intent_weight != 0:
        await store.increment_user_item_score(account_id, product_id, intent_weight)


# ── Oracle query (runs in thread pool executor) ────────────────────────────────


def _query_oracle(account_id: str) -> list[dict]:
    """
    Synchronous Oracle query for a user's recent consumer_events.

    Queries consumer_events only (ACCOUNTID is a proper indexed column).
    Returns a list of normalized event dicts ready for feature store ingestion.
    """
    # Validate account_id before SQL interpolation (prevent injection)
    if not _OBJECT_ID_RE.match(account_id):
        logger.debug(f"Skipping Oracle query: invalid account_id format '{account_id}'")
        return []

    norms: list[dict] = []
    try:
        from src.module.database import oracle_import  # lazy import — Oracle optional

        lookback = (datetime.now() - timedelta(days=_LOOKBACK_DAYS)).strftime("%Y%m%d")

        # consumer_events — ACCOUNTID is an indexed column: fast single-user lookup
        events_df = oracle_import(f"""
            SELECT EVENTNAME, EVENTVALUE, ACCOUNTID, SESSIONID, USERAGENT, TIMESTAMP_
            FROM toki.marketplace_consumer_EVENTS
            WHERE ACCOUNTID = '{account_id}'
              AND P_DATE >= '{lookback}'
            ORDER BY TIMESTAMP_ DESC
            FETCH FIRST 300 ROWS ONLY
            """)

        if not events_df.empty:
            for _, row in events_df.iterrows():
                row_norms = normalize_consumer_event(
                    event_name=str(row.get("EVENTNAME", "")),
                    event_value_raw=row.get("EVENTVALUE"),
                    account_id=str(row.get("ACCOUNTID", "")),
                    session_id=str(row.get("SESSIONID") or ""),
                    user_agent=str(row.get("USERAGENT") or ""),
                    taxon_label_map=store.taxon_label_map,
                    event_timestamp=None,
                )
                norms.extend(row_norms)

    except ImportError:
        logger.debug("Oracle driver not available — skipping history load")
    except Exception as exc:
        logger.debug(f"Oracle query error for {account_id[:12]}...: {exc}")

    return norms


def is_loaded(account_id: str) -> bool:
    """Check whether Oracle history has been fetched for this account."""
    return account_id in _loaded_users


def invalidate(account_id: str) -> None:
    """Force a re-fetch on the next request for this account."""
    _loaded_users.discard(account_id)
