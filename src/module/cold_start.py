"""
Cold-start bridge — the legacy TOKI Shop feed on port 8018.

    GET http://10.21.60.94:8018/api/recommendations/{account_id}

The legacy demographic model answers for *every* account (falling back to
demographic cohorts when it has no history), which makes it the seed source
whenever the engagement-based engine has nothing for a user.  It covers the
full ~80-taxon catalogue, not just devices.

Thin compatibility layer over :mod:`src.module.upstream`.

Historical note: the old parser looked for ``recommendations`` / ``products`` /
``productIds`` keys.  The live service returns ``taxonRecommendations``, so this
bridge silently returned ``[]`` for every user and no cold-start seeding ever
happened.
"""

from __future__ import annotations

from src.module.upstream import SHOP_FEED, flatten

try:
    from config import SHOP_FEED_URL
except ImportError:  # pragma: no cover
    SHOP_FEED_URL = "http://10.21.60.94:8018/api/recommendations"


def read_cache(account_id: str, top_n: int = 30) -> list[str]:
    """Synchronous cache-only read — safe from thread-pool context."""
    return flatten(SHOP_FEED.read_cache(account_id), set(), top_n)


async def fetch(account_id: str, top_n: int = 30) -> list[str]:
    """Async fetch of a flat, catalog-validated seed list across all taxons."""
    return flatten(await SHOP_FEED.fetch(account_id), set(), top_n)


async def fetch_by_taxon(account_id: str) -> dict[str, list[str]]:
    """Full ``{taxon_slug: [product_id, ...]}`` map from the legacy engine."""
    return await SHOP_FEED.fetch(account_id)


def invalidate(account_id: str) -> None:
    """Evict one user from the cache (call after N new high-intent events)."""
    SHOP_FEED.invalidate(account_id)


def is_cold_start(account_id: str) -> bool:
    """True when the engagement engine has no product history for this user."""
    from src.module.feature_store import store

    return not store.get_user_top_products(account_id, top_n=1)


def stats() -> dict:
    """Diagnostic snapshot of the legacy-feed cache."""
    return SHOP_FEED.stats()
