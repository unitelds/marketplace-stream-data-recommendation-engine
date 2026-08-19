"""
Handset feed client: fetches per-user personalized device recommendations
from the external Marketplace catalogue API.

  GET {MARKETPLACE_API_BASE_URL}/{account_id}

Expected response (keys match HANDSET_FEED_MAP column-name style):
  {
    "HANDSET_PROD_ID":   ["productId1", ...],
    "TABLET_PROD_ID":    [...],
    "WATCH_PROD_ID":     [...],
    "EARBUDS_PROD_ID":   [...],
    "ACCESSORY_PROD_ID": [...],
    "CPE_PROD_ID":       [...],
  }

This module normalises the response into taxon-slug keys and caches per account_id.
Falls back to {} on any error or timeout — callers must handle the empty-dict case.
"""

from __future__ import annotations

import time
from typing import Optional

import httpx
from loguru import logger

try:
    from config import (
        HANDSET_FEED_CACHE_SIZE,
        HANDSET_FEED_CACHE_TTL,
        HANDSET_FEED_MAP,
        MARKETPLACE_API_BASE_URL,
        MARKETPLACE_API_TIMEOUT,
    )
except ImportError:
    MARKETPLACE_API_BASE_URL = (
        "https://staging-marketplace.toki.mn/ms/catalogue/v1/recommendation"
    )
    MARKETPLACE_API_TIMEOUT = 2
    HANDSET_FEED_MAP = {
        "handset-cellphone": "HANDSET_PROD_ID",
        "tablet": "TABLET_PROD_ID",
        "watch-and-smart-watches": "WATCH_PROD_ID",
        "headphones-earphones": "EARBUDS_PROD_ID",
        "handset-accessory": "ACCESSORY_PROD_ID",
        "cpe": "CPE_PROD_ID",
    }
    HANDSET_FEED_CACHE_TTL = 3600
    HANDSET_FEED_CACHE_SIZE = 10000

# Reverse map: "ACCESSORY_PROD_ID" → "handset-accessory"
_FIELD_TO_TAXON_SLUG: dict[str, str] = {v: k for k, v in HANDSET_FEED_MAP.items()}

# Which taxon slugs are considered "accessory / companion" for a handset product
ACCESSORY_TAXON_SLUGS: tuple[str, ...] = (
    "handset-accessory",
    "headphones-earphones",
    "watch-and-smart-watches",
)

# account_id → (monotonic_timestamp, {taxon_slug: [product_id, ...]})
_cache: dict[str, tuple[float, dict[str, list[str]]]] = {}


async def fetch_handset_feed(account_id: str) -> dict[str, list[str]]:
    """
    Return {taxon_slug: [product_id, ...]} for all device categories.

    Hits the external Marketplace catalogue API and normalises the response.
    Result is cached per account for HANDSET_FEED_CACHE_TTL seconds.
    Returns {} on any network error, non-200, or malformed payload.
    """
    now = time.monotonic()
    cached = _cache.get(account_id)
    if cached and now - cached[0] < HANDSET_FEED_CACHE_TTL:
        return cached[1]

    try:
        async with httpx.AsyncClient(timeout=MARKETPLACE_API_TIMEOUT) as client:
            resp = await client.get(f"{MARKETPLACE_API_BASE_URL}/{account_id}")
            resp.raise_for_status()
            raw: dict = resp.json()
    except Exception as exc:
        logger.debug(f"Handset feed unavailable for {account_id}: {exc}")
        return {}

    result: dict[str, list[str]] = {}
    for field, products in raw.items():
        if not isinstance(products, list):
            continue
        slug = _FIELD_TO_TAXON_SLUG.get(field)
        if slug:
            result[slug] = [p for p in products if isinstance(p, str) and p]

    _cache[account_id] = (now, result)
    # Evict oldest entry when cache is full
    if len(_cache) > HANDSET_FEED_CACHE_SIZE:
        oldest = min(_cache, key=lambda k: _cache[k][0])
        del _cache[oldest]

    return result


def filter_feed_by_taxon(
    feed: dict[str, list[str]],
    taxon_slug: str,
    exclude_ids: set[str],
    top_n: int,
) -> list[str]:
    """Return up to top_n product IDs from feed[taxon_slug] not in exclude_ids."""
    return [pid for pid in feed.get(taxon_slug, []) if pid not in exclude_ids][:top_n]


def get_all_taxon_slugs() -> list[str]:
    """All taxon slugs covered by the external handset feed."""
    return list(HANDSET_FEED_MAP.keys())
