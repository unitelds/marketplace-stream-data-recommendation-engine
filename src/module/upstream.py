"""
Upstream feed clients — the two external recommendation sources on 10.21.60.94.

Both upstreams live on the same host and return the SAME response envelope:

    {
      "userId": "<account_id>",
      "taxonRecommendations": { "<taxon-slug>": ["<product_id>", ...], ... },
      ...                                  # 8018 adds rankedProducts/isPersonalized/...
    }

  ┌─ CATALOG_FEED ── Marketplace Catalog API (Handset-shop feed) ───────────────┐
  │  GET http://10.21.60.94:9000/marketplace/{account_id}                       │
  │  Exactly six device taxons:                                                 │
  │    handset-cellphone · tablet · watch-and-smart-watches ·                   │
  │    headphones-earphones · handset-accessory · cpe                           │
  │  Read-only service (OpenAPI exposes only /health, /ready, /marketplace/{id}) │
  └─────────────────────────────────────────────────────────────────────────────┘

  ┌─ SHOP_FEED ───── TOKI Shop Feed (legacy demographic model) ─────────────────┐
  │  GET http://10.21.60.94:8018/api/recommendations/{account_id}               │
  │  Full catalogue coverage (~80 taxons) incl. TV, kitchen, computer, gaming.  │
  │  Always answers — falls back to demographic cohorts for unknown users, so   │
  │  it is the cold-start bridge for the engagement-based engine.               │
  └─────────────────────────────────────────────────────────────────────────────┘

Design notes
------------
* One shared, pooled ``httpx.AsyncClient`` per event loop instead of building a
  fresh client (and TLS/TCP handshake) on every call.
* ``TTLCache`` uses an ``OrderedDict`` so eviction is O(1); the previous
  ``min(cache, key=...)`` scan was O(n) over up to 50 000 entries per insert.
* Single-flight coalescing: concurrent misses for the same account share one
  upstream request rather than stampeding it.
* Product IDs are validated against the local catalog before being served —
  roughly 9% of catalog-feed IDs are not present in
  ``marketplace_catalog_data_extended_version3``.

Every fetch degrades to an empty result on error; callers must handle ``{}``.
"""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from typing import Iterable, Optional

import httpx
from loguru import logger

try:
    from config import (
        CATALOG_FEED_CACHE_SIZE,
        CATALOG_FEED_CACHE_TTL,
        CATALOG_FEED_TIMEOUT,
        CATALOG_FEED_URL,
        DEVICE_TAXON_SLUGS,
        SHOP_FEED_CACHE_SIZE,
        SHOP_FEED_CACHE_TTL,
        SHOP_FEED_TIMEOUT,
        SHOP_FEED_URL,
        UPSTREAM_MAX_CONNECTIONS,
    )
except ImportError:  # pragma: no cover - config is always present in deployment
    CATALOG_FEED_URL = "http://10.21.60.94:9000/marketplace"
    CATALOG_FEED_TIMEOUT = 1.5
    CATALOG_FEED_CACHE_TTL = 3600
    CATALOG_FEED_CACHE_SIZE = 20_000
    SHOP_FEED_URL = "http://10.21.60.94:8018/api/recommendations"
    SHOP_FEED_TIMEOUT = 1.5
    SHOP_FEED_CACHE_TTL = 600
    SHOP_FEED_CACHE_SIZE = 50_000
    UPSTREAM_MAX_CONNECTIONS = 100
    DEVICE_TAXON_SLUGS = (
        "handset-cellphone",
        "tablet",
        "watch-and-smart-watches",
        "headphones-earphones",
        "handset-accessory",
        "cpe",
    )


# ── Shared pooled HTTP client ─────────────────────────────────────────────────
# Keyed by event loop so each gunicorn worker (and each test loop) gets its own.
_clients: dict[int, httpx.AsyncClient] = {}


def _http() -> httpx.AsyncClient:
    """Return the pooled AsyncClient bound to the running event loop."""
    key = id(asyncio.get_running_loop())
    client = _clients.get(key)
    if client is None or client.is_closed:
        client = httpx.AsyncClient(
            limits=httpx.Limits(
                max_connections=UPSTREAM_MAX_CONNECTIONS,
                max_keepalive_connections=UPSTREAM_MAX_CONNECTIONS // 2,
                keepalive_expiry=30.0,
            ),
            headers={"Accept": "application/json"},
        )
        _clients[key] = client
    return client


async def aclose() -> None:
    """Close pooled clients — called from the FastAPI lifespan shutdown hook."""
    for client in list(_clients.values()):
        if not client.is_closed:
            await client.aclose()
    _clients.clear()


# ── TTL + LRU cache ───────────────────────────────────────────────────────────


class TTLCache:
    """Bounded TTL cache with O(1) LRU eviction, keyed by account_id."""

    __slots__ = ("_data", "_ttl", "_max", "hits", "misses")

    def __init__(self, ttl_seconds: float, max_size: int) -> None:
        self._data: OrderedDict[str, tuple[float, dict]] = OrderedDict()
        self._ttl = ttl_seconds
        self._max = max_size
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[dict]:
        entry = self._data.get(key)
        if entry is None:
            self.misses += 1
            return None
        stamped_at, value = entry
        if time.monotonic() - stamped_at >= self._ttl:
            self._data.pop(key, None)
            self.misses += 1
            return None
        self._data.move_to_end(key)
        self.hits += 1
        return value

    def set(self, key: str, value: dict) -> None:
        self._data[key] = (time.monotonic(), value)
        self._data.move_to_end(key)
        while len(self._data) > self._max:
            self._data.popitem(last=False)

    def invalidate(self, key: str) -> None:
        self._data.pop(key, None)

    def clear(self) -> None:
        self._data.clear()

    def stats(self) -> dict:
        total = self.hits + self.misses
        return {
            "entries": len(self._data),
            "max_size": self._max,
            "ttl_seconds": self._ttl,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": round(self.hits / total, 4) if total else 0.0,
        }


# ── Response parsing ──────────────────────────────────────────────────────────


def parse_taxon_recommendations(payload: object) -> dict[str, list[str]]:
    """
    Normalise an upstream response into ``{taxon_slug: [product_id, ...]}``.

    Handles the live envelope (``taxonRecommendations``) plus the flat-list and
    ``{"products": [{"productId", "taxonId"}]}`` shapes that older upstream
    builds emitted, so a rollback on 9000/8018 does not blank the feed.
    """
    if not isinstance(payload, dict):
        return {}

    raw = payload.get("taxonRecommendations")
    if isinstance(raw, dict):
        out: dict[str, list[str]] = {}
        for slug, pids in raw.items():
            if not isinstance(pids, list) or not slug:
                continue
            clean = [str(p) for p in pids if isinstance(p, (str, int)) and p]
            if clean:
                out[str(slug)] = clean
        return out

    # Legacy: [{"productId": ..., "taxonId": ...}, ...] under products/recommendations
    items = payload.get("products") or payload.get("recommendations") or []
    if isinstance(items, list):
        out = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            pid, taxon = item.get("productId"), item.get("taxonId")
            if pid and taxon:
                out.setdefault(str(taxon), []).append(str(pid))
        return out
    return {}


# ── Feed client ───────────────────────────────────────────────────────────────


class FeedClient:
    """
    One upstream recommendation feed.

    ``fetch`` is async and network-backed; ``read_cache`` is a synchronous,
    cache-only read that is safe to call from thread-pool context (the hybrid
    ranker and the scheduled push both run there).
    """

    def __init__(
        self,
        name: str,
        base_url: str,
        timeout: float,
        cache_ttl: float,
        cache_size: int,
        allowed_slugs: Optional[Iterable[str]] = None,
    ) -> None:
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.cache = TTLCache(cache_ttl, cache_size)
        self.allowed_slugs = frozenset(allowed_slugs) if allowed_slugs else None
        self.errors = 0
        # account_id → in-flight task, so concurrent misses share one request
        self._inflight: dict[str, asyncio.Task] = {}

    def url_for(self, account_id: str) -> str:
        return f"{self.base_url}/{account_id}"

    async def fetch(self, account_id: str) -> dict[str, list[str]]:
        """Return ``{taxon_slug: [product_id, ...]}``; ``{}`` on any failure."""
        if not account_id:
            return {}
        cached = self.cache.get(account_id)
        if cached is not None:
            return cached

        task = self._inflight.get(account_id)
        if task is None:
            task = asyncio.ensure_future(self._fetch_uncached(account_id))
            self._inflight[account_id] = task
            task.add_done_callback(
                lambda _t, aid=account_id: self._inflight.pop(aid, None)
            )
        try:
            return await asyncio.shield(task)
        except Exception:
            return {}

    async def _fetch_uncached(self, account_id: str) -> dict[str, list[str]]:
        try:
            resp = await _http().get(self.url_for(account_id), timeout=self.timeout)
            resp.raise_for_status()
            feed = parse_taxon_recommendations(resp.json())
        except Exception as exc:
            self.errors += 1
            logger.warning(f"[{self.name}] feed unavailable for {account_id}: {exc}")
            return {}

        if self.allowed_slugs is not None:
            feed = {s: p for s, p in feed.items() if s in self.allowed_slugs}
        self.cache.set(account_id, feed)
        logger.debug(
            f"[{self.name}] {account_id}: {len(feed)} taxons, "
            f"{sum(len(v) for v in feed.values())} products"
        )
        return feed

    def read_cache(self, account_id: str) -> dict[str, list[str]]:
        """Cache-only read — zero latency, safe from a worker thread."""
        return self.cache.get(account_id) or {}

    def invalidate(self, account_id: str) -> None:
        self.cache.invalidate(account_id)

    def stats(self) -> dict:
        return {
            "name": self.name,
            "upstream": self.base_url,
            "timeout_seconds": self.timeout,
            "errors": self.errors,
            "inflight": len(self._inflight),
            **self.cache.stats(),
        }


# ── The two live upstreams ────────────────────────────────────────────────────

CATALOG_FEED = FeedClient(
    name="catalog-feed",
    base_url=CATALOG_FEED_URL,
    timeout=CATALOG_FEED_TIMEOUT,
    cache_ttl=CATALOG_FEED_CACHE_TTL,
    cache_size=CATALOG_FEED_CACHE_SIZE,
    allowed_slugs=DEVICE_TAXON_SLUGS,
)

SHOP_FEED = FeedClient(
    name="shop-feed",
    base_url=SHOP_FEED_URL,
    timeout=SHOP_FEED_TIMEOUT,
    cache_ttl=SHOP_FEED_CACHE_TTL,
    cache_size=SHOP_FEED_CACHE_SIZE,
)


async def fetch_both(account_id: str) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """
    Fetch both upstreams concurrently.

    Wall-clock cost is ``max(CATALOG_FEED_TIMEOUT, SHOP_FEED_TIMEOUT)``, not the
    sum. Either side independently degrades to ``{}``.
    """
    catalog, shop = await asyncio.gather(
        CATALOG_FEED.fetch(account_id),
        SHOP_FEED.fetch(account_id),
        return_exceptions=True,
    )
    return (
        catalog if isinstance(catalog, dict) else {},
        shop if isinstance(shop, dict) else {},
    )


# ── Selection helpers ─────────────────────────────────────────────────────────


def _known(product_id: str) -> bool:
    """True when the product exists in the synced catalog."""
    from src.module.feature_store import store

    return not store.catalog_ready or product_id in store.product_features


def select(
    feed: dict[str, list[str]],
    slug: str,
    exclude: set[str],
    top_n: int,
    *,
    require_in_stock: bool = False,
) -> list[str]:
    """
    Up to ``top_n`` product IDs from ``feed[slug]``, skipping excluded IDs and
    any ID missing from the local catalog (upstream can reference delisted SKUs).
    """
    from src.module.feature_store import store

    out: list[str] = []
    for pid in feed.get(slug, ()):
        if len(out) >= top_n:
            break
        if pid in exclude or not _known(pid):
            continue
        if require_in_stock and store.product_features.get(pid, {}).get("stock", 0) <= 0:
            continue
        out.append(pid)
    return out


def select_many(
    feed: dict[str, list[str]],
    slugs: Iterable[str],
    exclude: set[str],
    top_n: int,
    *,
    require_in_stock: bool = False,
) -> list[str]:
    """Round-robin across ``slugs`` so no single taxon monopolises the slots."""
    per_slug = [
        select(feed, s, exclude, top_n, require_in_stock=require_in_stock)
        for s in slugs
    ]
    out: list[str] = []
    seen: set[str] = set()
    for tier in range(top_n):
        for products in per_slug:
            if tier < len(products) and products[tier] not in seen:
                seen.add(products[tier])
                out.append(products[tier])
                if len(out) >= top_n:
                    return out
    return out


def flatten(feed: dict[str, list[str]], exclude: set[str], top_n: int) -> list[str]:
    """Flat, de-duplicated, catalog-validated product list across all taxons."""
    return select_many(feed, list(feed.keys()), exclude, top_n)


def stats() -> dict:
    """Diagnostic snapshot for the health/dashboard endpoints."""
    return {"catalog_feed": CATALOG_FEED.stats(), "shop_feed": SHOP_FEED.stats()}
