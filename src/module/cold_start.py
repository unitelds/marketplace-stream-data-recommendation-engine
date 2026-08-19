"""
Cold-start bridge: per-user recommendations from the former rec engine.

  GET http://10.21.60.94:8018/api/recommendations/{user_id}

Used as seed products across ALL placement endpoints and the hybrid ranker
whenever the core engine has no interaction history for a user.  Responses
are cached in-process (per gunicorn worker) to avoid upstream load.

Two access patterns:
  async fetch(account_id)   — network call if cache miss; used from async endpoints
  sync  read_cache(id)      — cache-only (no network); safe from thread-pool context
"""
from __future__ import annotations

import time
from typing import Optional

import httpx
from loguru import logger

try:
    from config import (
        FORMER_REC_ENGINE_CACHE_SIZE,
        FORMER_REC_ENGINE_CACHE_TTL,
        FORMER_REC_ENGINE_TIMEOUT,
        FORMER_REC_ENGINE_URL,
    )
except ImportError:
    FORMER_REC_ENGINE_URL = "http://10.21.60.94:8018"
    FORMER_REC_ENGINE_TIMEOUT = 0.5
    FORMER_REC_ENGINE_CACHE_TTL = 600   # 10 min — former engine data changes slowly
    FORMER_REC_ENGINE_CACHE_SIZE = 50_000

# account_id → (monotonic_ts, [product_id, ...])
_cache: dict[str, tuple[float, list[str]]] = {}


def _parse_pids(data) -> list[str]:
    """Normalise the former engine response to a flat product-ID list."""
    if isinstance(data, list):
        return [str(p) for p in data if p and isinstance(p, (str, int))]
    if isinstance(data, dict):
        raw: list = (
            data.get("recommendations")
            or data.get("products")
            or data.get("productIds")
            or []
        )
        return [
            str(p["productId"] if isinstance(p, dict) else p)
            for p in raw
            if p
        ]
    return []


def _evict_if_full() -> None:
    if len(_cache) >= FORMER_REC_ENGINE_CACHE_SIZE:
        oldest = min(_cache, key=lambda k: _cache[k][0])
        del _cache[oldest]


# ── Public API ────────────────────────────────────────────────────────────────


def read_cache(account_id: str, top_n: int = 30) -> list[str]:
    """Synchronous cache-only read — zero latency, safe from thread-pool context."""
    entry = _cache.get(account_id)
    if entry and time.monotonic() - entry[0] < FORMER_REC_ENGINE_CACHE_TTL:
        return entry[1][:top_n]
    return []


async def fetch(account_id: str, top_n: int = 30) -> list[str]:
    """Async fetch; serves from cache if still fresh, otherwise hits the network."""
    now = time.monotonic()
    entry = _cache.get(account_id)
    if entry and now - entry[0] < FORMER_REC_ENGINE_CACHE_TTL:
        return entry[1][:top_n]
    try:
        async with httpx.AsyncClient(timeout=FORMER_REC_ENGINE_TIMEOUT) as client:
            resp = await client.get(
                f"{FORMER_REC_ENGINE_URL}/api/recommendations/{account_id}"
            )
            resp.raise_for_status()
            pids = _parse_pids(resp.json())
        _evict_if_full()
        _cache[account_id] = (now, pids)
        logger.debug(f"Cold-start [{account_id}]: {len(pids)} products from former engine")
        return pids[:top_n]
    except Exception as exc:
        logger.debug(f"Cold-start engine unavailable [{account_id}]: {exc}")
        return []


def invalidate(account_id: str) -> None:
    """Evict one user from the cache (call after N new high-intent events)."""
    _cache.pop(account_id, None)


def is_cold_start(account_id: str) -> bool:
    """True when the core engine has no product history for this user."""
    from src.module.feature_store import store
    return not store.get_user_top_products(account_id, top_n=1)


def stats() -> dict:
    """Diagnostic snapshot of the in-process cache."""
    now = time.monotonic()
    live = sum(1 for ts, _ in _cache.values() if now - ts < FORMER_REC_ENGINE_CACHE_TTL)
    return {
        "total_entries": len(_cache),
        "live_entries": live,
        "stale_entries": len(_cache) - live,
        "max_size": FORMER_REC_ENGINE_CACHE_SIZE,
        "ttl_seconds": FORMER_REC_ENGINE_CACHE_TTL,
        "upstream": FORMER_REC_ENGINE_URL,
        "timeout_seconds": FORMER_REC_ENGINE_TIMEOUT,
    }
