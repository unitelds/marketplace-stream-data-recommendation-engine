"""
API key authentication + per-key rate limiting middleware.

Authentication:
  Every request to /api/v1/* must carry one of:
    - Header:  X-API-Key: <key>
    - Query:   ?api_key=<key>

  /api/v1/health is exempt (liveness probes don't need keys).
  /docs, /redoc, /openapi.json are exempt (developer tooling).

Key tiers and rate limits (requests / second, per key):
  internal  — 500 req/s   shop-side backend services, batch pipelines
  standard  — 100 req/s   normal shop frontend clients
  readonly  — 20  req/s   analytics / monitoring callers

Keys are loaded at import time from the environment variable TOKI_API_KEYS
in the format:
  TOKI_API_KEYS="key1:internal,key2:standard,key3:readonly"

A hard-coded dev key "dev-local-unsafe" with tier=internal is always active
when TOKI_ENV != "production" so the dev container never needs env config.

Rate limiting:
  Implemented via slowapi (Starlette-native limiter backed by an in-process
  token-bucket per key). The global limit is applied before routing so it
  fires even for unregistered endpoints.

  Limit strings follow slowapi convention:  "500/second", "100/minute", etc.
"""

from __future__ import annotations

import os
import time
from collections import defaultdict
from threading import Lock

from fastapi import Request
from fastapi.responses import JSONResponse
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware

# ── Key registry ───────────────────────────────────────────────────────────────

TIER_LIMITS: dict[str, int] = {
    "internal": 500,  # req/s
    "standard": 100,
    "readonly": 20,
}

# key → tier
_KEY_REGISTRY: dict[str, str] = {}

# Exempt paths — exact or prefix match, no API key required
_EXEMPT_EXACT = {"/", "/favicon.ico"}
_EXEMPT_PREFIXES = (
    "/docs",
    "/redoc",
    "/openapi.json",
    "/api/v1/health",
    "/api/v1/metrics",
    "/dashboard",
)


def _load_keys() -> None:
    """Populate _KEY_REGISTRY from TOKI_API_KEYS env var and dev defaults."""
    raw = os.getenv("TOKI_API_KEYS", "")
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if ":" in entry:
            key, tier = entry.split(":", 1)
        else:
            key, tier = entry, "standard"
        key = key.strip()
        tier = tier.strip().lower()
        if key and tier in TIER_LIMITS:
            _KEY_REGISTRY[key] = tier
            logger.debug(f"API key registered: …{key[-6:]} ({tier})")
        else:
            logger.warning(f"Skipped invalid TOKI_API_KEYS entry: {entry!r}")

    env = os.getenv("TOKI_ENV", "production")
    if env != "production":
        _KEY_REGISTRY["dev-local-unsafe"] = "internal"
        logger.info("Dev API key active (non-production only)")


_load_keys()


def get_key_tier(key: str) -> str | None:
    return _KEY_REGISTRY.get(key)


def is_key_valid(key: str) -> bool:
    return key in _KEY_REGISTRY


# ── Token-bucket rate limiter (per-key, in-process) ───────────────────────────


class _TokenBucket:
    """Thread-safe token bucket: refills `rate` tokens/second up to `capacity`."""

    __slots__ = ("capacity", "rate", "_tokens", "_last", "_lock")

    def __init__(self, rate: int) -> None:
        self.capacity = float(rate)
        self.rate = float(rate)
        self._tokens = float(rate)
        self._last = time.monotonic()
        self._lock = Lock()

    def consume(self) -> bool:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._last = now
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False


_buckets: dict[str, _TokenBucket] = {}
_bucket_lock = Lock()


def _get_bucket(key: str) -> _TokenBucket:
    if key not in _buckets:
        with _bucket_lock:
            if key not in _buckets:
                tier = _KEY_REGISTRY.get(key, "standard")
                rate = TIER_LIMITS.get(tier, 100)
                _buckets[key] = _TokenBucket(rate)
    return _buckets[key]


# ── Request counter for observability ─────────────────────────────────────────

_request_counts: dict[str, int] = defaultdict(int)
_rejected_counts: dict[str, int] = defaultdict(int)


def auth_stats() -> dict:
    return {
        "registered_keys": len(_KEY_REGISTRY),
        "tiers": {
            tier: sum(1 for t in _KEY_REGISTRY.values() if t == tier)
            for tier in TIER_LIMITS
        },
        "active_buckets": len(_buckets),
        "request_counts": dict(_request_counts),
        "rejected_counts": dict(_rejected_counts),
    }


# ── Middleware ─────────────────────────────────────────────────────────────────


class APIKeyMiddleware(BaseHTTPMiddleware):
    """
    1. Extract API key from X-API-Key header or ?api_key= query parameter.
    2. Reject unknown keys with 401.
    3. Apply per-key token-bucket rate limit; reject excess with 429.
    4. Attach key and tier to request.state for downstream logging.
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Exempt liveness probes, docs, and root
        if path in _EXEMPT_EXACT or any(path.startswith(p) for p in _EXEMPT_PREFIXES):
            return await call_next(request)

        # Extract key
        api_key = request.headers.get("X-API-Key") or request.query_params.get(
            "api_key"
        )

        if not api_key:
            _rejected_counts["missing_key"] += 1
            return JSONResponse(
                status_code=401,
                content={
                    "error": "missing_api_key",
                    "detail": "Supply X-API-Key header or ?api_key= query parameter.",
                },
            )

        if not is_key_valid(api_key):
            _rejected_counts["invalid_key"] += 1
            logger.warning(f"Invalid API key attempt from {request.client.host}")
            return JSONResponse(
                status_code=401,
                content={
                    "error": "invalid_api_key",
                    "detail": "The provided API key is not recognised.",
                },
            )

        # Rate limit
        bucket = _get_bucket(api_key)
        if not bucket.consume():
            tier = _KEY_REGISTRY[api_key]
            limit = TIER_LIMITS[tier]
            _rejected_counts[f"rate_limit_{api_key[-6:]}"] += 1
            return JSONResponse(
                status_code=429,
                headers={"Retry-After": "1"},
                content={
                    "error": "rate_limit_exceeded",
                    "detail": f"Tier '{tier}' limit is {limit} req/s. Retry after 1 second.",
                    "limit": limit,
                    "tier": tier,
                },
            )

        # Attach to request state for route-level logging
        request.state.api_key_suffix = api_key[-6:]
        request.state.api_key_tier = _KEY_REGISTRY[api_key]
        _request_counts[api_key[-6:]] += 1

        return await call_next(request)
