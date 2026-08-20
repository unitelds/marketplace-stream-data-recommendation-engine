"""
Outbound delivery — push generated recommendations to the TOKI marketplace.

    POST {MARKETPLACE_PUSH_URL}
    Authorization: Bearer {MARKETPLACE_PUSH_TOKEN}
    {
      "accountId": "<24-hex ObjectId>",
      "products": [ {"productId": "...", "taxonId": "..."}, ... ]
    }

  staging → https://staging-marketplace.toki.mn/ms/catalogue/v1/recommendation
  prod    → https://marketplace.toki.mn/ms/catalogue/v1/recommendation

Why this module exists
----------------------
The push target used to share a config variable with the *inbound* catalog feed
(``MARKETPLACE_API_BASE_URL``), which pointed production pushes at
``http://10.21.60.94:9000/marketplace``.  That service is read-only — its
OpenAPI schema exposes only ``GET /health``, ``GET /ready`` and
``GET /marketplace/{user_id}`` — so every production push returned 404.
Reads and writes are now separate settings that cannot drift into each other.

Both marketplace hosts reject unauthenticated calls (401 "Authentication
required"), so ``MARKETPLACE_PUSH_TOKEN`` must be set for delivery to succeed.
"""

from __future__ import annotations

import asyncio
from typing import Iterable, Optional, Sequence

import httpx
from loguru import logger

try:
    from config import (
        MARKETPLACE_PUSH_PAYLOAD_FORMAT,
        MARKETPLACE_PUSH_RETRIES,
        MARKETPLACE_PUSH_TIMEOUT,
        MARKETPLACE_PUSH_TOKEN,
        MARKETPLACE_PUSH_URL,
        UPSTREAM_MAX_CONNECTIONS,
    )
except ImportError:  # pragma: no cover
    MARKETPLACE_PUSH_URL = (
        "https://staging-marketplace.toki.mn/ms/catalogue/v1/recommendation"
    )
    MARKETPLACE_PUSH_TOKEN = ""
    MARKETPLACE_PUSH_TIMEOUT = 5.0
    MARKETPLACE_PUSH_RETRIES = 2
    MARKETPLACE_PUSH_PAYLOAD_FORMAT = "products"
    UPSTREAM_MAX_CONNECTIONS = 100


_clients: dict[int, httpx.AsyncClient] = {}


def auth_headers() -> dict[str, str]:
    """Bearer header, or an empty dict when no token is configured."""
    if not MARKETPLACE_PUSH_TOKEN:
        return {}
    return {"Authorization": f"Bearer {MARKETPLACE_PUSH_TOKEN}"}


def _http() -> httpx.AsyncClient:
    """Pooled AsyncClient bound to the running event loop."""
    key = id(asyncio.get_running_loop())
    client = _clients.get(key)
    if client is None or client.is_closed:
        client = httpx.AsyncClient(
            timeout=MARKETPLACE_PUSH_TIMEOUT,
            limits=httpx.Limits(
                max_connections=UPSTREAM_MAX_CONNECTIONS,
                max_keepalive_connections=UPSTREAM_MAX_CONNECTIONS // 2,
                keepalive_expiry=30.0,
            ),
            headers={"Content-Type": "application/json", **auth_headers()},
        )
        _clients[key] = client
    return client


async def aclose() -> None:
    """Close pooled clients — called from the FastAPI lifespan shutdown hook."""
    for client in list(_clients.values()):
        if not client.is_closed:
            await client.aclose()
    _clients.clear()


def _collect(taxon_feeds: Iterable[object]) -> list[tuple[str, str]]:
    """Flatten taxon feeds into de-duplicated ``(product_id, taxon_id)`` pairs."""
    pairs: list[tuple[str, str]] = []
    seen: set[str] = set()
    for feed in taxon_feeds:
        if isinstance(feed, dict):
            taxon_id = feed.get("taxon_id")
            recs = feed.get("recommendations") or []
        else:
            taxon_id = getattr(feed, "taxon_id", None)
            recs = getattr(feed, "recommendations", None) or []
        if not taxon_id:
            continue
        for pid in recs:
            if pid and pid not in seen:
                seen.add(pid)
                pairs.append((pid, taxon_id))
    return pairs


def build_payload(
    account_id: str,
    taxon_feeds: Iterable[object],
    fmt: Optional[str] = None,
) -> dict:
    """Build the request body in the configured marketplace payload format."""
    pairs = _collect(taxon_feeds)
    fmt = fmt or MARKETPLACE_PUSH_PAYLOAD_FORMAT

    if fmt == "product_ids":
        return {"accountId": account_id, "productId": [pid for pid, _ in pairs]}

    if fmt == "taxon_map":
        taxon_map: dict[str, list[str]] = {}
        for pid, taxon in pairs:
            taxon_map.setdefault(taxon, []).append(pid)
        return {
            "accountId": account_id,
            "productId": [pid for pid, _ in pairs],
            "taxonRecommendations": taxon_map,
        }

    return {
        "accountId": account_id,
        "products": [{"productId": pid, "taxonId": taxon} for pid, taxon in pairs],
    }


def payload_size(payload: dict) -> int:
    """Number of products carried by a payload, whatever its format."""
    if "products" in payload:
        return len(payload["products"])
    return len(payload.get("productId") or [])


async def push(
    account_id: str,
    taxon_feeds: Iterable[object],
    *,
    url: Optional[str] = None,
    timeout: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> tuple[str, Optional[str], int]:
    """
    Deliver one account's feed.

    Returns ``(status, error, product_count)`` where status is one of
    ``ok`` | ``failed`` | ``skipped``.  Retries transient failures (timeouts and
    5xx) up to ``MARKETPLACE_PUSH_RETRIES`` times with exponential backoff; 4xx
    responses are not retried because they will not succeed on a repeat.
    """
    payload = build_payload(account_id, taxon_feeds)
    count = payload_size(payload)
    if not count:
        return "skipped", "no products in feed", 0

    target = url or MARKETPLACE_PUSH_URL
    if not target:
        return "skipped", "no push URL configured", count

    http = client or _http()
    kwargs: dict = {"json": payload, "headers": auth_headers()}
    if timeout is not None:
        kwargs["timeout"] = timeout

    last_error = "unknown error"
    for attempt in range(MARKETPLACE_PUSH_RETRIES + 1):
        try:
            resp = await http.post(target, **kwargs)
            if resp.status_code < 400:
                return "ok", None, count
            body = resp.text[:160]
            last_error = f"HTTP {resp.status_code}: {body}"
            if resp.status_code in (401, 403):
                last_error += " — check MARKETPLACE_PUSH_TOKEN"
            if resp.status_code < 500:
                break  # client error: retrying cannot help
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"[:200]
        if attempt < MARKETPLACE_PUSH_RETRIES:
            await asyncio.sleep(0.25 * (2**attempt))

    logger.warning(f"Push failed [{account_id}] → {target}: {last_error}")
    return "failed", last_error, count


async def push_many(
    items: Sequence[tuple[str, Iterable[object]]],
    *,
    url: Optional[str] = None,
    concurrency: int = 50,
) -> list[tuple[str, str, Optional[str], int]]:
    """
    Push many accounts concurrently over the shared connection pool.

    ``items`` is a sequence of ``(account_id, taxon_feeds)``.
    Returns ``(account_id, status, error, product_count)`` per item.
    """
    sem = asyncio.Semaphore(concurrency)
    http = _http()

    async def _one(account_id: str, feeds: Iterable[object]):
        async with sem:
            status, error, count = await push(
                account_id, feeds, url=url, client=http
            )
            return account_id, status, error, count

    return list(await asyncio.gather(*(_one(a, f) for a, f in items)))


def target_url() -> str:
    """The configured push endpoint (for logging and health reporting)."""
    return MARKETPLACE_PUSH_URL


def is_configured() -> bool:
    """True when both a URL and a bearer token are present."""
    return bool(MARKETPLACE_PUSH_URL and MARKETPLACE_PUSH_TOKEN)
