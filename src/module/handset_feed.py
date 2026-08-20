"""
Marketplace Catalog API adapter — the handset-shop device feed.

    GET http://10.21.60.94:9000/marketplace/{account_id}

Thin compatibility layer over :mod:`src.module.upstream`, which owns the HTTP
client, cache and response parsing shared with the TOKI Shop feed.

Historical note: this module used to expect a response keyed by column names
(``{"HANDSET_PROD_ID": [...]}``).  The live service returns
``{"userId": ..., "taxonRecommendations": {"<slug>": [...]}}``, so the old
parser matched nothing and the feed silently resolved to ``{}`` on every call.
"""

from __future__ import annotations

from src.module.upstream import CATALOG_FEED, select

try:
    from config import ACCESSORY_TAXON_SLUGS, DEVICE_TAXON_SLUGS
except ImportError:  # pragma: no cover
    DEVICE_TAXON_SLUGS = (
        "handset-cellphone",
        "tablet",
        "watch-and-smart-watches",
        "headphones-earphones",
        "handset-accessory",
        "cpe",
    )
    ACCESSORY_TAXON_SLUGS = (
        "handset-accessory",
        "headphones-earphones",
        "watch-and-smart-watches",
    )


async def fetch_handset_feed(account_id: str) -> dict[str, list[str]]:
    """``{taxon_slug: [product_id, ...]}`` for the six device categories."""
    return await CATALOG_FEED.fetch(account_id)


def filter_feed_by_taxon(
    feed: dict[str, list[str]],
    taxon_slug: str,
    exclude_ids: set[str],
    top_n: int,
) -> list[str]:
    """Up to ``top_n`` catalog-validated product IDs for one taxon slug."""
    return select(feed, taxon_slug, exclude_ids, top_n)


def get_all_taxon_slugs() -> list[str]:
    """All taxon slugs covered by the Marketplace Catalog API."""
    return list(DEVICE_TAXON_SLUGS)


def stats() -> dict:
    return CATALOG_FEED.stats()
