"""
Catalog synchronization: pull master_catalog_profile from PostgreSQL,
normalize all fields, and build TF-IDF content vectors for content-based filtering.

Normalization pipeline:
  1. Price "4100000 MNT" → float
  2. Stock string → int
  3. specifications / connectivity / discount JSON strings → parsed dict → flattened text
  4. keywords JSON list string → joined text
  5. details: may be plain text OR JSON blob → extract text only
  6. Build taxon label/slug → taxon_id maps (resolves Mongolian taxon_click labels)
  7. Fit TF-IDF vectorizer on combined text corpus
  8. Populate FeatureStore with all artifacts
"""

from __future__ import annotations

import asyncio
import re
import time
from collections import defaultdict
from typing import Optional

import numpy as np
import pandas as pd
from loguru import logger
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import normalize

from src.module.event_processor import (
    flatten_specs,
    normalize_details,
    parse_keywords,
    parse_price,
    safe_parse,
)
from src.module.feature_store import store
from src.module.intent_scorer import PREMIUM_GRADE_ORDINAL, PRICE_RANGE_ORDINAL

# ─── Config imports ────────────────────────────────────────────────────────────
try:
    from config import CATALOG_QUERY, CATALOG_TABLE, DATABASE_URL
except ImportError:
    DATABASE_URL = None
    CATALOG_QUERY = "SELECT * FROM marketplace_catalog_data_extended_version3"
    CATALOG_TABLE = "marketplace_catalog_data_extended_version3"

_SYNC_LOCK = asyncio.Lock()


# ─── Row-level normalization ──────────────────────────────────────────────────


def _parse_stock(v: object) -> int:
    """Parse stock value to int, returning 0 on failure."""
    try:
        return max(0, int(float(str(v))))
    except (ValueError, TypeError):
        return 0


def _safe_str(v: object, default: str = "") -> str:
    if v is None or (isinstance(v, float) and v != v):
        return default
    return str(v).strip()


def _build_content_text(row: dict) -> str:
    """
    Combine all text-bearing fields into a single TF-IDF document.

    Field priority (repeated for boosting):
      manufacturer × 3, product names × 2, category, keywords, specs, details
    """
    parts: list[str] = []

    # High-signal fields — repeat to boost weight
    mfr = _safe_str(row.get("manufacturer"))
    if mfr:
        parts.extend([mfr] * 3)

    for field in ("actual_product", "generic_name"):
        val = _safe_str(row.get(field))
        if val:
            parts.extend([val] * 2)

    # Category hierarchy
    for field in (
        "main_category",
        "sub_category",
        "product_category",
        "exact_product_category",
        "best_used_for",
        "taxon_name",
    ):
        val = _safe_str(row.get(field))
        if val:
            parts.append(val)

    # Keywords
    kws = parse_keywords(row.get("keywords"))
    if kws:
        parts.append(" ".join(kws))

    # Specifications
    specs = flatten_specs(row.get("specifications"))
    if specs:
        parts.append(specs)

    # Details (plain text extraction; skip JSON-embedded image blobs)
    details = normalize_details(row.get("details"))
    if details:
        parts.append(details)

    # Short description (may contain Mongolian — TF-IDF handles unicode)
    desc = _safe_str(row.get("description"))
    if desc and len(desc) < 500:
        parts.append(desc)

    # Price range, premium grade as tokens
    for field in ("price_range", "premium_grade", "carried_located_in"):
        val = _safe_str(row.get(field))
        if val and val.lower() not in ("nan", "none", ""):
            parts.append(val)

    return " ".join(parts)


def normalize_catalog_row(row: pd.Series) -> dict:
    """
    Normalize a single catalog row into a typed feature dict.

    Returns a dict ready for storage in feature_store.product_features.
    """
    d = row.to_dict() if hasattr(row, "to_dict") else dict(row)

    price_range = _safe_str(d.get("price_range"), "unknown").lower()
    premium_grade = _safe_str(d.get("premium_grade"), "standard").lower()

    return {
        "product_id": _safe_str(d.get("product_id")),
        "taxon_id": _safe_str(d.get("taxon_id")),
        "taxon_name": _safe_str(d.get("taxon_name")),
        "manufacturer": _safe_str(d.get("manufacturer")),
        "generic_name": _safe_str(d.get("generic_name")),
        "actual_product": _safe_str(d.get("actual_product")),
        "main_category": _safe_str(d.get("main_category")),
        "sub_category": _safe_str(d.get("sub_category")),
        "product_category": _safe_str(d.get("product_category")),
        "shop_name": _safe_str(d.get("shop_name")),
        "price": parse_price(d.get("price")),
        "price_range": price_range,
        "price_range_ordinal": PRICE_RANGE_ORDINAL.get(price_range, 1),
        "premium_grade": premium_grade,
        "premium_grade_ordinal": PREMIUM_GRADE_ORDINAL.get(premium_grade, 0),
        "stock": _parse_stock(d.get("stock", 0)),
        "best_used_for": _safe_str(d.get("best_used_for")),
        "keywords": parse_keywords(d.get("keywords")),
        "url_link": _safe_str(d.get("url_link")),
        "sku": _safe_str(d.get("sku")),
        "group_id": _safe_str(d.get("group_id")),
        "content_text": _build_content_text(d),
    }


# ─── Taxon map builder ────────────────────────────────────────────────────────


def build_taxon_maps(catalog_df: pd.DataFrame) -> tuple[dict, dict, dict]:
    """
    Build three taxon resolution maps from the catalog:

    1. label_map: taxon_name slug → taxon_id
    2. name_to_id: same as above (alias)
    3. id_to_products: taxon_id → [product_id, ...]
    """
    label_map: dict[str, str] = {}
    name_to_id: dict[str, str] = {}
    id_to_products: dict[str, list] = defaultdict(list)

    for _, row in catalog_df.iterrows():
        tid = _safe_str(row.get("taxon_id"))
        tname = _safe_str(row.get("taxon_name"))
        pid = _safe_str(row.get("product_id"))

        if not tid or not pid:
            continue

        # taxon_name slug → taxon_id
        if tname:
            label_map[tname] = tid
            name_to_id[tname] = tid
            # Also index individual words from the slug for partial matching
            for word in re.split(r"[-_\s]+", tname):
                if len(word) > 3:
                    label_map.setdefault(word.lower(), tid)

        id_to_products[tid].append(pid)

    return label_map, name_to_id, id_to_products


# ─── TF-IDF vectorizer ────────────────────────────────────────────────────────


def build_tfidf_index(content_texts: list[str]):
    """
    Fit and transform TF-IDF on catalog content texts.

    Returns (vectorizer, normalized_sparse_matrix).
    """
    vectorizer = TfidfVectorizer(
        analyzer="word",
        token_pattern=r"(?u)\b\w\w+\b",  # min 2 chars, handles unicode/Mongolian
        ngram_range=(1, 2),
        max_features=30_000,
        sublinear_tf=True,
        min_df=1,
        max_df=0.95,
    )
    matrix = vectorizer.fit_transform(content_texts)
    # L2 normalize rows so cosine similarity = dot product
    matrix = normalize(matrix, norm="l2", copy=False)
    return vectorizer, matrix


# ─── Main sync entry point ────────────────────────────────────────────────────


async def sync_catalog(force: bool = False) -> bool:
    """
    Pull catalog from PostgreSQL, normalize, build TF-IDF index, update FeatureStore.

    Returns True on success, False on error.
    Uses an async lock so concurrent startup calls don't double-build.
    """
    async with _SYNC_LOCK:
        if store.catalog_ready and not force:
            age = time.time() - (store.catalog_synced_at or 0)
            if age < 600:  # skip if synced in last 10 min
                logger.debug("Catalog sync skipped — recent sync exists")
                return True

        logger.info("Starting catalog sync from PostgreSQL…")
        try:
            catalog_df = await asyncio.get_event_loop().run_in_executor(
                None, _fetch_catalog
            )
        except Exception as exc:
            logger.error(f"Catalog fetch failed: {exc}")
            return False

        if catalog_df is None or catalog_df.empty:
            logger.warning("Catalog query returned empty result")
            return False

        logger.info(f"Fetched {len(catalog_df)} catalog rows — normalizing…")

        # Lowercase column names for consistency
        catalog_df.columns = [c.lower() for c in catalog_df.columns]

        # Normalize each row and build feature dicts
        features: dict[str, dict] = {}
        for _, row in catalog_df.iterrows():
            feat = normalize_catalog_row(row)
            pid = feat.get("product_id")
            if pid:
                features[pid] = feat

        if not features:
            logger.warning("No valid product_ids found in catalog")
            return False

        # Build TF-IDF index
        product_ids = list(features.keys())
        content_texts = [features[pid]["content_text"] for pid in product_ids]
        logger.info(f"Building TF-IDF index for {len(product_ids)} products…")
        vectorizer, matrix = await asyncio.get_event_loop().run_in_executor(
            None, build_tfidf_index, content_texts
        )

        # Build taxon maps
        label_map, name_to_id, id_to_products = build_taxon_maps(catalog_df)

        # Commit all artifacts to the feature store atomically
        store.catalog_df = catalog_df
        store.product_features = features
        store.product_ids = product_ids
        store.product_id_to_idx = {pid: i for i, pid in enumerate(product_ids)}
        store.tfidf_vectorizer = vectorizer
        store.tfidf_matrix = matrix
        store.taxon_label_map = label_map
        store.taxon_name_to_id = name_to_id
        store.taxon_id_to_name = {v: k for k, v in name_to_id.items()}  # reverse map
        store.taxon_id_to_products = id_to_products
        store.catalog_size = len(product_ids)
        store.catalog_synced_at = time.time()
        store.catalog_ready = True

        logger.info(
            f"Catalog sync complete: {len(product_ids)} products, "
            f"{len(label_map)} taxon labels, TF-IDF shape {matrix.shape}"
        )
        return True


def _fetch_catalog() -> Optional[pd.DataFrame]:
    """Synchronous PostgreSQL fetch (runs in executor thread)."""
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(DATABASE_URL, connect_args={"connect_timeout": 10})
        with engine.connect() as conn:
            df = pd.read_sql(text(CATALOG_QUERY), conn)
        engine.dispose()
        return df
    except Exception as exc:
        logger.error(f"PostgreSQL catalog fetch error: {exc}")
        raise


async def run_scheduled_sync(interval_minutes: int = 10) -> None:
    """Background task: periodically re-sync the catalog."""
    interval_seconds = interval_minutes * 60
    while True:
        await asyncio.sleep(interval_seconds)
        logger.info("Scheduled catalog re-sync triggered")
        await sync_catalog(force=True)
