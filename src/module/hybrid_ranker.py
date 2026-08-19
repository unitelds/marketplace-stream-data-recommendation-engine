"""
Hybrid ranker: merge content-based and popularity candidates, then apply
layered context re-rankers to produce a final top-N recommendation list.

Re-ranking pipeline (applied in order):
  1. Merge CBF + popularity via Reciprocal Rank Fusion (RRF)
  2. Remove OOS products and already-purchased products
  3. Session taxon boost — promote products matching user's browse path
  4. Basket-aware re-rank — boost cross-category complements, penalize duplicates
  5. Limit-check price filter — demote products above the user's checked limit
  6. Intent-level adjustment — high-intent users get conversion-optimised list
  7. Device-type top-N truncation
"""

from __future__ import annotations

import hashlib
from typing import Optional

from loguru import logger

import src.module.cold_start as cold_start
from src.module.content_based import get_similar_products, get_taxon_products
from src.module.feature_store import store
from src.module.intent_scorer import PRICE_RANGE_ORDINAL

# ─── RRF parameters ───────────────────────────────────────────────────────────
RRF_K = 60
WEIGHT_CBF = 0.6
WEIGHT_POPULAR = 0.4

# ─── Device → max results ─────────────────────────────────────────────────────
DEVICE_TOP_N = {
    "miniprogram": 8,
    "mobile": 12,
    "desktop": 20,
    "unknown": 15,
}

# ─── Intent thresholds ────────────────────────────────────────────────────────
HIGH_INTENT_THRESHOLD = 8.0
LOW_INTENT_THRESHOLD = 2.0


def _rrf_merge(
    list_a: list[tuple[str, float]],
    list_b: list[tuple[str, float]],
    weight_a: float = WEIGHT_CBF,
    weight_b: float = WEIGHT_POPULAR,
) -> list[tuple[str, float]]:
    """Reciprocal Rank Fusion of two ranked candidate lists."""
    scores: dict[str, float] = {}
    for rank, (pid, _) in enumerate(list_a):
        scores[pid] = scores.get(pid, 0.0) + weight_a / (RRF_K + rank + 1)
    for rank, (pid, _) in enumerate(list_b):
        scores[pid] = scores.get(pid, 0.0) + weight_b / (RRF_K + rank + 1)
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)


def _session_taxon_boost(
    candidates: list[tuple[str, float]],
    taxon_path: list[str],
    boost_factor: float = 0.25,
) -> list[tuple[str, float]]:
    """Boost products whose taxon appears in the session browse path (last 3)."""
    if not taxon_path:
        return candidates
    recent = set(taxon_path[-3:])
    result = []
    for pid, score in candidates:
        feat = store.product_features.get(pid, {})
        if feat.get("taxon_id") in recent:
            score = score * (1.0 + boost_factor)
        result.append((pid, score))
    return sorted(result, key=lambda x: x[1], reverse=True)


def _basket_aware_rerank(
    candidates: list[tuple[str, float]],
    basket_product_ids: list[str],
    penalize_same_category: bool = True,
) -> list[tuple[str, float]]:
    """
    Basket-aware re-ranking:
      - Boost products from different categories (cross-sell)
      - Penalize products in the same exact product_category as basket items
    """
    if not basket_product_ids:
        return candidates

    basket_categories = set()
    basket_taxons = set()
    for bpid in basket_product_ids:
        feat = store.product_features.get(bpid, {})
        cat = feat.get("product_category", "")
        taxon = feat.get("taxon_id", "")
        if cat:
            basket_categories.add(cat)
        if taxon:
            basket_taxons.add(taxon)

    result = []
    for pid, score in candidates:
        feat = store.product_features.get(pid, {})
        cat = feat.get("product_category", "")
        taxon = feat.get("taxon_id", "")

        if penalize_same_category and cat and cat in basket_categories:
            score = score * 0.6  # penalize duplicate category in basket
        elif taxon and taxon not in basket_taxons:
            score = score * 1.15  # cross-sell boost for different taxon
        result.append((pid, score))

    return sorted(result, key=lambda x: x[1], reverse=True)


def _limit_check_filter(
    candidates: list[tuple[str, float]],
    user_price_tier_max: Optional[int],
    demote_factor: float = 0.5,
) -> list[tuple[str, float]]:
    """
    When a user checked their lease limit, demote products priced above their tier.
    user_price_tier_max: max PRICE_RANGE_ORDINAL acceptable (0=budget, 1=mid, 2=high-end, 3=luxury)
    """
    if user_price_tier_max is None:
        return candidates
    result = []
    for pid, score in candidates:
        feat = store.product_features.get(pid, {})
        tier = feat.get("price_range_ordinal", 1)
        if tier > user_price_tier_max:
            score = score * demote_factor
        result.append((pid, score))
    return sorted(result, key=lambda x: x[1], reverse=True)


def _diversity_cap(
    candidates: list[tuple[str, float]],
    max_per_shop: int = 5,
) -> list[tuple[str, float]]:
    """Limit concentration from any single shop to avoid monopolising results."""
    shop_counts: dict[str, int] = {}
    result = []
    for pid, score in candidates:
        feat = store.product_features.get(pid, {})
        shop = feat.get("shop_name", "")
        count = shop_counts.get(shop, 0)
        if shop and count >= max_per_shop:
            continue
        shop_counts[shop] = count + 1
        result.append((pid, score))
    return result


def recommend(
    account_id: str,
    *,
    context_taxon_id: Optional[str] = None,
    top_n: Optional[int] = None,
    exclude_product_ids: Optional[list[str]] = None,
) -> dict:
    """
    Main recommendation entry point for a given account_id.

    Steps:
      1. Determine user's seed products (from interaction history)
      2. Run CBF retrieval from seeds
      3. Run popularity retrieval (taxon-scoped or global)
      4. Merge via RRF
      5. Apply context re-rankers (session, basket, limit, diversity)
      6. Truncate to device-appropriate top-N
      7. Return structured result dict
    """
    if not store.catalog_ready:
        logger.warning("Catalog not ready — returning empty recommendations")
        return _empty_result(account_id, "catalog_not_ready", context_taxon_id)

    session = store.get_session(account_id)
    device_type = session.get("device_type", "unknown")
    taxon_path: list[str] = session.get("taxon_path", [])
    basket: list[str] = session.get("basket", [])
    intent_score: float = session.get("intent_score", 0.0)
    limit_checked: bool = session.get("limit_checked", False)
    effective_top_n = top_n or DEVICE_TOP_N.get(device_type, 15)

    exclude = set(exclude_product_ids or [])

    # ── Step 1: seed products ─────────────────────────────────────────────────
    seed_products = store.get_user_top_products(account_id, top_n=10)
    strategy = "popular"

    # Cold start: use former rec engine seeds from cache (populated by placement endpoints)
    if not seed_products:
        seed_products = cold_start.read_cache(account_id, top_n=10)
        if seed_products:
            strategy = "cold_start_seed"

    # ── Step 2: CBF candidates ────────────────────────────────────────────────
    cbf_candidates: list[tuple[str, float]] = []
    if seed_products:
        cbf_candidates = get_similar_products(
            seed_products, top_k=60, exclude_ids=exclude
        )
        strategy = "cbf"

    # If no CBF but we have a taxon context, use taxon-scoped content search
    if not cbf_candidates and context_taxon_id:
        cbf_candidates = get_taxon_products(
            context_taxon_id, top_k=60, exclude_ids=exclude
        )
        strategy = "taxon_cbf"

    # If we have both seed and session taxon, also pull from session taxon
    if cbf_candidates and taxon_path:
        active_taxon = taxon_path[-1]
        if active_taxon != context_taxon_id:
            session_taxon_candidates = get_taxon_products(
                active_taxon, top_k=30, exclude_ids=exclude
            )
            cbf_candidates = _rrf_merge(
                cbf_candidates, session_taxon_candidates, 0.7, 0.3
            )

    # ── Step 3: popularity candidates (fallback + enrichment) ─────────────────
    taxon_for_popular = context_taxon_id or (taxon_path[-1] if taxon_path else None)
    pop_products = store.get_popular_products(taxon_id=taxon_for_popular, top_n=60)
    pop_candidates = [
        (pid, score)
        for pid, score in [
            (pid, store._popularity.get(pid, 0.01)) for pid in pop_products
        ]
        if pid not in exclude
    ]

    if not cbf_candidates and not pop_candidates:
        # Global popularity fallback
        global_pop = store.get_popular_products(top_n=effective_top_n * 2)
        pop_candidates = [
            (pid, store._popularity.get(pid, 0.01))
            for pid in global_pop
            if pid not in exclude
        ]
        strategy = "popular"

    # ── Step 4: Merge ─────────────────────────────────────────────────────────
    if cbf_candidates and pop_candidates:
        merged = _rrf_merge(cbf_candidates, pop_candidates, WEIGHT_CBF, WEIGHT_POPULAR)
        if seed_products:
            strategy = "hybrid"
    elif cbf_candidates:
        merged = cbf_candidates
    else:
        merged = pop_candidates

    # ── Step 5: Context re-rankers ────────────────────────────────────────────
    # 5a. Session taxon boost
    merged = _session_taxon_boost(merged, taxon_path)

    # 5b. Basket-aware
    all_basket = list(set(basket + seed_products[:3]))
    if all_basket:
        merged = _basket_aware_rerank(merged, all_basket)

    # 5c. Limit-check price filter
    if limit_checked:
        # User checked limit → prefer budget/mid tier
        merged = _limit_check_filter(merged, user_price_tier_max=1)

    # 5d. Intent-level adjustment
    if intent_score >= HIGH_INTENT_THRESHOLD:
        # High intent: keep order as-is (conversion-optimised by scores)
        pass
    elif intent_score <= LOW_INTENT_THRESHOLD:
        # Low intent: inject more taxon diversity at the end
        if context_taxon_id:
            extra = get_taxon_products(context_taxon_id, top_k=10, exclude_ids=exclude)
            extra_pids = {pid for pid, _ in merged[:effective_top_n]}
            for pid, s in extra:
                if pid not in extra_pids:
                    merged.append((pid, s * 0.5))

    # 5e. Shop diversity cap
    merged = _diversity_cap(merged)

    # ── Step 6: Final top-N ───────────────────────────────────────────────────
    final_ids = [pid for pid, _ in merged[: effective_top_n * 2]]
    final_ids = final_ids[:effective_top_n]

    # Resolve taxon_id for response (context > session last > user preferred)
    response_taxon = (
        context_taxon_id
        or (taxon_path[-1] if taxon_path else None)
        or (store.get_user_interacted_taxons(account_id, top_n=1) or [None])[0]
    )

    return {
        "id": account_id,
        "taxon_id": response_taxon,
        "recommendations": final_ids,
        "strategy": strategy,
        "intent_score": round(intent_score, 2),
        "device": device_type,
        "count": len(final_ids),
    }


def _empty_result(account_id: str, reason: str, taxon_id: Optional[str] = None) -> dict:
    return {
        "id": account_id,
        "taxon_id": taxon_id,
        "recommendations": [],
        "strategy": reason,
        "intent_score": 0.0,
        "device": "unknown",
        "count": 0,
    }


def recommend_multi_taxon(
    account_id: str,
    *,
    top_taxons: int = 3,
    top_n_per_taxon: int = 10,
    extra_taxon_ids: Optional[list[str]] = None,
    exclude_product_ids: Optional[list[str]] = None,
) -> dict:
    """
    Generate per-taxon recommendations for a user.

    Taxon selection order:
      1. Current session taxon path (most recent, highest recency signal)
      2. User's interaction-weighted historical taxons
      3. Any extra taxon_ids passed by caller (e.g. from context)

    Cross-taxon deduplication: once a product appears in an earlier taxon's list
    it is excluded from later ones, ensuring unique products across all feeds.

    Returns:
      {
        "id": account_id,
        "taxon_feeds": [
          {
            "taxon_id": "...",
            "taxon_name": "...",
            "recommendations": ["pid", ...],
            "score": 8.5
          }, ...
        ],
        "total_products": N,
        "strategy": "multi_taxon_hybrid",
        "intent_score": 8.5,
        "device": "mobile"
      }
    """
    if not store.catalog_ready:
        return {
            "id": account_id,
            "taxon_feeds": [],
            "total_products": 0,
            "strategy": "catalog_not_ready",
            "intent_score": 0.0,
            "device": "unknown",
        }

    session = store.get_session(account_id)
    device_type = session.get("device_type", "unknown")
    session_taxons: list[str] = session.get("taxon_path", [])
    intent_score: float = session.get("intent_score", 0.0)

    # Build ordered candidate taxon list (deduplicated)
    seen_taxons: set[str] = set()
    ordered_taxons: list[str] = []

    def _add(tid: str) -> None:
        import re as _re

        if (
            tid
            and tid not in seen_taxons
            and tid in store.taxon_id_to_products
            and _re.match(r"^[0-9a-fA-F]{24}$", tid)
        ):
            seen_taxons.add(tid)
            ordered_taxons.append(tid)

    # 1. Session path (most recent first — reversed)
    for t in reversed(session_taxons[-5:]):
        _add(t)

    # 2. Historical preference (interaction-weighted)
    for t in store.get_user_interacted_taxons(account_id, top_n=top_taxons + 3):
        _add(t)

    # 3. Caller-supplied extras
    for t in extra_taxon_ids or []:
        _add(t)

    # Trim to requested limit
    selected_taxons = ordered_taxons[:top_taxons]

    # Cold-start: user has no history — use hash-based taxon rotation so every
    # user gets a different starting position in the catalog taxon list.
    # This guarantees diverse results across users even with zero interaction data.
    if not selected_taxons:
        import re as _re

        _OID = _re.compile(r"^[0-9a-fA-F]{24}$")
        all_taxon_ids = [
            t
            for t in store.taxon_id_to_products
            if store.taxon_id_to_products[t] and _OID.match(t)
        ]
        if all_taxon_ids:
            # Deterministic but user-unique: MD5 of account_id → offset into taxon list
            h = int(hashlib.md5(account_id.encode()).hexdigest()[:8], 16)
            offset = h % len(all_taxon_ids)
            rotated = all_taxon_ids[offset:] + all_taxon_ids[:offset]
            selected_taxons = rotated[:top_taxons]

    # Generate per-taxon recommendations with cross-taxon deduplication
    used_products: set[str] = set(exclude_product_ids or [])
    taxon_feeds: list[dict] = []

    for taxon_id in selected_taxons:
        result = recommend(
            account_id,
            context_taxon_id=taxon_id,
            top_n=top_n_per_taxon,
            exclude_product_ids=list(used_products),
        )
        recs = result.get("recommendations", [])
        if not recs:
            continue
        used_products.update(recs)
        taxon_feeds.append(
            {
                "taxon_id": taxon_id,
                "taxon_name": store.taxon_id_to_name.get(taxon_id, ""),
                "recommendations": recs,
                "count": len(recs),
                "score": result.get("intent_score", 0.0),
            }
        )

    return {
        "id": account_id,
        "taxon_feeds": taxon_feeds,
        "total_products": len(used_products - set(exclude_product_ids or [])),
        "strategy": "multi_taxon_hybrid" if taxon_feeds else "popular",
        "intent_score": round(intent_score, 2),
        "device": device_type,
    }
