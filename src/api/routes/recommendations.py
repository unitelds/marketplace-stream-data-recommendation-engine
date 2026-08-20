"""
Placement-specific recommendation endpoints.

Three UI areas served:
  1. POST /api/v1/recommendations/taxon   — taxon/category page product grid
  2. POST /api/v1/recommendations/product — product detail page "similar products" panel
  3. POST /api/v1/recommendations/basket  — basket/cart page cross-sell panel

Each placement uses a distinct blend of the three retrieval pipelines:
  • TF-IDF content-based filtering (CBF)  — src/module/content_based.py
  • Item-based collaborative filtering (CF) — src/module/collaborative.py
  • Popularity fallback                   — FeatureStore._popularity

Merge strategy: Reciprocal Rank Fusion (RRF) with placement-tuned weights.
All pipelines share the same context re-rankers from hybrid_ranker.py.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, status
from loguru import logger

import src.module.cold_start as cold_start
import src.module.upstream as upstream
from src.api.schemas.event import (
    BasketPageRequest,
    HandsetAccessoriesRequest,
    HandsetFeedRequest,
    HandsetFeedResponse,
    HandsetFeedTaxonItem,
    PlacementRecommendationResponse,
    ProductPageRequest,
    TaxonPageRequest,
)
from src.module.collaborative import get_cf_candidates, get_item_similar_products
from src.module.content_based import get_similar_products, get_taxon_products
from src.module.feature_store import store
from src.module.handset_feed import ACCESSORY_TAXON_SLUGS, get_all_taxon_slugs
from src.module.hybrid_ranker import (
    DEVICE_TOP_N,
    RRF_K,
    _basket_aware_rerank,
    _diversity_cap,
    _limit_check_filter,
    _session_taxon_boost,
)
from src.module.metrics import metrics
from src.module.upstream import CATALOG_FEED, flatten, select

router = APIRouter(prefix="/api/v1/recommendations", tags=["placements"])

# ── RRF weights per placement ──────────────────────────────────────────────────
# Taxon page: broad discovery — popularity matters more
_TAXON_W_CBF = 0.45
_TAXON_W_CF = 0.30
_TAXON_W_POP = 0.25

# PDP: similarity is primary; CF adds social proof
_PRODUCT_W_CBF = 0.60
_PRODUCT_W_CF = 0.40

# Basket: cross-sell is primary; popularity gives coverage
_BASKET_W_CBF = 0.40
_BASKET_W_CF = 0.35
_BASKET_W_POP = 0.25


# ── Shared RRF helper ──────────────────────────────────────────────────────────


def _rrf_merge_three(
    list_a: list[tuple[str, float]],
    list_b: list[tuple[str, float]],
    list_c: list[tuple[str, float]],
    wa: float,
    wb: float,
    wc: float,
) -> list[tuple[str, float]]:
    """Three-list Reciprocal Rank Fusion."""
    scores: dict[str, float] = {}
    for rank, (pid, _) in enumerate(list_a):
        scores[pid] = scores.get(pid, 0.0) + wa / (RRF_K + rank + 1)
    for rank, (pid, _) in enumerate(list_b):
        scores[pid] = scores.get(pid, 0.0) + wb / (RRF_K + rank + 1)
    for rank, (pid, _) in enumerate(list_c):
        scores[pid] = scores.get(pid, 0.0) + wc / (RRF_K + rank + 1)
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)


def _rrf_merge_two(
    list_a: list[tuple[str, float]],
    list_b: list[tuple[str, float]],
    wa: float,
    wb: float,
) -> list[tuple[str, float]]:
    """Two-list Reciprocal Rank Fusion."""
    scores: dict[str, float] = {}
    for rank, (pid, _) in enumerate(list_a):
        scores[pid] = scores.get(pid, 0.0) + wa / (RRF_K + rank + 1)
    for rank, (pid, _) in enumerate(list_b):
        scores[pid] = scores.get(pid, 0.0) + wb / (RRF_K + rank + 1)
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)


def _apply_shared_rerankers(
    candidates: list[tuple[str, float]],
    session: dict,
    basket_ids: list[str],
) -> list[tuple[str, float]]:
    """Apply session-taxon boost, basket-aware rerank, limit-check, and diversity cap."""
    candidates = _session_taxon_boost(candidates, session.get("taxon_path", []))
    if basket_ids:
        candidates = _basket_aware_rerank(candidates, basket_ids)
    if session.get("limit_checked"):
        candidates = _limit_check_filter(candidates, user_price_tier_max=1)
    return _diversity_cap(candidates)


# ── 1. Taxon page ─────────────────────────────────────────────────────────────


@router.post(
    "/taxon",
    response_model=PlacementRecommendationResponse,
    status_code=status.HTTP_200_OK,
    summary="Taxon/category page product grid",
    description=(
        "Returns a personalized product list for a category page. "
        "Blends TF-IDF CBF (from user history), item-based CF (co-interaction), "
        "and taxon-scoped popularity via 3-way RRF. "
        "Products are filtered to the requested taxon, "
        "then re-ranked by session context and basket state."
    ),
)
async def recommend_taxon_page(
    req: TaxonPageRequest,
    background_tasks: BackgroundTasks,
) -> PlacementRecommendationResponse:
    if not store.catalog_ready:
        return _empty_placement(req.account_id, "taxon_page", "catalog_not_ready")

    session = store.get_session(req.account_id)
    device_type = session.get("device_type", "unknown")
    intent_score = session.get("intent_score", 0.0)
    basket = session.get("basket", [])
    exclude = set(req.exclude_product_ids)

    effective_top_n = req.top_n or DEVICE_TOP_N.get(device_type, 20)

    # ── Both upstream feeds, fetched concurrently ──────────────────────────────
    # Catalog API (:9000) supplies prebuilt device slots; the legacy shop feed
    # (:8018) covers the full taxonomy and seeds cold-start users.
    # Wall-clock cost is max(timeouts), not the sum; either side degrades to {}.
    catalog_feed, shop_feed = await upstream.fetch_both(req.account_id)

    # Both upstreams key by taxon *slug*; the request carries a taxon *id*.
    taxon_slug = store.taxon_id_to_name.get(req.taxon_id, "")

    # ── Prebuilt upstream slots, filled first; core engine fills the rest ──────
    upstream_ids = select(catalog_feed, taxon_slug, exclude, effective_top_n)
    exclude.update(upstream_ids)
    if len(upstream_ids) < effective_top_n:
        extra = select(
            shop_feed, taxon_slug, exclude, effective_top_n - len(upstream_ids)
        )
        upstream_ids.extend(extra)
        exclude.update(extra)
    remaining_n = max(0, effective_top_n - len(upstream_ids))

    # ── Cold-start seeds from the legacy shop feed ─────────────────────────────
    seed_products = store.get_user_top_products(req.account_id, top_n=10)
    if not seed_products:
        seed_products = flatten(shop_feed, exclude, 10)

    # ── Pipeline 1: CBF from user seed products, filtered to this taxon
    cbf_raw = (
        get_similar_products(
            seed_products,
            top_k=80,
            exclude_ids=exclude,
            require_in_stock=req.require_in_stock,
        )
        if seed_products and remaining_n > 0
        else []
    )
    cbf_candidates = [
        (pid, s)
        for pid, s in cbf_raw
        if store.product_features.get(pid, {}).get("taxon_id") == req.taxon_id
    ]

    # ── Pipeline 2: CF candidates, filtered to this taxon
    cf_raw = (
        get_cf_candidates(req.account_id, top_k=80, exclude_ids=exclude)
        if remaining_n > 0
        else []
    )
    cf_candidates = [
        (pid, s)
        for pid, s in cf_raw
        if store.product_features.get(pid, {}).get("taxon_id") == req.taxon_id
    ]

    # ── Pipeline 3: Taxon-scoped popularity
    pop_candidates = (
        get_taxon_products(
            req.taxon_id,
            top_k=80,
            exclude_ids=exclude,
            require_in_stock=req.require_in_stock,
        )
        if remaining_n > 0
        else []
    )

    # ── Merge core engine candidates
    core_strategy = "popular"
    if cbf_candidates or cf_candidates:
        merged = _rrf_merge_three(
            cbf_candidates,
            cf_candidates,
            pop_candidates,
            _TAXON_W_CBF,
            _TAXON_W_CF,
            _TAXON_W_POP,
        )
        core_strategy = (
            "cbf+cf+pop"
            if (cbf_candidates and cf_candidates)
            else ("cbf+pop" if cbf_candidates else "cf+pop")
        )
    else:
        merged = pop_candidates

    merged = _apply_shared_rerankers(merged, session, basket)
    core_ids = [pid for pid, _ in merged[:remaining_n]]

    # ── Assemble: prebuilt upstream slots first, core engine fills the remainder
    final_ids = upstream_ids + core_ids
    parts = []
    if upstream_ids:
        parts.append("upstream")
    if not store.get_user_top_products(req.account_id, top_n=1) and seed_products:
        parts.append("cold")
    parts.append(core_strategy)
    strategy = "+".join(parts)

    metrics.record_recommendations(
        count=len(final_ids), strategy=strategy, endpoint="taxon", device=device_type
    )
    background_tasks.add_task(_log_placement, req.account_id, final_ids, "taxon_page")

    return PlacementRecommendationResponse(
        account_id=req.account_id,
        placement="taxon_page",
        recommendations=final_ids,
        strategy=strategy,
        intent_score=round(intent_score, 2),
        device=device_type,
        count=len(final_ids),
        context_taxon_id=req.taxon_id,
        served_at=datetime.utcnow(),
    )


# ── 2. Product detail page ────────────────────────────────────────────────────


@router.post(
    "/product",
    response_model=PlacementRecommendationResponse,
    status_code=status.HTTP_200_OK,
    summary="Product detail page — 'You may also like' panel",
    description=(
        "Returns similar products for the product detail page. "
        "Primary signal: TF-IDF cosine similarity from the anchor product's content vector. "
        "Secondary signal: item-based CF co-interaction — users who viewed/bought the anchor "
        "product also interacted with these items. "
        "Merged via 2-way RRF (CBF 60% + CF 40%). "
        "Results are re-ranked by the user's session taxon path and basket state."
    ),
)
async def recommend_product_page(
    req: ProductPageRequest,
    background_tasks: BackgroundTasks,
) -> PlacementRecommendationResponse:
    if not store.catalog_ready:
        return _empty_placement(req.account_id, "product_page", "catalog_not_ready")

    session = store.get_session(req.account_id)
    device_type = session.get("device_type", "unknown")
    intent_score = session.get("intent_score", 0.0)
    basket = session.get("basket", [])
    exclude = set(req.exclude_product_ids)
    exclude.add(req.product_id)

    effective_top_n = req.top_n or DEVICE_TOP_N.get(device_type, 10)

    # ── Pipeline 1: CBF — TF-IDF cosine similarity from anchor product
    cbf_candidates = get_similar_products(
        [req.product_id],
        top_k=60,
        exclude_ids=exclude,
        require_in_stock=req.require_in_stock,
    )

    # ── Pipeline 2: CF item similarity — co-interaction across users
    cf_candidates = get_item_similar_products(
        req.product_id,
        top_k=60,
        exclude_ids=exclude,
    )

    # ── Cold start: augment CBF seeds with former rec engine when CF is empty
    if not cf_candidates and cold_start.is_cold_start(req.account_id):
        cs_pids = await cold_start.fetch(req.account_id, top_n=20)
        if cs_pids:
            extra_cbf = get_similar_products(
                [p for p in cs_pids if p not in exclude][:5],
                top_k=30,
                exclude_ids=exclude,
                require_in_stock=req.require_in_stock,
            )
            # Merge extra CBF at lower weight
            seen = {pid for pid, _ in cbf_candidates}
            cbf_candidates = cbf_candidates + [(p, s * 0.6) for p, s in extra_cbf if p not in seen]

    # ── Merge
    strategy = "cbf_similarity"
    if cf_candidates:
        merged = _rrf_merge_two(
            cbf_candidates,
            cf_candidates,
            _PRODUCT_W_CBF,
            _PRODUCT_W_CF,
        )
        strategy = "cbf+cf_similarity"
    else:
        merged = cbf_candidates

    # Fallback: taxon-scoped popularity if both pipelines empty
    if not merged:
        anchor_taxon = store.product_features.get(req.product_id, {}).get("taxon_id")
        if anchor_taxon:
            merged = get_taxon_products(
                anchor_taxon,
                top_k=effective_top_n * 2,
                exclude_ids=exclude,
                require_in_stock=req.require_in_stock,
            )
            strategy = "taxon_popular_fallback"
        # Last resort: cold-start products from former rec engine
        if not merged:
            cs_pids = await cold_start.fetch(req.account_id, top_n=effective_top_n)
            merged = [(p, 1.0) for p in cs_pids if p not in exclude]
            if merged:
                strategy = "cold_start_fallback"

    # ── Rerank and truncate
    merged = _apply_shared_rerankers(merged, session, basket)
    final_ids = [pid for pid, _ in merged[:effective_top_n]]

    anchor_taxon_id = store.product_features.get(req.product_id, {}).get("taxon_id")
    metrics.record_recommendations(
        count=len(final_ids), strategy=strategy, endpoint="product", device=device_type
    )
    background_tasks.add_task(_log_placement, req.account_id, final_ids, "product_page")

    return PlacementRecommendationResponse(
        account_id=req.account_id,
        placement="product_page",
        recommendations=final_ids,
        strategy=strategy,
        intent_score=round(intent_score, 2),
        device=device_type,
        count=len(final_ids),
        context_taxon_id=anchor_taxon_id,
        context_product_id=req.product_id,
        served_at=datetime.utcnow(),
    )


# ── 3. Basket / cart page ─────────────────────────────────────────────────────


@router.post(
    "/basket",
    response_model=PlacementRecommendationResponse,
    status_code=status.HTTP_200_OK,
    summary="Basket/cart page — cross-sell 'Complete your purchase' panel",
    description=(
        "Returns complementary products for the basket/cart page. "
        "Prioritises products from categories NOT already in the basket (cross-sell). "
        "Pipeline: CBF seeds from all basket items (blended vector) + CF co-purchase "
        "signals + global popularity. "
        "Basket-aware reranker heavily penalises same-category duplicates and boosts "
        "cross-category complements. "
        "If user has checked their lease limit this session, premium products are "
        "demoted in favour of budget/mid-tier items."
    ),
)
async def recommend_basket_page(
    req: BasketPageRequest,
    background_tasks: BackgroundTasks,
) -> PlacementRecommendationResponse:
    if not store.catalog_ready:
        return _empty_placement(req.account_id, "basket_page", "catalog_not_ready")

    session = store.get_session(req.account_id)
    device_type = session.get("device_type", "unknown")
    intent_score = session.get("intent_score", 0.0)
    limit_checked = session.get("limit_checked", False)

    # Combine explicit basket with session basket state
    all_basket = list(dict.fromkeys(req.basket_product_ids + session.get("basket", [])))
    exclude = set(all_basket) | set(req.exclude_product_ids)

    effective_top_n = req.top_n or DEVICE_TOP_N.get(device_type, 10)

    # ── Cold start: seed from the legacy shop feed when the user is new
    cs_pids: list[str] = []
    if cold_start.is_cold_start(req.account_id):
        cs_pids = await cold_start.fetch(req.account_id, top_n=20)

    # ── Pipeline 1: CBF from all basket items as seeds
    cbf_seed = all_basket + [p for p in cs_pids if p not in exclude][:5]
    cbf_candidates = get_similar_products(
        cbf_seed,
        top_k=80,
        exclude_ids=exclude,
        require_in_stock=req.require_in_stock,
    )

    # ── Pipeline 2: CF — aggregate co-interactions for all basket items
    cf_raw_combined: dict[str, float] = {}
    for basket_pid in all_basket[:5]:  # cap to avoid O(N²) on large baskets
        for pid, score in get_item_similar_products(
            basket_pid, top_k=50, exclude_ids=exclude
        ):
            cf_raw_combined[pid] = cf_raw_combined.get(pid, 0.0) + score
    cf_candidates = sorted(cf_raw_combined.items(), key=lambda x: x[1], reverse=True)

    # ── Pipeline 3: Global popularity (coverage for sparse CF/CBF)
    pop_products = store.get_popular_products(top_n=60)
    pop_candidates = [
        (pid, store._popularity.get(pid, 0.01))
        for pid in pop_products
        if pid not in exclude
    ]

    # ── Merge
    strategy = "basket_popular"
    if cbf_candidates or cf_candidates:
        merged = _rrf_merge_three(
            cbf_candidates,
            cf_candidates,
            pop_candidates,
            _BASKET_W_CBF,
            _BASKET_W_CF,
            _BASKET_W_POP,
        )
        strategy = (
            "basket_cbf+cf+pop"
            if (cbf_candidates and cf_candidates)
            else ("basket_cbf+pop" if cbf_candidates else "basket_cf+pop")
        )
    else:
        merged = pop_candidates

    # ── Basket-specific rerank: cross-sell boost is primary
    merged = _basket_aware_rerank(merged, all_basket, penalize_same_category=True)
    merged = _session_taxon_boost(merged, session.get("taxon_path", []))
    if limit_checked:
        merged = _limit_check_filter(merged, user_price_tier_max=1)
    merged = _diversity_cap(merged, max_per_shop=3)  # tighter diversity in basket panel

    final_ids = [pid for pid, _ in merged[:effective_top_n]]
    metrics.record_recommendations(
        count=len(final_ids), strategy=strategy, endpoint="basket", device=device_type
    )
    background_tasks.add_task(_log_placement, req.account_id, final_ids, "basket_page")

    return PlacementRecommendationResponse(
        account_id=req.account_id,
        placement="basket_page",
        recommendations=final_ids,
        strategy=strategy,
        intent_score=round(intent_score, 2),
        device=device_type,
        count=len(final_ids),
        served_at=datetime.utcnow(),
    )


# ── 4. Handset accessories ────────────────────────────────────────────────────


@router.post(
    "/handset/accessories",
    response_model=PlacementRecommendationResponse,
    status_code=status.HTTP_200_OK,
    summary="Handset product page — 'Compatible accessories' panel",
    description=(
        "Returns accessories and companion products for a specific handset/device page. "
        "Pipeline: (1) external Marketplace catalogue API feed filtered to accessory taxons, "
        "(2) TF-IDF CBF from the anchor handset seeded into accessory taxons, "
        "(3) item-based CF co-purchase for the handset, filtered to accessory taxons. "
        "External feed slots are filled first; internal engine fills the remainder via 2-way RRF. "
        "Re-ranked by session context, basket state, and diversity cap."
    ),
)
async def recommend_handset_accessories(
    req: HandsetAccessoriesRequest,
    background_tasks: BackgroundTasks,
) -> PlacementRecommendationResponse:
    if not store.catalog_ready:
        return _empty_placement(
            req.account_id, "handset_accessories", "catalog_not_ready"
        )

    session = store.get_session(req.account_id)
    device_type = session.get("device_type", "unknown")
    intent_score = session.get("intent_score", 0.0)
    basket = session.get("basket", [])
    exclude = set(req.exclude_product_ids)
    exclude.add(req.handset_product_id)

    effective_top_n = req.top_n or DEVICE_TOP_N.get(device_type, 10)

    # Resolve which catalog taxon IDs count as "accessory" for this handset
    if req.accessory_taxon_ids:
        accessory_taxon_id_set = set(req.accessory_taxon_ids)
    else:
        # Map well-known accessory slugs to their taxon IDs via the catalog maps
        accessory_taxon_id_set = {
            store.taxon_name_to_id[slug]
            for slug in ACCESSORY_TAXON_SLUGS
            if slug in store.taxon_name_to_id
        }

    # ── Upstream companion products (accessories + earphones + watches) ───────
    # Round-robin across the three slugs so one category can't take every slot.
    catalog_feed = await CATALOG_FEED.fetch(req.account_id)
    api_ids = upstream.select_many(
        catalog_feed,
        ACCESSORY_TAXON_SLUGS,
        exclude,
        effective_top_n,
        require_in_stock=req.require_in_stock,
    )
    exclude.update(api_ids)
    remaining_n = max(0, effective_top_n - len(api_ids))

    # ── Pipeline 1: CBF — TF-IDF similarity from handset product, accessory-scoped
    cbf_raw = (
        get_similar_products(
            [req.handset_product_id],
            top_k=80,
            exclude_ids=exclude,
            require_in_stock=req.require_in_stock,
        )
        if remaining_n > 0
        else []
    )
    cbf_candidates = [
        (pid, s)
        for pid, s in cbf_raw
        if not accessory_taxon_id_set
        or store.product_features.get(pid, {}).get("taxon_id") in accessory_taxon_id_set
    ]

    # ── Pipeline 2: CF item similarity — co-purchase signals for the handset
    cf_raw = (
        get_item_similar_products(
            req.handset_product_id,
            top_k=60,
            exclude_ids=exclude,
        )
        if remaining_n > 0
        else []
    )
    cf_candidates = [
        (pid, s)
        for pid, s in cf_raw
        if not accessory_taxon_id_set
        or store.product_features.get(pid, {}).get("taxon_id") in accessory_taxon_id_set
    ]

    # ── Merge core engine candidates
    core_strategy = "accessories_popular_fallback"
    if cbf_candidates or cf_candidates:
        if cf_candidates:
            merged = _rrf_merge_two(
                cbf_candidates,
                cf_candidates,
                _PRODUCT_W_CBF,
                _PRODUCT_W_CF,
            )
            core_strategy = "accessories_cbf+cf"
        else:
            merged = cbf_candidates
            core_strategy = "accessories_cbf"
    else:
        # Popularity fallback: pull from all accessory taxons
        merged_pop: dict[str, float] = {}
        for taxon_id in accessory_taxon_id_set:
            for pid, s in get_taxon_products(
                taxon_id,
                top_k=30,
                exclude_ids=exclude,
                require_in_stock=req.require_in_stock,
            ):
                merged_pop[pid] = max(merged_pop.get(pid, 0.0), s)
        merged = sorted(merged_pop.items(), key=lambda x: x[1], reverse=True)

    merged = _apply_shared_rerankers(merged, session, basket)
    core_ids = [pid for pid, _ in merged[:remaining_n]]

    final_ids = api_ids + core_ids
    strategy = f"marketplace_api+{core_strategy}" if api_ids else core_strategy

    metrics.record_recommendations(
        count=len(final_ids),
        strategy=strategy,
        endpoint="handset_accessories",
        device=device_type,
    )
    background_tasks.add_task(
        _log_placement, req.account_id, final_ids, "handset_accessories"
    )

    anchor_taxon_id = store.product_features.get(req.handset_product_id, {}).get(
        "taxon_id"
    )
    return PlacementRecommendationResponse(
        account_id=req.account_id,
        placement="handset_accessories",
        recommendations=final_ids,
        strategy=strategy,
        intent_score=round(intent_score, 2),
        device=device_type,
        count=len(final_ids),
        context_taxon_id=anchor_taxon_id,
        context_product_id=req.handset_product_id,
        served_at=datetime.utcnow(),
    )


# ── 5. Handset / device multi-taxon feed ──────────────────────────────────────


@router.post(
    "/handset/feed",
    response_model=HandsetFeedResponse,
    status_code=status.HTTP_200_OK,
    summary="Per-user multi-taxon handset/device feed",
    description=(
        "Returns personalised recommendations across all device categories "
        "(phones, tablets, wearables, earphones, accessories). "
        "Each taxon slot is filled first from the Marketplace Catalog API (:9000), "
        "then the TOKI Shop feed (:8018), then the internal TF-IDF CBF + taxon "
        "popularity engine. "
        "Defaults to the six DEVICE_TAXON_SLUGS; any catalog taxon slug works."
    ),
)
async def recommend_handset_feed(
    req: HandsetFeedRequest,
    background_tasks: BackgroundTasks,
) -> HandsetFeedResponse:
    if not store.catalog_ready:
        return HandsetFeedResponse(
            account_id=req.account_id,
            taxon_feeds=[],
            total_products=0,
            strategy="catalog_not_ready",
            served_at=datetime.utcnow(),
        )

    session = store.get_session(req.account_id)
    device_type = session.get("device_type", "unknown")
    intent_score = session.get("intent_score", 0.0)
    seed_products = store.get_user_top_products(req.account_id, top_n=15)

    requested_slugs = req.taxon_slugs or get_all_taxon_slugs()
    global_exclude = set(req.exclude_product_ids)

    # Fetch both upstreams once, concurrently, for all requested taxons.
    # Catalog API (:9000) covers the six device taxons; the legacy shop feed
    # (:8018) covers everything else, so requests for e.g. "tv" still resolve.
    catalog_feed, shop_feed = await upstream.fetch_both(req.account_id)
    handset_feed = catalog_feed
    if not seed_products:
        seed_products = flatten(shop_feed, global_exclude, 15)

    taxon_feeds: list[HandsetFeedTaxonItem] = []
    all_served_ids: list[str] = []

    for slug in requested_slugs:
        # Resolve taxon_id from catalog maps
        taxon_id = store.taxon_name_to_id.get(slug)
        slot_exclude = global_exclude | set(all_served_ids)

        # Upstream products for this taxon: catalog API first, then shop feed
        api_slot = select(catalog_feed, slug, slot_exclude, req.top_n_per_taxon)
        slot_exclude.update(api_slot)
        source = "catalog_api" if api_slot else "internal"
        if len(api_slot) < req.top_n_per_taxon:
            shop_slot = select(
                shop_feed, slug, slot_exclude, req.top_n_per_taxon - len(api_slot)
            )
            if shop_slot:
                source = "catalog_api+shop_feed" if api_slot else "shop_feed"
                api_slot.extend(shop_slot)
                slot_exclude.update(shop_slot)
        remaining_n = max(0, req.top_n_per_taxon - len(api_slot))

        internal_ids: list[str] = []

        if remaining_n > 0:
            if taxon_id:
                # CBF from user seed products filtered to this taxon
                cbf_raw = (
                    get_similar_products(
                        seed_products,
                        top_k=remaining_n * 3,
                        exclude_ids=slot_exclude,
                        require_in_stock=req.require_in_stock,
                    )
                    if seed_products
                    else []
                )
                cbf_taxon = [
                    (pid, s)
                    for pid, s in cbf_raw
                    if store.product_features.get(pid, {}).get("taxon_id") == taxon_id
                ]

                # Taxon-scoped popularity fallback
                pop_taxon = get_taxon_products(
                    taxon_id,
                    top_k=remaining_n * 2,
                    exclude_ids=slot_exclude,
                    require_in_stock=req.require_in_stock,
                )

                if cbf_taxon:
                    merged = _rrf_merge_two(
                        cbf_taxon, pop_taxon, _TAXON_W_CBF, _TAXON_W_POP
                    )
                    merged = _apply_shared_rerankers(merged, session, [])
                else:
                    merged = pop_taxon

                internal_ids = [pid for pid, _ in merged[:remaining_n]]
                if api_slot and internal_ids:
                    source = "mixed"

        final_slot = api_slot + internal_ids
        all_served_ids.extend(final_slot)

        taxon_feeds.append(
            HandsetFeedTaxonItem(
                taxon_slug=slug,
                taxon_id=taxon_id,
                recommendations=final_slot,
                count=len(final_slot),
                source=source,
            )
        )

    strategy = "upstream+cbf+pop" if (catalog_feed or shop_feed) else "cbf+pop"
    metrics.record_recommendations(
        count=len(all_served_ids),
        strategy=strategy,
        endpoint="handset_feed",
        device=device_type,
    )
    background_tasks.add_task(
        _log_placement, req.account_id, all_served_ids, "handset_feed"
    )

    return HandsetFeedResponse(
        account_id=req.account_id,
        taxon_feeds=taxon_feeds,
        total_products=len(all_served_ids),
        strategy=strategy,
        intent_score=round(intent_score, 2),
        device=device_type,
        served_at=datetime.utcnow(),
    )


# ── Helpers ────────────────────────────────────────────────────────────────────


def _empty_placement(
    account_id: str, placement: str, reason: str
) -> PlacementRecommendationResponse:
    return PlacementRecommendationResponse(
        account_id=account_id,
        placement=placement,
        recommendations=[],
        strategy=reason,
        count=0,
        served_at=datetime.utcnow(),
    )


_PLACEMENT_LOG_READY = False


def _log_placement(account_id: str, product_ids: list[str], placement: str) -> None:
    """Background task: log delivered placement recommendations."""
    if not product_ids:
        return
    try:
        from datetime import timezone

        import pandas as pd
        from sqlalchemy import create_engine

        from config import WRITE_DATABASE_URL

        now = datetime.now(timezone.utc).isoformat()
        df = pd.DataFrame(
            [
                {
                    "account_id": account_id,
                    "product_id": pid,
                    "strategy": placement,
                    "served_at": now,
                }
                for pid in product_ids
            ]
        )
        engine = create_engine(WRITE_DATABASE_URL, connect_args={"connect_timeout": 5})
        with engine.begin() as conn:
            df.to_sql(
                "rec_engine_delivery_log",
                con=conn,
                if_exists="append",
                index=False,
                chunksize=100,
            )
        engine.dispose()
    except Exception as exc:
        logger.debug(f"Placement log failed (non-critical): {exc}")
