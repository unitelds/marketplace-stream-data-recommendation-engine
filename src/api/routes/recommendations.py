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

from src.api.schemas.event import (
    BasketPageRequest,
    PlacementRecommendationResponse,
    ProductPageRequest,
    TaxonPageRequest,
)
from src.module.collaborative import get_cf_candidates, get_item_similar_products
from src.module.content_based import get_similar_products, get_taxon_products
from src.module.feature_store import store
from src.module.hybrid_ranker import (
    DEVICE_TOP_N,
    RRF_K,
    _basket_aware_rerank,
    _diversity_cap,
    _limit_check_filter,
    _session_taxon_boost,
)

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

    # ── Pipeline 1: CBF from user seed products, filtered to this taxon
    seed_products = store.get_user_top_products(req.account_id, top_n=10)
    cbf_raw = (
        get_similar_products(
            seed_products,
            top_k=80,
            exclude_ids=exclude,
            require_in_stock=req.require_in_stock,
        )
        if seed_products
        else []
    )
    # Keep only products belonging to the requested taxon
    cbf_candidates = [
        (pid, s)
        for pid, s in cbf_raw
        if store.product_features.get(pid, {}).get("taxon_id") == req.taxon_id
    ]

    # ── Pipeline 2: CF candidates, filtered to this taxon
    cf_raw = get_cf_candidates(req.account_id, top_k=80, exclude_ids=exclude)
    cf_candidates = [
        (pid, s)
        for pid, s in cf_raw
        if store.product_features.get(pid, {}).get("taxon_id") == req.taxon_id
    ]

    # ── Pipeline 3: Taxon-scoped popularity
    pop_candidates = get_taxon_products(
        req.taxon_id,
        top_k=80,
        exclude_ids=exclude,
        require_in_stock=req.require_in_stock,
    )

    # ── Merge
    strategy = "popular"
    if cbf_candidates or cf_candidates:
        merged = _rrf_merge_three(
            cbf_candidates,
            cf_candidates,
            pop_candidates,
            _TAXON_W_CBF,
            _TAXON_W_CF,
            _TAXON_W_POP,
        )
        strategy = (
            "cbf+cf+pop"
            if (cbf_candidates and cf_candidates)
            else ("cbf+pop" if cbf_candidates else "cf+pop")
        )
    else:
        merged = pop_candidates
        strategy = "popular"

    # ── Rerank and truncate
    merged = _apply_shared_rerankers(merged, session, basket)
    final_ids = [pid for pid, _ in merged[:effective_top_n]]

    response_taxon = req.taxon_id
    background_tasks.add_task(_log_placement, req.account_id, final_ids, "taxon_page")

    return PlacementRecommendationResponse(
        account_id=req.account_id,
        placement="taxon_page",
        recommendations=final_ids,
        strategy=strategy,
        intent_score=round(intent_score, 2),
        device=device_type,
        count=len(final_ids),
        context_taxon_id=response_taxon,
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

    # ── Rerank and truncate
    merged = _apply_shared_rerankers(merged, session, basket)
    final_ids = [pid for pid, _ in merged[:effective_top_n]]

    anchor_taxon_id = store.product_features.get(req.product_id, {}).get("taxon_id")
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

    # ── Pipeline 1: CBF from all basket items as seeds
    cbf_candidates = get_similar_products(
        all_basket,
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
