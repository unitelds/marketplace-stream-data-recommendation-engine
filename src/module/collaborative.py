"""
Item-based Collaborative Filtering using the in-memory user–item interaction matrix.

Approach: co-interaction scoring
  For each seed product the user has interacted with, find all other users who
  also interacted with it (co-interactors). Aggregate what those co-interactors
  liked to surface items that behaviorally similar users preferred.

  Score(candidate) = Σ_seed  [ sim(seed, candidate) × w_seed ]

  where sim(seed, candidate) is the Jaccard-weighted co-interaction count between
  the two items' user sets, multiplied by the anchor user's interaction weight
  for the seed.

This is lightweight, fully in-memory, and improves as more users interact with
the platform. Falls back gracefully to empty list when interaction data is sparse.

Public API
----------
  get_cf_candidates(account_id, top_k, exclude_ids) → list[(product_id, score)]
  get_item_similar_products(product_id, top_k, exclude_ids) → list[(product_id, score)]
  cf_stats() → dict
"""

from __future__ import annotations

from collections import defaultdict
from typing import Optional

from loguru import logger

from src.module.feature_store import store

# Minimum co-interactions needed to count a user as a "bridge" between items
_MIN_BRIDGE_SCORE = 0.5

# Max users examined per seed product (keeps the inner loop O(k × U_max))
_MAX_USERS_PER_SEED = 200

# Soft-cap on how many co-users we examine per user-seed pair
_MAX_SEEDS = 15


def _get_item_user_map() -> dict[str, dict[str, float]]:
    """
    Build inverted index: product_id → {user_id: score}.

    Built on-the-fly from the feature store's user-item matrix.
    Cheap enough for thousands of users; cache if needed later.
    """
    item_users: dict[str, dict[str, float]] = defaultdict(dict)
    for user_id, item_scores in store._user_item_scores.items():
        for pid, score in item_scores.items():
            if score > 0:
                item_users[pid][user_id] = score
    return item_users


def get_cf_candidates(
    account_id: str,
    top_k: int = 50,
    exclude_ids: Optional[set[str]] = None,
) -> list[tuple[str, float]]:
    """
    Return top_k CF-based product candidates for account_id.

    Steps:
      1. Get user's top seed products (by intent score)
      2. For each seed, find co-interactors (other users who touched that product)
      3. For each co-interactor, gather their other interactions (weighted by their score)
      4. Aggregate candidate scores; normalise by co-interactor count
      5. Return sorted descending, excluding seeds and exclude_ids

    Falls back to [] when the user has no interaction history.
    """
    user_scores = store._user_item_scores.get(account_id, {})
    if not user_scores:
        return []

    # Seeds: up to _MAX_SEEDS positive-score products
    seeds = sorted(
        ((pid, s) for pid, s in user_scores.items() if s > _MIN_BRIDGE_SCORE),
        key=lambda x: x[1],
        reverse=True,
    )[:_MAX_SEEDS]

    if not seeds:
        return []

    exclude = set(exclude_ids or [])
    exclude.update(pid for pid, _ in seeds)

    item_users = _get_item_user_map()

    # Candidate score accumulator: product_id → weighted score sum
    candidate_scores: dict[str, float] = defaultdict(float)
    candidate_support: dict[str, int] = defaultdict(int)  # co-interactor count

    for seed_pid, seed_weight in seeds:
        co_users = item_users.get(seed_pid, {})
        if not co_users:
            continue

        # Limit to top co-interactors by their interaction strength
        top_co_users = sorted(co_users.items(), key=lambda x: x[1], reverse=True)[
            :_MAX_USERS_PER_SEED
        ]

        for co_user_id, co_score in top_co_users:
            if co_user_id == account_id:
                continue

            # Every product this co-user touched (except seeds/excluded)
            co_interactions = store._user_item_scores.get(co_user_id, {})
            for candidate_pid, c_score in co_interactions.items():
                if candidate_pid in exclude or c_score <= 0:
                    continue

                # Weight = seed_weight × co_score × candidate_score (normalised)
                contribution = seed_weight * (co_score / (co_score + 1.0)) * c_score
                candidate_scores[candidate_pid] += contribution
                candidate_support[candidate_pid] += 1

    if not candidate_scores:
        return []

    # Normalise by support count to avoid popularity bias
    normalised = {
        pid: score / (1.0 + candidate_support[pid] ** 0.5)
        for pid, score in candidate_scores.items()
    }

    # Filter to in-stock products only
    results = [
        (pid, score)
        for pid, score in sorted(normalised.items(), key=lambda x: x[1], reverse=True)
        if store.product_features.get(pid, {}).get("stock", 0) > 0
    ]

    return results[:top_k]


def get_item_similar_products(
    product_id: str,
    top_k: int = 50,
    exclude_ids: Optional[set[str]] = None,
) -> list[tuple[str, float]]:
    """
    Find products frequently co-interacted with product_id across all users.

    Used for the product-detail-page "similar products" panel:
      - Pulls all users who interacted with this product
      - Aggregates their other interactions
      - Scores candidates by co-occurrence strength and user intent weight

    Returns list of (product_id, score) sorted descending.
    Falls back to [] when the product has no interaction data.
    """
    item_users = _get_item_user_map()
    co_users = item_users.get(product_id, {})

    if not co_users:
        logger.debug(f"CF item-similarity: no co-users for {product_id[:12]}…")
        return []

    exclude = set(exclude_ids or {})
    exclude.add(product_id)

    candidate_scores: dict[str, float] = defaultdict(float)
    candidate_support: dict[str, int] = defaultdict(int)

    top_co_users = sorted(co_users.items(), key=lambda x: x[1], reverse=True)[
        :_MAX_USERS_PER_SEED
    ]

    for co_user_id, anchor_score in top_co_users:
        co_interactions = store._user_item_scores.get(co_user_id, {})
        for candidate_pid, c_score in co_interactions.items():
            if candidate_pid in exclude or c_score <= 0:
                continue
            candidate_scores[candidate_pid] += anchor_score * c_score
            candidate_support[candidate_pid] += 1

    if not candidate_scores:
        return []

    normalised = {
        pid: score / (1.0 + candidate_support[pid] ** 0.5)
        for pid, score in candidate_scores.items()
    }

    results = [
        (pid, score)
        for pid, score in sorted(normalised.items(), key=lambda x: x[1], reverse=True)
        if store.product_features.get(pid, {}).get("stock", 0) > 0
    ]

    return results[:top_k]


def cf_stats() -> dict:
    """Diagnostic stats for the CF layer."""
    item_users = _get_item_user_map()
    total_interactions = sum(len(scores) for scores in store._user_item_scores.values())
    avg_items_per_user = (
        total_interactions / len(store._user_item_scores)
        if store._user_item_scores
        else 0.0
    )
    return {
        "tracked_users": len(store._user_item_scores),
        "total_user_item_interactions": total_interactions,
        "avg_items_per_user": round(avg_items_per_user, 2),
        "items_with_interactions": len(item_users),
    }
