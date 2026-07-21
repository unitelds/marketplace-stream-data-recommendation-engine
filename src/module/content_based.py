"""
Content-based filtering using TF-IDF cosine similarity over the product catalog.

Given seed product IDs (from user history) or a taxon_id, returns similar
product_ids ranked by cosine similarity in the TF-IDF space.

Falls back gracefully when the catalog is not yet loaded.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
from loguru import logger

from src.module.feature_store import store


def _get_seed_vector(seed_product_ids: list[str]):
    """
    Build a query vector by averaging the TF-IDF rows of seed products.

    Returns a dense (1, V) array or None if no seeds are in the index.
    """
    if store.tfidf_matrix is None or not seed_product_ids:
        return None

    indices = [
        store.product_id_to_idx[pid]
        for pid in seed_product_ids
        if pid in store.product_id_to_idx
    ]
    if not indices:
        return None

    # Average seed vectors → query vector
    seed_matrix = store.tfidf_matrix[indices]  # sparse [K × V]
    query = seed_matrix.mean(axis=0)  # dense (1, V)
    return np.asarray(query)  # shape (1, V)


def get_similar_products(
    seed_product_ids: list[str],
    top_k: int = 50,
    exclude_ids: Optional[set[str]] = None,
    require_in_stock: bool = True,
) -> list[tuple[str, float]]:
    """
    Find top_k products most similar to the seeds by TF-IDF cosine similarity.

    Returns:
        List of (product_id, score) sorted descending by similarity.
        Scores are in [0, 1] (cosine on L2-normalised vectors).
    """
    if not store.catalog_ready or store.tfidf_matrix is None:
        logger.debug("CBF: catalog not ready, returning empty")
        return []

    query = _get_seed_vector(seed_product_ids)
    if query is None:
        return []

    # Dot product with normalized matrix = cosine similarity
    # matrix shape: [N × V], query shape: [1 × V]
    scores = store.tfidf_matrix.dot(query.T)  # [N × 1]
    scores_flat = np.asarray(scores).flatten()

    exclude = set(seed_product_ids)
    if exclude_ids:
        exclude.update(exclude_ids)

    results: list[tuple[str, float]] = []
    # argsort descending
    for idx in np.argsort(scores_flat)[::-1]:
        if len(results) >= top_k:
            break
        pid = store.product_ids[idx]
        if pid in exclude:
            continue
        if require_in_stock:
            feat = store.product_features.get(pid, {})
            if feat.get("stock", 0) <= 0:
                continue
        results.append((pid, float(scores_flat[idx])))

    return results


def get_taxon_products(
    taxon_id: str,
    top_k: int = 50,
    exclude_ids: Optional[set[str]] = None,
    require_in_stock: bool = True,
) -> list[tuple[str, float]]:
    """
    Return products in a given taxon, optionally scoring by popularity.

    Used for cold-start when user clicked a taxon but has no product history.
    """
    if not store.catalog_ready:
        return []

    candidates = store.taxon_id_to_products.get(taxon_id, [])
    exclude = exclude_ids or set()
    results: list[tuple[str, float]] = []

    for pid in candidates:
        if pid in exclude:
            continue
        if require_in_stock:
            feat = store.product_features.get(pid, {})
            if feat.get("stock", 0) <= 0:
                continue
        score = store._popularity.get(pid, 0.0)
        results.append((pid, score))
        if len(results) >= top_k:
            break

    return sorted(results, key=lambda x: x[1], reverse=True)


def embed_query_text(text: str) -> Optional[np.ndarray]:
    """
    Vectorize a free-text query using the fitted TF-IDF vectorizer.

    Useful for future text-based search or query expansion.
    """
    if store.tfidf_vectorizer is None:
        return None
    try:
        vec = store.tfidf_vectorizer.transform([text])
        from sklearn.preprocessing import normalize as sk_normalize

        return np.asarray(sk_normalize(vec, norm="l2").todense())
    except Exception as e:
        logger.warning(f"embed_query_text failed: {e}")
        return None
