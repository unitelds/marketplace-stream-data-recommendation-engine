"""
In-memory feature store with optional Redis backend.

Holds:
  - Catalog DataFrame + TF-IDF vectors for content similarity
  - User session state (30-min sliding window per account)
  - User–item implicit feedback scores (time-decayed)
  - Taxon label/name → taxon_id lookup maps
  - Popularity signals for cold-start fallback
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import Any, Optional

import pandas as pd
from loguru import logger

SESSION_WINDOW_SECONDS = 1800  # 30-minute session inactivity window


class FeatureStore:
    """Thread-safe singleton in-memory feature store."""

    _instance: Optional["FeatureStore"] = None

    def __init__(self) -> None:
        # ── Catalog state ──────────────────────────────────────────────────────
        self.catalog_df: Optional[pd.DataFrame] = None
        self.catalog_ready: bool = False
        self.catalog_synced_at: Optional[float] = None
        self.catalog_size: int = 0

        # TF-IDF content vectors (set by catalog_sync)
        self.tfidf_matrix = None  # scipy sparse [N × V]
        self.tfidf_vectorizer = None  # fitted TfidfVectorizer
        self.product_ids: list[str] = []  # matrix row → product_id
        self.product_id_to_idx: dict[str, int] = {}

        # Rich per-product feature cache (product_id → dict)
        self.product_features: dict[str, dict] = {}

        # ── Taxon resolution maps ──────────────────────────────────────────────
        # Mongolian display name or slug → taxon_id
        self.taxon_label_map: dict[str, str] = {}
        # taxon slug → taxon_id
        self.taxon_name_to_id: dict[str, str] = {}
        # taxon_id → taxon slug (reverse map for response labelling)
        self.taxon_id_to_name: dict[str, str] = {}
        # taxon_id → list of product_ids
        self.taxon_id_to_products: dict[str, list[str]] = defaultdict(list)

        # ── User sessions ──────────────────────────────────────────────────────
        self._sessions: dict[str, dict] = {}

        # ── User-item implicit feedback ────────────────────────────────────────
        self._user_item_scores: dict[str, dict[str, float]] = defaultdict(dict)

        # ── Global popularity ──────────────────────────────────────────────────
        self._popularity: dict[str, float] = defaultdict(float)

        # ── Async write lock ───────────────────────────────────────────────────
        self._write_lock = asyncio.Lock()

    @classmethod
    def get(cls) -> "FeatureStore":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ──────────────────────────────────────────────────────────────────────────
    # Session management
    # ──────────────────────────────────────────────────────────────────────────

    async def update_session(
        self,
        account_id: str,
        *,
        taxon_id: Optional[str] = None,
        product_id: Optional[str] = None,
        intent_weight: float = 0.0,
        basket_add: Optional[str] = None,
        basket_remove: Optional[str] = None,
        limit_checked: bool = False,
        device_type: str = "unknown",
    ) -> None:
        async with self._write_lock:
            now = time.time()
            session = self._sessions.get(account_id)

            # Start new session if expired or missing
            if (
                session is None
                or (now - session.get("last_updated", 0)) > SESSION_WINDOW_SECONDS
            ):
                session = {
                    "taxon_path": [],
                    "basket": [],
                    "intent_score": 0.0,
                    "limit_checked": False,
                    "device_type": device_type,
                    "started_at": now,
                    "last_updated": now,
                }

            session["last_updated"] = now
            session["intent_score"] += max(0.0, intent_weight)

            if taxon_id:
                path = session["taxon_path"]
                # Avoid consecutive duplicates; keep last 10
                if not path or path[-1] != taxon_id:
                    path.append(taxon_id)
                    session["taxon_path"] = path[-10:]

            if basket_add and basket_add not in session["basket"]:
                session["basket"].append(basket_add)

            if basket_remove and basket_remove in session["basket"]:
                session["basket"].remove(basket_remove)

            if limit_checked:
                session["limit_checked"] = True

            if device_type not in ("unknown", ""):
                session["device_type"] = device_type

            self._sessions[account_id] = session

    def get_session(self, account_id: str) -> dict:
        """Return active session dict or {} if session has expired."""
        now = time.time()
        session = self._sessions.get(account_id, {})
        if session and (now - session.get("last_updated", 0)) > SESSION_WINDOW_SECONDS:
            return {}
        return session

    # ──────────────────────────────────────────────────────────────────────────
    # User-item feedback management
    # ──────────────────────────────────────────────────────────────────────────

    async def increment_user_item_score(
        self, account_id: str, product_id: str, delta: float
    ) -> None:
        async with self._write_lock:
            current = self._user_item_scores[account_id].get(product_id, 0.0)
            self._user_item_scores[account_id][product_id] = current + delta
            if delta > 0:
                self._popularity[product_id] = (
                    self._popularity.get(product_id, 0.0) + delta
                )

    def get_user_top_products(self, account_id: str, top_n: int = 20) -> list[str]:
        """Return account's highest-scored product_ids (positive scores only)."""
        scores = self._user_item_scores.get(account_id, {})
        positive = {pid: s for pid, s in scores.items() if s > 0}
        return sorted(positive, key=positive.__getitem__, reverse=True)[:top_n]

    def get_top_users(self, top_n: int = 1000) -> list[str]:
        """Return the top N most active users sorted by cumulative interaction score."""
        if not self._user_item_scores:
            return []
        totals = {
            uid: sum(s for s in sc.values() if s > 0)
            for uid, sc in self._user_item_scores.items()
        }
        return sorted(totals, key=totals.__getitem__, reverse=True)[:top_n]

    def get_user_score_for_product(self, account_id: str, product_id: str) -> float:
        return self._user_item_scores.get(account_id, {}).get(product_id, 0.0)

    def get_user_interacted_taxons(self, account_id: str, top_n: int = 5) -> list[str]:
        """Infer user's preferred taxons from their interaction-weighted product history."""
        top_pids = self.get_user_top_products(account_id, top_n=50)
        taxon_scores: dict[str, float] = defaultdict(float)
        scores = self._user_item_scores.get(account_id, {})
        for pid in top_pids:
            feat = self.product_features.get(pid, {})
            tid = feat.get("taxon_id")
            if tid:
                taxon_scores[tid] += scores.get(pid, 0.0)
        return sorted(taxon_scores, key=taxon_scores.__getitem__, reverse=True)[:top_n]

    def get_popular_products(
        self, taxon_id: Optional[str] = None, top_n: int = 20
    ) -> list[str]:
        """Return popular in-stock products, optionally scoped to a taxon."""
        in_stock = {
            pid for pid, f in self.product_features.items() if f.get("stock", 0) > 0
        }
        if taxon_id:
            candidates = self.taxon_id_to_products.get(taxon_id, [])
            scores = {
                pid: self._popularity.get(pid, 0.0)
                for pid in candidates
                if pid in in_stock
            }
        else:
            scores = {pid: s for pid, s in self._popularity.items() if pid in in_stock}
        return sorted(scores, key=scores.__getitem__, reverse=True)[:top_n]

    # ──────────────────────────────────────────────────────────────────────────
    # Diagnostics
    # ──────────────────────────────────────────────────────────────────────────

    def stats(self) -> dict:
        return {
            "catalog_ready": self.catalog_ready,
            "catalog_size": self.catalog_size,
            "catalog_synced_at": self.catalog_synced_at,
            "active_sessions": len(self._sessions),
            "tracked_users": len(self._user_item_scores),
            "popularity_entries": len(self._popularity),
            "taxon_label_map_size": len(self.taxon_label_map),
            "taxon_name_map_size": len(self.taxon_name_to_id),
            "taxon_id_to_name_size": len(self.taxon_id_to_name),
            "taxons_with_products": len(self.taxon_id_to_products),
        }


# Module-level singleton
store = FeatureStore.get()
