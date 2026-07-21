"""
Engagement event weight definitions and time-decayed buying intent scoring.

Intent hierarchy (higher = stronger purchase signal):
  order-events complete → 5.0
  limit-events (lease check) → 4.0
  cart-events add → 3.5
  wishlist-events add → 3.0
  view_product → 1.0
  taxon_click → 0.5
  cart-events remove → -1.5  (negative: abandon signal)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

# ─── Per-event, per-action weight table ──────────────────────────────────────
INTENT_WEIGHTS: dict[str, dict[str, float]] = {
    "order-events": {
        "complete": 5.0,
        "completed": 5.0,
        "placed": 4.5,
        "initiated": 4.0,
        "default": 4.0,
    },
    "limit-events": {
        "checked": 4.0,
        "default": 4.0,
    },
    "cart-events": {
        "add": 3.5,
        "added": 3.5,
        "remove": -1.5,
        "removed": -1.5,
        "modified": 1.0,
        "update": 0.5,
        "default": 2.0,
    },
    "wishlist-events": {
        "add": 3.0,
        "added": 3.0,
        "remove": -0.5,
        "removed": -0.5,
        "default": 2.5,
    },
    "view_product": {"default": 1.0},
    "taxon_click": {"default": 0.5},
}

# Score halves every 7 days (time decay)
HALF_LIFE_DAYS = 7.0
DECAY_BASE = 0.5 ** (1.0 / HALF_LIFE_DAYS)

# Price tier ordinals for re-ranking with limit-check context
PRICE_RANGE_ORDINAL: dict[str, int] = {
    "budget": 0,
    "mid": 1,
    "mid-range": 1,
    "high-end": 2,
    "luxury": 3,
    "premium": 2,
}

PREMIUM_GRADE_ORDINAL: dict[str, int] = {
    "standard": 0,
    "mid": 1,
    "premium": 2,
    "ultra-premium": 3,
}


def get_intent_weight(activity_name: str, action: Optional[str] = None) -> float:
    """Return intent weight for an event type, refined by sub-action when available."""
    weights = INTENT_WEIGHTS.get(activity_name, {})
    if not weights:
        return 0.0
    if action:
        key = action.lower().strip()
        if key in weights:
            return weights[key]
    return weights.get("default", 0.0)


def apply_time_decay(base_score: float, event_timestamp: Optional[datetime]) -> float:
    """Apply exponential time-decay: score × decay_base^days_elapsed."""
    if event_timestamp is None or base_score == 0:
        return base_score
    now = datetime.now(timezone.utc)
    ts = event_timestamp
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    days_elapsed = max(0.0, (now - ts).total_seconds() / 86400.0)
    return base_score * (DECAY_BASE**days_elapsed)


def compute_session_intent(weights: list[float]) -> float:
    """Sum positive intent weights from all events in a session."""
    return sum(w for w in weights if w > 0)
