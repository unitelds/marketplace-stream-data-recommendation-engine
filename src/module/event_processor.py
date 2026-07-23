"""
Event normalization pipeline.

Handles:
  - Parsing ACTIVITYDATA (Python dict literal or JSON string from Oracle)
  - Flexible schema extraction (keys vary per activity type)
  - Taxon label → taxon_id resolution (Mongolian slug normalization)
  - consumer_events EVENTVALUE parsing for taxon_click
  - Device type detection from USERAGENT
"""

from __future__ import annotations

import ast
import json
import re
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger

from src.module.intent_scorer import get_intent_weight

# ─── Price parser ─────────────────────────────────────────────────────────────
_PRICE_RE = re.compile(r"[\d,]+(?:\.\d+)?")


def parse_price(value: Any) -> Optional[float]:
    """Extract numeric MNT price from '4100000 MNT' or plain number."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    m = _PRICE_RE.search(str(value))
    return float(m.group().replace(",", "")) if m else None


# ─── Safe JSON / Python-literal parser ───────────────────────────────────────
def safe_parse(value: Any) -> Any:
    """Parse value that may be a dict/list, a JSON string, or a Python literal."""
    if value is None:
        return {}
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, float) and value != value:  # NaN
        return {}
    s = str(value).strip()
    if not s or s in ("nan", "NaN", "None", "null"):
        return {}
    try:
        return json.loads(s)
    except (json.JSONDecodeError, ValueError):
        pass
    # Suppress SyntaxWarning from ast.literal_eval on legacy Oracle strings
    # that contain decimal literals like 0001 (valid in Python 2 but warned in 3.10+)
    import warnings

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SyntaxWarning)
            return ast.literal_eval(s)
    except Exception:
        pass
    return {}


# ─── Keyword list normalizer ──────────────────────────────────────────────────
def parse_keywords(value: Any) -> list[str]:
    """Parse keywords from JSON list, comma-separated string, or Python list."""
    if isinstance(value, list):
        return [str(k).strip() for k in value if k]
    if not value or (isinstance(value, float) and value != value):
        return []
    parsed = safe_parse(str(value))
    if isinstance(parsed, list):
        return [str(k).strip() for k in parsed if k]
    return [k.strip() for k in str(value).split(",") if k.strip()]


# ─── Specifications normalizer ────────────────────────────────────────────────
def flatten_specs(value: Any) -> str:
    """Parse specifications JSON → flat 'key value' string suitable for TF-IDF."""
    parsed = safe_parse(value)
    if isinstance(parsed, dict):
        parts = []
        for k, v in parsed.items():
            if isinstance(v, list):
                parts.append(f"{k} {' '.join(str(i) for i in v)}")
            else:
                parts.append(f"{k} {v}")
        return " ".join(parts)
    return str(value) if value else ""


# ─── Details field normalizer ─────────────────────────────────────────────────
def normalize_details(value: Any) -> str:
    """
    'details' column can be plain text OR a JSON object (images, product_state…).
    Extract readable text only.
    """
    if not value or (isinstance(value, float) and value != value):
        return ""
    parsed = safe_parse(str(value))
    if isinstance(parsed, dict):
        skip_keys = {"images", "image", "image_urls", "url_link", "checksum"}
        return " ".join(
            str(v)
            for k, v in parsed.items()
            if k not in skip_keys and isinstance(v, str) and len(v) > 4
        )
    return str(value)


# ─── Taxon EVENTVALUE parser (consumer_events taxon_click) ───────────────────
def extract_taxon_label(event_value: Any) -> Optional[str]:
    """
    consumer_events.EVENTVALUE for taxon_click:
      {'taxon': {'label': 'Гэрийн техник'}}  <- Mongolian display name
      {'taxon': {'label': 'household-appliances-multi-purpose-vacuum'}}  <- slug
    Returns the label string or None.
    """
    parsed = safe_parse(event_value)
    if isinstance(parsed, dict):
        taxon = parsed.get("taxon", {})
        if isinstance(taxon, dict):
            return (
                taxon.get("label")
                or taxon.get("id")
                or taxon.get("taxon_id")
                or taxon.get("slug")
            )
        if isinstance(taxon, str):
            return taxon
    return None


# ─── ACTIVITYDATA schema keys (varies across activity types) ─────────────────
_ACCOUNT_KEYS = ("accountid", "account_id", "userId", "user_id", "ACCOUNTID")
_PRODUCT_KEYS = ("productid", "product_id", "productId", "PRODUCTID")
_ACTION_KEYS = ("action", "type", "eventType", "event_type", "status", "event")
_SESSION_KEYS = ("sessionid", "session_id", "sessionId", "SESSIONID")
_QUANTITY_KEYS = ("quantity", "qty", "count")
_TAXON_KEYS = ("taxon_id", "taxonId", "taxon", "category_id", "categoryId")
_PRICE_KEYS = ("price", "mainprice", "saleprice", "MAINPRICE", "SALEPRICE", "amount")


def _first(d: dict, keys: tuple, default=None):
    """Return the first matching key value from dict (case-insensitive fallback)."""
    for k in keys:
        if k in d:
            return d[k]
    k_lower_map = {dk.lower(): dv for dk, dv in d.items()}
    for k in keys:
        val = k_lower_map.get(k.lower())
        if val is not None:
            return val
    return default


def parse_activity_data(raw: Any) -> dict:
    """
    Parse ACTIVITYDATA into a standard schema.

    The raw value comes from Oracle as a Python literal string. Fields vary by
    activity type (order-events, cart-events, wishlist-events, limit-events,
    view_product). This function extracts what's available with safe fallbacks.
    """
    parsed = safe_parse(raw)
    if not isinstance(parsed, dict):
        return {}

    account_id = _first(parsed, _ACCOUNT_KEYS)
    product_id = _first(parsed, _PRODUCT_KEYS)
    action = _first(parsed, _ACTION_KEYS, "default")
    session_id = _first(parsed, _SESSION_KEYS)
    quantity = _first(parsed, _QUANTITY_KEYS, 1)
    taxon_id = _first(parsed, _TAXON_KEYS)
    price_raw = _first(parsed, _PRICE_KEYS)

    # Timestamp extraction
    ts_raw = (
        parsed.get("timestamp")
        or parsed.get("created_at")
        or parsed.get("event_time")
        or parsed.get("eventTime")
    )
    timestamp: Optional[datetime] = None
    if ts_raw:
        try:
            if isinstance(ts_raw, (int, float)):
                epoch = ts_raw / 1000 if ts_raw > 1e10 else ts_raw
                timestamp = datetime.fromtimestamp(epoch, tz=timezone.utc)
            else:
                timestamp = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
        except Exception:
            pass

    return {
        "account_id": str(account_id).strip() if account_id else None,
        "product_id": str(product_id).strip() if product_id else None,
        "action": str(action).lower().strip() if action else "default",
        "session_id": str(session_id).strip() if session_id else None,
        "quantity": max(1, int(quantity)) if quantity else 1,
        "price": parse_price(price_raw),
        "taxon_id": str(taxon_id).strip() if taxon_id else None,
        "timestamp": timestamp,
        "raw": parsed,
    }


# ─── Full event normalization for customer_activities ────────────────────────
def normalize_event(
    activity_name: str,
    activity_data_raw: Any,
    taxon_label_map: dict[str, str],
    event_id: Optional[str] = None,
    event_timestamp: Optional[datetime] = None,
) -> Optional[dict]:
    """
    Normalize one customer_activities row into a standard event dict.

    Returns None for events that cannot be processed (no account_id).
    """
    if not activity_name:
        return None

    activity = parse_activity_data(activity_data_raw)
    if not activity.get("account_id"):
        logger.debug(f"Skipping {activity_name}: no account_id in ACTIVITYDATA")
        return None

    action = activity.get("action", "default")
    weight = get_intent_weight(activity_name, action)
    is_basket_add = activity_name == "cart-events" and action in ("add", "added")
    is_basket_remove = activity_name == "cart-events" and action in (
        "remove",
        "removed",
    )
    is_limit_check = activity_name == "limit-events"

    return {
        "event_id": event_id,
        "activity_name": activity_name,
        "account_id": activity["account_id"],
        "product_id": activity.get("product_id"),
        "action": action,
        "session_id": activity.get("session_id"),
        "quantity": activity.get("quantity", 1),
        "price": activity.get("price"),
        "taxon_id": activity.get("taxon_id"),
        "intent_weight": weight,
        "is_basket_add": is_basket_add,
        "is_basket_remove": is_basket_remove,
        "is_limit_check": is_limit_check,
        "timestamp": activity.get("timestamp") or event_timestamp,
    }


# ─── consumer_events normalization (taxon_click + product_click) ─────────────


def extract_product_ids_and_taxon(event_value: Any) -> tuple[list[str], Optional[str]]:
    """
    Parse product_click EVENTVALUE:
      {'productIds': ['pid1', 'pid2'], 'taxon': {'label': 'Mongolian'}}

    Returns (product_ids, taxon_label).
    """
    parsed = safe_parse(event_value)
    product_ids: list[str] = []
    taxon_label: Optional[str] = None

    if isinstance(parsed, dict):
        # productIds field (various key names)
        raw_pids = (
            parsed.get("productIds")
            or parsed.get("productId")
            or parsed.get("product_ids")
            or parsed.get("product_id")
        )
        if isinstance(raw_pids, list):
            product_ids = [str(p).strip() for p in raw_pids if p]
        elif raw_pids:
            product_ids = [str(raw_pids).strip()]

        taxon = parsed.get("taxon", {})
        if isinstance(taxon, dict):
            taxon_label = (
                taxon.get("label")
                or taxon.get("id")
                or taxon.get("taxon_id")
                or taxon.get("slug")
            )
        elif isinstance(taxon, str):
            taxon_label = taxon

    return product_ids, taxon_label


def _resolve_taxon_label(
    label: Optional[str], taxon_label_map: dict[str, str]
) -> Optional[str]:
    """Resolve a raw taxon label (Mongolian or slug) → taxon_id."""
    if not label:
        return None
    taxon_id = taxon_label_map.get(label)
    if not taxon_id:
        slug = re.sub(r"[\s_]+", "-", label.lower())
        taxon_id = taxon_label_map.get(slug)
    return taxon_id


def normalize_consumer_event(
    event_name: str,
    event_value_raw: Any,
    account_id: Optional[str],
    session_id: Optional[str],
    user_agent: Optional[str],
    taxon_label_map: dict[str, str],
    event_timestamp: Optional[datetime] = None,
) -> list[dict]:
    """
    Normalize consumer_events rows into a list of standard event dicts.

    Handles:
      taxon_click  → 1 event with taxon_id (no product)
      product_click → N events, one per product in productIds[], with taxon_id

    Returns empty list for unrecognised or unparseable events.
    """
    device = detect_device_type(user_agent)

    if event_name == "taxon_click":
        label = extract_taxon_label(event_value_raw)
        if not label:
            return []
        taxon_id = _resolve_taxon_label(label, taxon_label_map)
        return [
            {
                "event_id": None,
                "activity_name": "taxon_click",
                "account_id": account_id,
                "product_id": None,
                "taxon_id": taxon_id,
                "taxon_label_raw": label,
                "session_id": session_id,
                "user_agent": user_agent,
                "device_type": device,
                "intent_weight": get_intent_weight("taxon_click"),
                "is_basket_add": False,
                "is_basket_remove": False,
                "is_limit_check": False,
                "timestamp": event_timestamp,
            }
        ]

    if event_name == "product_click":
        product_ids, taxon_label = extract_product_ids_and_taxon(event_value_raw)
        if not product_ids:
            return []
        taxon_id = _resolve_taxon_label(taxon_label, taxon_label_map)
        weight = get_intent_weight("product_click")
        return [
            {
                "event_id": None,
                "activity_name": "product_click",
                "account_id": account_id,
                "product_id": pid,
                "taxon_id": taxon_id,
                "taxon_label_raw": taxon_label,
                "session_id": session_id,
                "user_agent": user_agent,
                "device_type": device,
                "intent_weight": weight,
                "is_basket_add": False,
                "is_basket_remove": False,
                "is_limit_check": False,
                "timestamp": event_timestamp,
            }
            for pid in product_ids
        ]

    return []  # Unrecognised consumer event type


def detect_device_type(user_agent: Optional[str]) -> str:
    """Classify device type from USERAGENT string."""
    if not user_agent:
        return "unknown"
    ua = user_agent.lower()
    if any(kw in ua for kw in ("miniprogram", "miniapp", "wechatminiprogram")):
        return "miniprogram"
    if any(kw in ua for kw in ("mobile", "android", "iphone", "ipad", "ipod")):
        return "mobile"
    return "desktop"
