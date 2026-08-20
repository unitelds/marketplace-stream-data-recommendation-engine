# config.py - Centralized Configuration for TOKI Recommendation System
# Update values here when deploying from staging to production.
# All application modules import settings from this file.

import os
from enum import Enum

# ─────────────────────────────────────────────────────────
# ENVIRONMENT: "staging" or "production"
# ─────────────────────────────────────────────────────────
ENVIRONMENT = os.getenv("TOKI_ENV", "production")

# ─────────────────────────────────────────────────────────
# NETWORK / SERVER
# ─────────────────────────────────────────────────────────
HOST = os.getenv("TOKI_HOST", "0.0.0.0")
# Production ports
APP_PORT = int(os.getenv("TOKI_APP_PORT", 8018))
MONITORING_PORT = int(os.getenv("TOKI_MONITORING_PORT", 8019))
GUNICORN_BIND = f"{HOST}:{APP_PORT}"

# ─────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────
DB_USER = os.getenv("TOKI_DB_USER", "marketplace_user")
DB_PASSWORD = os.getenv("TOKI_DB_PASSWORD", "xUkOeEx5z4qnonm")
DB_HOST = os.getenv("TOKI_DB_HOST", "10.21.67.188")
DB_PORT = int(os.getenv("TOKI_DB_PORT", 5432))
DB_NAME = os.getenv("TOKI_DB_NAME", "postgres")
DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Write database — internal/created tables go here
WRITE_DB_NAME = os.getenv("TOKI_WRITE_DB_NAME", "marketplace")
WRITE_DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{WRITE_DB_NAME}"
)

# ─────────────────────────────────────────────────────────
# TABLE NAMES — change the suffix when promoting to prod
# ─────────────────────────────────────────────────────────
# Catalog table (staging vs production)
# staging  → marketplace_catalog_data_extended_version3_staging  (in postgres DB)
# production → marketplace_catalog_data_extended_version3 (in postgres DB)
# Former production table (archived): marketplace_catalog_data_extended_version2_archived_20260519
# Former staging  table (archived): marketplace_catalog_data_extended_version2_staging_archived_20260519
if ENVIRONMENT == "production":
    CATALOG_TABLE = "marketplace_catalog_data_extended_version3"
else:
    CATALOG_TABLE = "marketplace_catalog_data_extended_version3_staging"

# Profile table stays the same in all environments
PROFILE_TABLE = "master_profile_used"

# Internal tables — versioned
USER_RECOMMENDATIONS_TABLE = "user_recommendations_version2"
USER_PREFERENCES_TABLE = "user_preferences_version2"
SEARCH_LOGS_TABLE = "search_logs_version2"
CACHE_METRICS_TABLE = "cache_metrics_version2"
DELIVERED_RECOMMENDATIONS_TABLE = "marketplace_recommendations_delivered_version2"
ERROR_ANALYSIS_TABLE = "recommendation_error_analysis_version2"

# ─────────────────────────────────────────────────────────
# CATALOG QUERY — dynamic based on table name
# ─────────────────────────────────────────────────────────
# The whole table IS the live catalog (4,914 rows / 79 taxons as of 2026-08-20).
# The previous `created_at > 2025-09-01` filter silently dropped 5 products whose
# created_at predates the v3 migration; there is no business reason to hide them.
# Set TOKI_CATALOG_MIN_CREATED_AT=YYYYMMDD to re-enable a cutoff.
_CATALOG_MIN_CREATED_AT = os.getenv("TOKI_CATALOG_MIN_CREATED_AT", "").strip()
if _CATALOG_MIN_CREATED_AT:
    CATALOG_QUERY = (
        f"SELECT * FROM {CATALOG_TABLE} WHERE to_date(substr(created_at, 1, 10), "
        f"'YYYY-MM-DD') > to_date('{_CATALOG_MIN_CREATED_AT}', 'YYYYMMDD')"
    )
else:
    CATALOG_QUERY = f"SELECT * FROM {CATALOG_TABLE}"

# ─────────────────────────────────────────────────────────
# CACHE CONFIGURATION
# ─────────────────────────────────────────────────────────
RECOMMENDATION_CACHE_TTL = int(
    os.getenv("TOKI_REC_CACHE_TTL", 14400)
)  # 4 hours (extended: catalog stable on v3)
PRODUCT_CACHE_TTL = int(
    os.getenv("TOKI_PROD_CACHE_TTL", 7200)
)  # 2 hours (extended: less regeneration churn)
PROFILE_CACHE_TTL = int(os.getenv("TOKI_PROF_CACHE_TTL", 7200))  # 2 hours
BATCH_UPDATE_INTERVAL = 180
MAX_CACHE_AGE = 86400
ENABLE_SCHEDULED_SYNC = True
SYNC_INTERVAL_MINUTES = 10
ORACLE_POLL_INTERVAL_SECONDS = int(
    os.getenv("TOKI_ORACLE_POLL_INTERVAL", 60)
)  # 0 = disabled
BATCH_SIZE = 100  # doubled: 32-core server has headroom
MAX_CONCURRENT_DB_OPERATIONS = (
    16  # aligned to actual pool capacity (pool_size 8 + max_overflow 12 = 20)
)
USER_PREFERENCES_CACHE_TTL = 600
USER_PREFERENCES_CACHE_SIZE = 10000
CACHE_WARMUP_BATCH_SIZE = 40  # faster warmup on 32-core server
ENABLE_AGGRESSIVE_CACHING = True

# ─────────────────────────────────────────────────────────
# DB CONNECTION POOL
# ─────────────────────────────────────────────────────────
# 24 workers × (8 + 12) = 480 connections, well under the PG limit of 2000.
DB_POOL_SIZE = int(os.getenv("TOKI_DB_POOL_SIZE", 8))
DB_MAX_OVERFLOW = int(os.getenv("TOKI_DB_MAX_OVERFLOW", 12))
DB_POOL_TIMEOUT = 10
DB_POOL_RECYCLE = 600

DATABASE_POOL_CONFIG = {
    "pool_size": DB_POOL_SIZE,
    "max_overflow": DB_MAX_OVERFLOW,
    "pool_timeout": DB_POOL_TIMEOUT,
    "pool_recycle": DB_POOL_RECYCLE,
    "pool_pre_ping": True,
    "echo_pool": False,
    "pool_reset_on_return": "commit",
    "connect_args": {
        "connect_timeout": 5,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 3,
        "application_name": f"toki_rec_sys_{ENVIRONMENT}",
    },
}

# ─────────────────────────────────────────────────────────
# GUNICORN / UVICORN WORKERS
# ─────────────────────────────────────────────────────────
WORKERS = int(
    os.getenv("TOKI_WORKERS", 24)
)  # 16 → 24: utilise 75% of 32 cores (load avg was 1.0)
WORKER_CLASS = "uvicorn.workers.UvicornWorker"
WORKER_TIMEOUT = 120  # 300 → 120: flag hung requests faster (5 min too permissive)
KEEPALIVE = 5
MAX_REQUESTS = 5000  # 2000 → 5000: reduce worker-restart churn on stable server
MAX_REQUESTS_JITTER = 500  # 100 → 500: better stagger restarts across 24 workers

# ─────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────
LOG_DIR = os.getenv("TOKI_LOG_DIR", "logs")
LOG_LEVEL = os.getenv("TOKI_LOG_LEVEL", "INFO")
LOG_FILE = f"{LOG_DIR}/toki_recommendation_v2.log"
ERROR_REPORT_DIR = os.getenv("TOKI_ERROR_REPORT_DIR", "reports/error_analysis")

# ─────────────────────────────────────────────────────────
# RECOMMENDATION ENGINE
# ─────────────────────────────────────────────────────────
MAX_PRODUCTS_PER_CATEGORY = 100
DEFAULT_FEED_SHUFFLE = True
PRIORITY_PRODUCT_MAX = 20
# Max wall-clock seconds to wait for inline recs on the events endpoint.
# Recs that don't finish within this window are dropped; events are always accepted.
EVENTS_INFER_TIMEOUT = float(os.getenv("TOKI_EVENTS_INFER_TIMEOUT", 8.0))

# Expanded category taxonomy for computer components etc.
EXPANDED_TAXON_ALIASES = {
    "handset": ["handset-cellphone", "handset", "smartphone"],
    "tv": ["tv", "television", "televisions"],
    "tablet": ["tablet", "tablets"],
    "accessory": ["handset-accessory", "accessory", "accessories"],
    "laptop": ["laptop", "laptops", "notebook"],
    "desktop": ["desktop", "desktops", "pc"],
    "component": [
        "component",
        "components",
        "computer-component",
        "computer-components",
    ],
    "monitor": ["monitor", "monitors"],
    "networking": ["networking", "router", "routers", "switch", "switches"],
    "storage": ["storage", "hard-drive", "ssd", "external-storage"],
    "peripheral": ["peripheral", "peripherals", "keyboard", "mouse", "webcam"],
    "audio": ["audio", "headphone", "headphones", "speaker", "speakers", "earbuds"],
    "wearable": ["wearable", "wearables", "smartwatch", "fitness-tracker"],
    "gaming": ["gaming", "gaming-console", "gaming-accessory"],
    "smart-home": ["smart-home", "iot", "smart-device"],
    "camera": ["camera", "cameras", "action-camera"],
    "printer": ["printer", "printers", "scanner"],
}

# Priority products — recently released items shown first
PRIORITY_PRODUCTS = {
    "handset-cellphone": [
        "iphone 17",
        "iphone 17 pro max",
        "iphone 17 pro",
        "galaxy s25",
        "galaxy s25 ultra",
        "galaxy s25+",
        "pixel 9",
        "pixel 9 pro",
    ],
    "tv": ["samsung neo qled 2025", "lg oled 2025", "sony bravia 2025"],
    "tablet": ["ipad pro m4", "ipad air m3", "galaxy tab s10"],
    "laptop": ["macbook pro m4", "macbook air m4", "galaxy book5"],
    "component": [],
    "handset-accessory": [],
}

# Score weights for recommendation engine
SCORE_WEIGHTS = {
    "brand_preference": 0.20,
    "price_range": 0.15,
    "stock_availability": 0.10,
    "feature_match": 0.20,
    "premium_grade": 0.08,
    "content_compatibility": 0.07,
    "recency": 0.10,
    "popularity": 0.05,
    "diversity_bonus": 0.05,
}

# ═════════════════════════════════════════════════════════════════════════════
# EXTERNAL INTEGRATION TOPOLOGY
# ═════════════════════════════════════════════════════════════════════════════
# Verified against the live services on 2026-08-20.
#
#   INBOUND  (this engine reads)
#     10.21.60.94:9000  Marketplace Catalog API — "Handset-shop feed"
#                       GET /marketplace/{account_id}
#                       6 device taxons: handset · tablet · watch · earphones ·
#                       accessory · cpe.  Read-only (OpenAPI exposes only
#                       /health, /ready, /marketplace/{user_id}).
#     10.21.60.94:8018  TOKI Shop Feed — legacy demographic model
#                       GET /api/recommendations/{account_id}
#                       Full catalogue (~80 taxons).  Always answers, using
#                       demographic cohorts for unknown users → cold-start bridge.
#
#   INBOUND  (marketplace.toki.mn writes to us)
#     POST /api/v1/events, /api/v1/consumer-events   engagement stream
#
#   OUTBOUND (this engine writes)
#     POST {MARKETPLACE_PUSH_URL}                    recommendation delivery
#
# Both upstreams share one response envelope:
#   {"userId": "...", "taxonRecommendations": {"<slug>": ["<productId>", ...]}}
# ─────────────────────────────────────────────────────────────────────────────

# Device taxons carried by the Marketplace Catalog API (port 9000).
# These slugs match taxon_name in the catalog table one-for-one.
DEVICE_TAXON_SLUGS = (
    "handset-cellphone",
    "tablet",
    "watch-and-smart-watches",
    "headphones-earphones",
    "handset-accessory",
    "cpe",
)

# Companion categories offered alongside a handset on the product page.
ACCESSORY_TAXON_SLUGS = (
    "handset-accessory",
    "headphones-earphones",
    "watch-and-smart-watches",
)

# ── INBOUND 1: Marketplace Catalog API (handset-shop feed, port 9000) ─────────
CATALOG_FEED_URL = os.getenv(
    "TOKI_CATALOG_FEED_URL", "http://10.21.60.94:9000/marketplace"
)
CATALOG_FEED_TIMEOUT = float(os.getenv("TOKI_CATALOG_FEED_TIMEOUT", 1.5))
CATALOG_FEED_CACHE_TTL = int(os.getenv("TOKI_CATALOG_FEED_CACHE_TTL", 3600))  # 1 h
CATALOG_FEED_CACHE_SIZE = int(os.getenv("TOKI_CATALOG_FEED_CACHE_SIZE", 20_000))

# ── INBOUND 2: TOKI Shop Feed (legacy demographic engine, port 8018) ──────────
SHOP_FEED_URL = os.getenv(
    "TOKI_SHOP_FEED_URL", "http://10.21.60.94:8018/api/recommendations"
)
SHOP_FEED_TIMEOUT = float(os.getenv("TOKI_SHOP_FEED_TIMEOUT", 1.5))
SHOP_FEED_CACHE_TTL = int(os.getenv("TOKI_SHOP_FEED_CACHE_TTL", 600))  # 10 min
SHOP_FEED_CACHE_SIZE = int(os.getenv("TOKI_SHOP_FEED_CACHE_SIZE", 50_000))

# Shared httpx connection-pool ceiling for all upstream + push traffic.
UPSTREAM_MAX_CONNECTIONS = int(os.getenv("TOKI_UPSTREAM_MAX_CONNECTIONS", 100))

# ── OUTBOUND: recommendation delivery to marketplace.toki.mn ──────────────────
# Both hosts require `Authorization: Bearer <token>` — an unauthenticated call
# returns 401 {"message":"Authentication required"}.  Set the token at deploy
# time; never commit it.
_PUSH_URL_STAGING = "https://staging-marketplace.toki.mn/ms/catalogue/v1/recommendation"
_PUSH_URL_PRODUCTION = "https://marketplace.toki.mn/ms/catalogue/v1/recommendation"
MARKETPLACE_PUSH_URL = os.getenv(
    "TOKI_MARKETPLACE_PUSH_URL",
    _PUSH_URL_PRODUCTION if ENVIRONMENT == "production" else _PUSH_URL_STAGING,
)
MARKETPLACE_PUSH_TOKEN = os.getenv("TOKI_MARKETPLACE_PUSH_TOKEN", "")
MARKETPLACE_PUSH_TIMEOUT = float(os.getenv("TOKI_MARKETPLACE_PUSH_TIMEOUT", 5.0))
MARKETPLACE_PUSH_RETRIES = int(os.getenv("TOKI_MARKETPLACE_PUSH_RETRIES", 2))
# Request-body shape: "products" | "product_ids" | "taxon_map" — see
# src/module/marketplace_push.py.  Production rejects "products" with
# 400 {"message": "productId and accountId are required"}; confirm the contract
# with the marketplace team and set this accordingly.
MARKETPLACE_PUSH_PAYLOAD_FORMAT = os.getenv(
    "TOKI_MARKETPLACE_PUSH_PAYLOAD_FORMAT", "products"
)

# ── SCHEDULED TOP-USERS PUSH ─────────────────────────────────────────────────
# Pushes personalized feeds for the top PUSH_TOP_USERS_COUNT most active users
# every PUSH_TOP_USERS_INTERVAL_SECONDS.  Only ONE gunicorn worker runs the loop
# (file-lock coordination).
#
# OFF by default (2026-08-20): a live delivery came back
#   400 {"message": "productId and accountId are required"}
# so the loop would fire ~1000 rejected POSTs at production every 10 minutes.
# Re-enable with TOKI_PUSH_ENABLED=true once delivery is confirmed working.
# Event ingestion and all placement endpoints are unaffected by this switch.
PUSH_ENABLED = os.getenv("TOKI_PUSH_ENABLED", "false").lower() == "true"
PUSH_TOP_USERS_COUNT = int(os.getenv("TOKI_PUSH_TOP_USERS", 1000))
PUSH_TOP_USERS_INTERVAL_SECONDS = int(os.getenv("TOKI_PUSH_INTERVAL", 600))  # 10 min
PUSH_TOP_USERS_BATCH_SIZE = int(os.getenv("TOKI_PUSH_BATCH_SIZE", 50))

# Prebuilt upstream products to prepend per taxon slot before the core engine
# fills the remainder.
FEED_TOP_N_PER_TAXON = int(os.getenv("TOKI_FEED_TOP_N_PER_TAXON", 100))

# ── Deprecated aliases — retained so out-of-tree callers keep importing ───────
# Prefer CATALOG_FEED_URL / SHOP_FEED_URL / MARKETPLACE_PUSH_URL.
MARKETPLACE_API_BASE_URL = MARKETPLACE_PUSH_URL
MARKETPLACE_API_TIMEOUT = MARKETPLACE_PUSH_TIMEOUT
FORMER_REC_ENGINE_URL = SHOP_FEED_URL
FORMER_REC_ENGINE_TIMEOUT = SHOP_FEED_TIMEOUT
FORMER_REC_ENGINE_CACHE_TTL = SHOP_FEED_CACHE_TTL
FORMER_REC_ENGINE_CACHE_SIZE = SHOP_FEED_CACHE_SIZE
HANDSET_FEED_TOP_N = FEED_TOP_N_PER_TAXON
HANDSET_FEED_CACHE_SIZE = CATALOG_FEED_CACHE_SIZE
HANDSET_FEED_CACHE_TTL = CATALOG_FEED_CACHE_TTL

# ─────────────────────────────────────────────────────────
# APPLICATION METADATA
# ─────────────────────────────────────────────────────────
APP_TITLE = "TOKI Marketplace Recommendation System v2"
APP_VERSION = "4.3.0"
APP_DESCRIPTION = (
    "Engagement-based recommendation engine for the TOKI marketplace. "
    "Ingests the marketplace event stream, blends TF-IDF content-based and "
    "item-based collaborative filtering with two upstream feeds "
    "(Marketplace Catalog API :9000 and TOKI Shop Feed :8018), and delivers "
    "personalised recommendations back to marketplace.toki.mn."
)

# ─────────────────────────────────────────────────────────
# SECURITY — API key authentication
# ─────────────────────────────────────────────────────────
# Format: "key1:tier1,key2:tier2"   tiers: internal | standard | readonly
# Override at deploy time — never commit real keys to source control.
# The middleware reads this at import time from the environment.
# Example:
#   TOKI_API_KEYS="abc123:internal,xyz789:standard,mon456:readonly"
API_KEYS_ENV_VAR = "TOKI_API_KEYS"

# ─────────────────────────────────────────────────────────
# PERFORMANCE TUNING — 40 GB RAM / 32 CPU server
# ─────────────────────────────────────────────────────────
# Worker sizing: 2 × CPU + 1 is the classic rule; we cap at 28 to leave
# headroom for the OS and background catalog-sync threads.
# Each UvicornWorker is async — 1000 concurrent users ÷ 24 workers ≈ 42
# in-flight coroutines per worker, well within asyncio capacity.
PEAK_CONCURRENT_USERS = int(os.getenv("TOKI_PEAK_USERS", 1000))

# Feature store keeps all product vectors in RAM.
# 4914 products × 30000 TF-IDF float32 features ≈ 590 MB dense (actual: ~8 MB
# after scipy sparse compression).  User-item matrix grows with traffic;
# 1000 users × 500 products × 8 bytes ≈ 4 MB. Well within 40 GB budget.
FEATURE_STORE_MAX_USERS = int(os.getenv("TOKI_FS_MAX_USERS", 500_000))
FEATURE_STORE_MAX_PRODUCTS_PER_USER = int(os.getenv("TOKI_FS_MAX_PROD_PER_USER", 1000))

