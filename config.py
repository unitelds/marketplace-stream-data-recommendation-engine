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
CATALOG_QUERY = f"SELECT * FROM {CATALOG_TABLE} WHERE to_date(substr(created_at, 1, 10), 'YYYY-MM-DD') > to_date('20250901', 'YYYYMMDD');"

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
BATCH_SIZE = 100  # doubled: 32-core server has headroom
MAX_CONCURRENT_DB_OPERATIONS = (
    12  # aligned to actual pool capacity (pool_size+max_overflow=15)
)
USER_PREFERENCES_CACHE_TTL = 600
USER_PREFERENCES_CACHE_SIZE = 10000
CACHE_WARMUP_BATCH_SIZE = 40  # faster warmup on 32-core server
ENABLE_AGGRESSIVE_CACHING = True

# ─────────────────────────────────────────────────────────
# DB CONNECTION POOL
# ─────────────────────────────────────────────────────────
DB_POOL_SIZE = int(os.getenv("TOKI_DB_POOL_SIZE", 5))
DB_MAX_OVERFLOW = int(
    os.getenv("TOKI_DB_MAX_OVERFLOW", 10)
)  # 8 → 10: 24 workers × 15 max = 360 << PG limit 2000
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

# ─────────────────────────────────────────────────────────
# HANDSET FEED — per-user recommendations via external API
# ─────────────────────────────────────────────────────────
MARKETPLACE_API_BASE_URL = "http://10.21.60.94:9000/marketplace"  # {base}/{user_id}
MARKETPLACE_API_TIMEOUT = 2  # seconds — fall back to catalogue scoring on timeout

HANDSET_FEED_TABLE = "tmp_marketplace_handset_feed"  # retained for reference
HANDSET_FEED_MAP = {
    "handset-cellphone": "HANDSET_PROD_ID",
    "tablet": "TABLET_PROD_ID",
    "watch-and-smart-watches": "WATCH_PROD_ID",
    "headphones-earphones": "EARBUDS_PROD_ID",
    "handset-accessory": "ACCESSORY_PROD_ID",
    "cpe": "CPE_PROD_ID",
}
HANDSET_FEED_TOP_N = 100  # prebuilt products to prepend per taxon
HANDSET_FEED_CACHE_SIZE = 10000  # LRU entries (hot users)
HANDSET_FEED_CACHE_TTL = 3600  # 1 hour

# ─────────────────────────────────────────────────────────
# APPLICATION METADATA
# ─────────────────────────────────────────────────────────
APP_TITLE = "TOKI Marketplace Recommendation System v2"
APP_VERSION = "4.1.0"
APP_DESCRIPTION = (
    "Enhanced recommendation system with intelligent caching, "
    "dynamic feeds, prebuilt handset feed integration, expanded taxonomy, "
    "error analysis, and delivery tracking."
)
