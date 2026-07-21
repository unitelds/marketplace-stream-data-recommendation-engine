"""
Health and status endpoints.

GET /api/v1/health         — liveness check (always 200)
GET /api/v1/catalog/status — catalog sync state + TF-IDF stats
GET /api/v1/catalog/sync   — manually trigger a catalog re-sync
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks

from src.module.catalog_sync import sync_catalog
from src.module.feature_store import store

try:
    from config import APP_TITLE, APP_VERSION, ENVIRONMENT
except ImportError:
    APP_TITLE = "TOKI Recommendation Engine"
    APP_VERSION = "1.0.0"
    ENVIRONMENT = "development"

router = APIRouter(prefix="/api/v1", tags=["health"])


@router.get(
    "/health",
    summary="Liveness check",
    response_model=dict,
)
async def health() -> dict:
    """Always returns 200. Used by load balancers and container probes."""
    stats = store.stats()
    return {
        "status": "ok",
        "app": APP_TITLE,
        "version": APP_VERSION,
        "environment": ENVIRONMENT,
        "catalog_ready": stats["catalog_ready"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get(
    "/catalog/status",
    summary="Catalog and feature store diagnostics",
    response_model=dict,
)
async def catalog_status() -> dict:
    """Returns detailed catalog stats, TF-IDF index dimensions, and session counts."""
    stats = store.stats()
    tfidf_shape = None
    if store.tfidf_matrix is not None:
        tfidf_shape = list(store.tfidf_matrix.shape)

    synced_at = None
    if stats.get("catalog_synced_at"):
        synced_at = datetime.fromtimestamp(
            stats["catalog_synced_at"], tz=timezone.utc
        ).isoformat()
        age_minutes = round((time.time() - stats["catalog_synced_at"]) / 60, 1)
    else:
        age_minutes = None

    return {
        "catalog_ready": stats["catalog_ready"],
        "catalog_size": stats["catalog_size"],
        "catalog_synced_at": synced_at,
        "catalog_age_minutes": age_minutes,
        "tfidf_shape": tfidf_shape,
        "taxon_label_map_size": stats["taxon_label_map_size"],
        "taxon_name_map_size": stats["taxon_name_map_size"],
        "taxons_with_products": stats["taxons_with_products"],
        "active_sessions": stats["active_sessions"],
        "tracked_users": stats["tracked_users"],
        "popularity_entries": stats["popularity_entries"],
    }


@router.post(
    "/catalog/sync",
    summary="Trigger manual catalog re-sync",
    response_model=dict,
)
async def trigger_catalog_sync(background_tasks: BackgroundTasks) -> dict:
    """Enqueues a forced catalog re-sync in the background and returns immediately."""
    background_tasks.add_task(sync_catalog, True)
    return {
        "status": "sync_triggered",
        "message": "Catalog re-sync started in background. Check /catalog/status for progress.",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
