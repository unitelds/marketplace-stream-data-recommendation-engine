"""
FastAPI application factory with lifespan management.

Startup sequence:
  1. Create logs directory
  2. Trigger catalog sync from PostgreSQL (non-blocking — API serves immediately)
  3. Schedule periodic catalog re-sync background task

Shutdown:
  1. Cancel background sync task gracefully
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from src.api.routes.events import router as events_router
from src.api.routes.health import router as health_router
from src.module.catalog_sync import run_scheduled_sync, sync_catalog

try:
    from config import (
        APP_DESCRIPTION,
        APP_TITLE,
        APP_VERSION,
        LOG_DIR,
        SYNC_INTERVAL_MINUTES,
    )
except ImportError:
    APP_TITLE = "TOKI Recommendation Engine"
    APP_VERSION = "1.0.0"
    APP_DESCRIPTION = "Marketplace stream-data recommendation engine"
    LOG_DIR = "logs"
    SYNC_INTERVAL_MINUTES = 10

_sync_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown lifecycle."""
    global _sync_task

    # ── Startup ────────────────────────────────────────────────────────────────
    os.makedirs(LOG_DIR, exist_ok=True)
    logger.info(f"Starting {APP_TITLE} v{APP_VERSION}")

    # Kick off catalog sync in background — don't block startup
    asyncio.create_task(sync_catalog())
    logger.info("Initial catalog sync scheduled")

    # Start periodic re-sync
    _sync_task = asyncio.create_task(run_scheduled_sync(SYNC_INTERVAL_MINUTES))
    logger.info(f"Catalog auto-sync every {SYNC_INTERVAL_MINUTES} min")

    yield  # ← API is live here

    # ── Shutdown ───────────────────────────────────────────────────────────────
    if _sync_task and not _sync_task.done():
        _sync_task.cancel()
        try:
            await _sync_task
        except asyncio.CancelledError:
            pass
    logger.info("Recommendation engine shutdown complete")


def create_app() -> FastAPI:
    """Build and configure the FastAPI application."""
    app = FastAPI(
        title=APP_TITLE,
        version=APP_VERSION,
        description=APP_DESCRIPTION,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # CORS — restrict in production via environment variable
    allowed_origins = os.getenv("TOKI_CORS_ORIGINS", "*").split(",")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

    # Register routers
    app.include_router(health_router)
    app.include_router(events_router)

    # Root redirect to docs
    @app.get("/", include_in_schema=False)
    async def root():
        from fastapi.responses import RedirectResponse

        return RedirectResponse(url="/docs")

    return app


# Module-level app instance (used by uvicorn)
app = create_app()
