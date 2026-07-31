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
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from src.api.middleware.auth import APIKeyMiddleware, auth_stats
from src.api.routes.events import router as events_router
from src.api.routes.health import router as health_router
from src.api.routes.recommendations import router as recommendations_router
from src.module.catalog_sync import run_scheduled_sync, sync_catalog


class _TimingMiddleware(BaseHTTPMiddleware):
    """Attach X-Process-Time header to every response."""

    async def dispatch(self, request: Request, call_next):
        t0 = time.perf_counter()
        response = await call_next(request)
        response.headers["X-Process-Time"] = (
            f"{(time.perf_counter() - t0) * 1000:.1f}ms"
        )
        return response


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
_delivery_task: asyncio.Task | None = None


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

    # Start async delivery-log writer (batches queue → PostgreSQL every 5 s)
    from src.api.routes.events import _delivery_writer_loop

    _delivery_task = asyncio.create_task(_delivery_writer_loop())
    logger.info("Delivery log async writer started")

    yield  # ← API is live here

    # ── Shutdown ───────────────────────────────────────────────────────────────
    for task in (_sync_task, _delivery_task):
        if task and not task.done():
            task.cancel()
            try:
                await task
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

    # Middleware stack (applied bottom-up: timing → auth → cors → route)
    app.add_middleware(_TimingMiddleware)
    app.add_middleware(APIKeyMiddleware)

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
    app.include_router(recommendations_router)

    @app.get("/api/v1/auth/stats", include_in_schema=True, tags=["health"])
    async def _auth_stats():
        """API key usage counters and rate-limit bucket state."""
        return auth_stats()

    # Root redirect to docs
    @app.get("/", include_in_schema=False)
    async def root():
        from fastapi.responses import RedirectResponse

        return RedirectResponse(url="/docs")

    return app


# Module-level app instance (used by uvicorn)
app = create_app()
