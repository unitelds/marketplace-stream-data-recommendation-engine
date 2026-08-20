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

from src.api.routes.dashboard import router as dashboard_router
from src.api.routes.events import router as events_router
from src.api.routes.health import router as health_router
from src.api.routes.recommendations import router as recommendations_router
from src.module import marketplace_push, upstream
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
        ORACLE_POLL_INTERVAL_SECONDS,
        PUSH_ENABLED,
        PUSH_TOP_USERS_BATCH_SIZE,
        PUSH_TOP_USERS_COUNT,
        PUSH_TOP_USERS_INTERVAL_SECONDS,
        SYNC_INTERVAL_MINUTES,
    )
except ImportError:
    APP_TITLE = "TOKI Recommendation Engine"
    APP_VERSION = "1.0.0"
    APP_DESCRIPTION = "Marketplace stream-data recommendation engine"
    LOG_DIR = "logs"
    SYNC_INTERVAL_MINUTES = 10
    ORACLE_POLL_INTERVAL_SECONDS = 60
    PUSH_ENABLED = True
    PUSH_TOP_USERS_COUNT = 1000
    PUSH_TOP_USERS_INTERVAL_SECONDS = 600
    PUSH_TOP_USERS_BATCH_SIZE = 50

_sync_task: asyncio.Task | None = None
_delivery_task: asyncio.Task | None = None
_oracle_poll_task: asyncio.Task | None = None
_scheduled_push_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown lifecycle."""
    global _sync_task, _delivery_task, _oracle_poll_task, _scheduled_push_task

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

    # Start Oracle event poller (pulls consumer_events from Oracle automatically)
    if ORACLE_POLL_INTERVAL_SECONDS > 0:
        from src.module.oracle_poller import run_oracle_poll_loop

        _oracle_poll_task = asyncio.create_task(
            run_oracle_poll_loop(ORACLE_POLL_INTERVAL_SECONDS)
        )
        logger.info(
            f"Oracle event poller started (interval={ORACLE_POLL_INTERVAL_SECONDS}s)"
        )
    else:
        logger.info("Oracle event poller disabled (TOKI_ORACLE_POLL_INTERVAL=0)")

    # Start scheduled top-users push (every PUSH_TOP_USERS_INTERVAL_SECONDS)
    if PUSH_ENABLED:
        from src.module.scheduled_push import run_scheduled_push_loop

        _scheduled_push_task = asyncio.create_task(
            run_scheduled_push_loop(
                interval_seconds=PUSH_TOP_USERS_INTERVAL_SECONDS,
                top_n=PUSH_TOP_USERS_COUNT,
                push_url=marketplace_push.target_url(),
                batch_size=PUSH_TOP_USERS_BATCH_SIZE,
            )
        )
        logger.info(
            f"Scheduled push started — every {PUSH_TOP_USERS_INTERVAL_SECONDS}s, "
            f"top {PUSH_TOP_USERS_COUNT} users → {marketplace_push.target_url()}"
        )
    else:
        logger.info("Scheduled push disabled (TOKI_PUSH_ENABLED=false)")

    logger.info(
        "Upstream feeds — catalog: {}  |  shop: {}".format(
            upstream.CATALOG_FEED.base_url, upstream.SHOP_FEED.base_url
        )
    )
    if not marketplace_push.is_configured():
        logger.warning(
            "TOKI_MARKETPLACE_PUSH_TOKEN is not set — "
            f"{marketplace_push.target_url()} will reject deliveries with 401"
        )

    yield  # ← API is live here

    # ── Shutdown ───────────────────────────────────────────────────────────────
    for task in (_sync_task, _delivery_task, _oracle_poll_task, _scheduled_push_task):
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    await upstream.aclose()
    await marketplace_push.aclose()
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

    # Middleware stack
    app.add_middleware(_TimingMiddleware)

    # API-key auth. Off by default: the marketplace event stream currently calls
    # /api/v1/events without a key, so turning this on before the caller is
    # updated would silently drop ingestion. Enable with TOKI_AUTH_ENABLED=true
    # once TOKI_API_KEYS is provisioned and the shop sends X-API-Key.
    if os.getenv("TOKI_AUTH_ENABLED", "false").lower() == "true":
        from src.api.middleware.auth import APIKeyMiddleware

        app.add_middleware(APIKeyMiddleware)
        logger.info("API key authentication enabled")
    else:
        logger.warning(
            "API key authentication DISABLED — set TOKI_AUTH_ENABLED=true to enforce"
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
    app.include_router(recommendations_router)
    app.include_router(dashboard_router)

    # Root redirect to docs
    @app.get("/", include_in_schema=False)
    async def root():
        from fastapi.responses import RedirectResponse

        return RedirectResponse(url="/docs")

    return app


# Module-level app instance (used by uvicorn)
app = create_app()
