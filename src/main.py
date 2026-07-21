"""
Entry point for the TOKI Marketplace Recommendation Engine.

Run directly:
    python -m src.main

Or via uvicorn for hot-reload in development:
    uvicorn src.api.app:app --host 0.0.0.0 --port 8018 --reload

Production (gunicorn + uvicorn workers):
    gunicorn src.api.app:app -k uvicorn.workers.UvicornWorker \
        -b 0.0.0.0:8018 -w 4
"""

import os
import sys

import uvicorn

try:
    from config import APP_PORT, HOST, LOG_LEVEL, WORKERS
except ImportError:
    HOST = "0.0.0.0"
    APP_PORT = 8018
    LOG_LEVEL = "info"
    WORKERS = 1


def main() -> None:
    """Launch the recommendation API with uvicorn."""
    reload = os.getenv("TOKI_ENV", "production") != "production"
    uvicorn.run(
        "src.api.app:app",
        host=HOST,
        port=APP_PORT,
        log_level=LOG_LEVEL.lower(),
        reload=reload,
        workers=1 if reload else WORKERS,
    )


if __name__ == "__main__":
    main()
