"""
Stress / load test for the TOKI Recommendation Engine.

Simulates peak-hour traffic: 1000 concurrent marketplace users
performing realistic action sequences across all three recommendation
placements plus event ingestion.

Usage
-----
# Quick smoke test (30 s, ramp to 50 users)
python tests/stress_test.py --smoke

# Full peak-load test (2 min, ramp to 1000 users)
python tests/stress_test.py --peak

# Locust web UI (interactive, set users/spawn rate in browser)
locust -f tests/stress_test.py --host=http://localhost:8018

Requirements: locust, aiohttp (both installed via pip)

Environment
-----------
STRESS_API_KEY   API key to use (default: toki-internal-key)
STRESS_HOST      Base URL       (default: http://localhost:8018)
"""

from __future__ import annotations

import json
import os
import random
import sys
import time
import uuid

from locust import FastHttpUser, between, events, task
from locust.env import Environment
from locust.stats import stats_history, stats_printer

# ── Configuration ──────────────────────────────────────────────────────────────

API_KEY = os.getenv("STRESS_API_KEY", "")
BASE_URL = os.getenv("STRESS_HOST", "http://localhost:8018")
HEADERS = {"Content-Type": "application/json"}

# Synthetic catalog IDs sampled from the live catalog
# (fetched once at startup in programmatic mode)
_SAMPLE_PRODUCT_IDS: list[str] = []
_SAMPLE_TAXON_IDS: list[str] = []


def _synthetic_account_id() -> str:
    """Generate a 24-char hex MongoDB-style account ID."""
    return uuid.uuid4().hex[:24]


def _random_product() -> str:
    if _SAMPLE_PRODUCT_IDS:
        return random.choice(_SAMPLE_PRODUCT_IDS)
    return uuid.uuid4().hex[:24]


def _random_taxon() -> str:
    if _SAMPLE_TAXON_IDS:
        return random.choice(_SAMPLE_TAXON_IDS)
    return uuid.uuid4().hex[:24]


# ── Payload builders ───────────────────────────────────────────────────────────


def _build_taxon_click(account_id: str, session_id: str) -> dict:
    return {
        "events": [
            {
                "EVENTNAME": "taxon_click",
                "EVENTVALUE": json.dumps(
                    {
                        "taxon": {
                            "label": random.choice(
                                [
                                    "laptop",
                                    "mobile-phone",
                                    "gaming-console",
                                    "tv",
                                    "tablet",
                                    "headphones-earphones",
                                    "smart-home",
                                    "camera",
                                ]
                            )
                        }
                    }
                ),
                "ACCOUNTID": account_id,
                "SESSIONID": session_id,
                "USERAGENT": random.choice(
                    [
                        "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1",
                        "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 Mobile Safari/537.36",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0.0.0",
                        "miniprogram/1.0 toki/3.2.1",
                    ]
                ),
            }
        ]
    }


def _build_view_product(account_id: str, session_id: str, product_id: str) -> dict:
    return {
        "events": [
            {
                "account_id": account_id,
                "session_id": session_id,
                "activity_name": "view_product",
                "activity_data": {
                    "accountid": account_id,
                    "productid": product_id,
                    "action": "view",
                },
            }
        ]
    }


def _build_cart_add(account_id: str, session_id: str, product_id: str) -> dict:
    return {
        "events": [
            {
                "account_id": account_id,
                "session_id": session_id,
                "activity_name": "cart-events",
                "activity_data": {
                    "accountid": account_id,
                    "productid": product_id,
                    "action": "add",
                    "quantity": 1,
                },
            }
        ]
    }


def _build_order(account_id: str, session_id: str, product_id: str) -> dict:
    return {
        "events": [
            {
                "account_id": account_id,
                "session_id": session_id,
                "activity_name": "order-events",
                "activity_data": {
                    "accountid": account_id,
                    "productid": product_id,
                    "action": "complete",
                    "quantity": 1,
                },
            }
        ]
    }


# ── Locust user behaviour ──────────────────────────────────────────────────────


class MarketplaceUser(FastHttpUser):
    """
    Simulates a typical marketplace session:
      60% browse (taxon click → taxon recs → product recs)
      30% shopping (view → cart add → basket recs)
      10% purchase (order complete)

    Each virtual user gets its own account_id and session_id.
    Wait time: 0.1–0.5 s between tasks (realistic for JS-heavy SPA).
    """

    host = BASE_URL
    wait_time = between(0.1, 0.5)

    def on_start(self):
        self.account_id = _synthetic_account_id()
        self.session_id = uuid.uuid4().hex
        self.basket: list[str] = []
        self.headers = {"Content-Type": "application/json"}

    # ── Heavy: taxon browse + recommendations (weight 6) ──────────────────────

    @task(6)
    def browse_taxon(self):
        """Send taxon_click event → fetch taxon page recommendations."""
        taxon_id = _random_taxon()

        # Fire event (updates session state)
        self.client.post(
            "/api/v1/consumer-events",
            json=_build_taxon_click(self.account_id, self.session_id),
            headers=self.headers,
            name="/api/v1/consumer-events [taxon_click]",
        )

        # Fetch taxon page recommendations
        self.client.post(
            "/api/v1/recommendations/taxon",
            json={
                "account_id": self.account_id,
                "taxon_id": taxon_id,
                "top_n": 20,
            },
            headers=self.headers,
            name="/api/v1/recommendations/taxon",
        )

    # ── Medium: product view + PDP recommendations (weight 5) ─────────────────

    @task(5)
    def view_product(self):
        """Send view_product event → fetch product page recommendations."""
        product_id = _random_product()

        self.client.post(
            "/api/v1/events",
            json=_build_view_product(self.account_id, self.session_id, product_id),
            headers=self.headers,
            name="/api/v1/events [view_product]",
        )

        resp = self.client.post(
            "/api/v1/recommendations/product",
            json={
                "account_id": self.account_id,
                "product_id": product_id,
                "top_n": 10,
            },
            headers=self.headers,
            name="/api/v1/recommendations/product",
        )
        # Stash a recommended product for possible cart add
        if resp.status_code == 200:
            recs = resp.json().get("recommendations", [])
            if recs:
                self.basket.append(recs[0])

    # ── Medium: basket / cart page (weight 3) ─────────────────────────────────

    @task(3)
    def view_basket(self):
        """Add to cart → fetch basket recommendations."""
        product_id = _random_product()
        self.basket.append(product_id)

        self.client.post(
            "/api/v1/events",
            json=_build_cart_add(self.account_id, self.session_id, product_id),
            headers=self.headers,
            name="/api/v1/events [cart-events add]",
        )

        self.client.post(
            "/api/v1/recommendations/basket",
            json={
                "account_id": self.account_id,
                "basket_product_ids": self.basket[-5:],  # last 5 basket items
                "top_n": 10,
            },
            headers=self.headers,
            name="/api/v1/recommendations/basket",
        )

    # ── Light: multi-taxon feed (weight 2) ────────────────────────────────────

    @task(2)
    def get_feed(self):
        """Fetch multi-taxon homepage feed."""
        self.client.post(
            "/api/v1/feed",
            json={
                "account_id": self.account_id,
                "top_taxons": 3,
                "top_n_per_taxon": 10,
            },
            headers=self.headers,
            name="/api/v1/feed",
        )

    # ── Rare: order (weight 1) ─────────────────────────────────────────────────

    @task(1)
    def place_order(self):
        """Complete an order — highest intent signal."""
        product_id = _random_product()
        self.client.post(
            "/api/v1/events",
            json=_build_order(self.account_id, self.session_id, product_id),
            headers=self.headers,
            name="/api/v1/events [order-events complete]",
        )
        if product_id in self.basket:
            self.basket.remove(product_id)

    # ── Very rare: health check (weight 1) ────────────────────────────────────

    @task(1)
    def check_health(self):
        self.client.get("/api/v1/health", name="/api/v1/health")


# ── Programmatic run mode (no Locust UI) ──────────────────────────────────────


def _fetch_sample_ids(base_url: str, api_key: str) -> None:
    """Pre-load real product/taxon IDs from the live catalog before the test."""
    import urllib.request

    # Catalog health check
    try:
        with urllib.request.urlopen(
            urllib.request.Request(f"{base_url}/api/v1/catalog/status"), timeout=5
        ) as resp:
            data = json.loads(resp.read())
        print(
            f"Catalog: {data['catalog_size']} products, {data['taxons_with_products']} taxons"
        )
    except Exception as exc:
        print(f"[WARN] Could not fetch catalog stats: {exc}")

    # Harvest real product + taxon IDs via feed endpoint using known seeder accounts
    seeder_accounts = [
        "66fbc5824e022311128232ae",
        "5ff870ee4f636263bd482270",
        "6a5e47214aeec353171ccaa0",
        "64b7b484fa4f99d010979ea0",
        "5ff870ee4f636263bd482270",
    ]
    for acct in seeder_accounts:
        body = json.dumps(
            {"account_id": acct, "top_taxons": 10, "top_n_per_taxon": 30}
        ).encode()
        req = urllib.request.Request(
            f"{base_url}/api/v1/feed",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                feed = json.loads(resp.read())
            for tf in feed.get("taxon_feeds", []):
                if tf["taxon_id"] not in _SAMPLE_TAXON_IDS:
                    _SAMPLE_TAXON_IDS.append(tf["taxon_id"])
                for pid in tf.get("recommendations", []):
                    if pid not in _SAMPLE_PRODUCT_IDS:
                        _SAMPLE_PRODUCT_IDS.append(pid)
        except Exception as exc:
            print(f"[WARN] Seed feed failed for {acct}: {exc}")

    print(
        f"Seeded: {len(_SAMPLE_PRODUCT_IDS)} product IDs, {len(_SAMPLE_TAXON_IDS)} taxon IDs"
    )


def run_programmatic(users: int, spawn_rate: int, duration_seconds: int) -> None:
    """Run a headless load test and print a summary report."""
    import gevent
    from locust.env import Environment
    from locust.log import setup_logging
    from locust.stats import stats_history, stats_printer

    setup_logging("WARNING", None)
    _fetch_sample_ids(BASE_URL, API_KEY)

    env = Environment(user_classes=[MarketplaceUser], host=BASE_URL)
    env.create_local_runner()

    gevent.spawn(stats_printer(env.stats))
    gevent.spawn(stats_history, env.runner)

    env.runner.start(users, spawn_rate=spawn_rate)

    print(f"\n{'─'*60}")
    print(f"  TOKI Recommendation Engine — Load Test")
    print(f"  Target: {BASE_URL}")
    print(f"  Users: {users} (spawn rate: {spawn_rate}/s)")
    print(f"  Duration: {duration_seconds}s")
    print(f"  API key tier: {os.getenv('STRESS_API_KEY_TIER', 'internal')}")
    print(f"{'─'*60}")

    t0 = time.time()
    while time.time() - t0 < duration_seconds:
        time.sleep(2)
        stats = env.runner.stats
        total = stats.total
        if total.num_requests > 0:
            rps = total.current_rps
            p95 = total.get_response_time_percentile(0.95)
            fail_rate = (total.num_failures / total.num_requests) * 100
            print(
                f"  t={int(time.time()-t0):3d}s | "
                f"RPS={rps:6.0f} | "
                f"users={env.runner.user_count:4d} | "
                f"p95={p95 or 0:5.0f}ms | "
                f"fail={fail_rate:.1f}%"
            )

    env.runner.quit()
    _print_summary(env.stats)


def _print_summary(stats) -> None:
    print(f"\n{'═'*60}")
    print("  RESULTS SUMMARY")
    print(f"{'═'*60}")

    for name, entry in sorted(
        stats.entries.items(), key=lambda x: x[1].total_rps, reverse=True
    ):
        if entry.num_requests == 0:
            continue
        endpoint_label = name[0] if isinstance(name, tuple) else str(name)
        p50 = entry.get_response_time_percentile(0.50) or 0
        p95 = entry.get_response_time_percentile(0.95) or 0
        p99 = entry.get_response_time_percentile(0.99) or 0
        fail = entry.num_failures
        print(
            f"  {endpoint_label:50s} "
            f"n={entry.num_requests:6d} "
            f"p50={p50:4.0f}ms  p95={p95:5.0f}ms  p99={p99:5.0f}ms  "
            f"fail={fail}"
        )

    total = stats.total
    print(f"\n{'─'*60}")
    print(f"  Total requests  : {total.num_requests:,}")
    print(f"  Total failures  : {total.num_failures:,}")
    print(
        f"  Failure rate    : {(total.num_failures/max(total.num_requests,1))*100:.2f}%"
    )
    print(f"  Avg RPS         : {total.total_rps:.1f}")
    print(f"  p50 latency     : {total.get_response_time_percentile(0.50) or 0:.0f} ms")
    print(f"  p95 latency     : {total.get_response_time_percentile(0.95) or 0:.0f} ms")
    print(f"  p99 latency     : {total.get_response_time_percentile(0.99) or 0:.0f} ms")
    print(f"{'═'*60}\n")

    # Exit non-zero if failure rate > 1%
    fail_rate = total.num_failures / max(total.num_requests, 1)
    if fail_rate > 0.01:
        print(f"[FAIL] Failure rate {fail_rate:.1%} > 1% threshold")
        sys.exit(1)
    else:
        print("[PASS] Failure rate within acceptable threshold")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TOKI rec engine load test")
    parser.add_argument(
        "--smoke", action="store_true", help="Quick smoke: 50 users, 30s"
    )
    parser.add_argument(
        "--peak", action="store_true", help="Peak load: 1000 users, 120s"
    )
    parser.add_argument("--users", type=int, default=100)
    parser.add_argument("--spawn-rate", type=int, default=50)
    parser.add_argument("--duration", type=int, default=60, help="Seconds")
    args = parser.parse_args()

    if args.smoke:
        run_programmatic(users=50, spawn_rate=10, duration_seconds=30)
    elif args.peak:
        run_programmatic(users=1000, spawn_rate=100, duration_seconds=120)
    else:
        run_programmatic(
            users=args.users, spawn_rate=args.spawn_rate, duration_seconds=args.duration
        )
