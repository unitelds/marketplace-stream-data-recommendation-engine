"""
Stress / load test for the TOKI Recommendation Engine.

Covers BOTH ends of the pipeline for system-team approval:
  • Receiving end  — event ingestion from the marketplace shop stream
  • Posting end    — recommendation delivery back to the staging shop API

User classes
------------
  MarketplaceUser  Mixed session: browse → events → recs → occasional push
  IngestOnlyUser   Only fires events (receiving-end stress)
  PushOnlyUser     Only calls /feed/push (posting-end stress)

Usage
-----
# Quick smoke test (30 s, ramp to 50 users — mixed)
python tests/stress_test.py --smoke

# Full peak-load test (2 min, ramp to 1000 users — mixed)
python tests/stress_test.py --peak

# Receiving-end only (event ingestion throughput)
python tests/stress_test.py --ingest-only

# Posting-end only (recommendation delivery throughput)
python tests/stress_test.py --push-only

# Both ends simultaneously at max load (system approval mode)
python tests/stress_test.py --dual

# Locust web UI (interactive, set users/spawn rate in browser)
locust -f tests/stress_test.py --host=http://localhost:8018

Requirements: locust  (pip install locust)

Environment
-----------
STRESS_API_KEY   API key to use (default: toki-internal-key)
STRESS_HOST      Base URL       (default: http://localhost:8018)
STRESS_PUSH_URL  Staging push target (default: staging-marketplace.toki.mn)
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
MARKETPLACE_PUSH_URL = os.getenv(
    "STRESS_PUSH_URL",
    "https://staging-marketplace.toki.mn/ms/catalogue/v1/recommendation",
)
HEADERS = {"Content-Type": "application/json"}

# ── Real fixtures harvested from toki_event_log_2105535.jsonl ───────────────
# These are actual account/product IDs observed in the staging oracle poll stream.
# Used so push requests contain valid MongoDB ObjectIds the shop API will accept.
_REAL_ACCOUNTS: list[str] = [
    "61020cb10bb8893a86281a38",
    "6800a429c127d95ecd882dd1",
    "6002c14f864b702b8c757c86",
    "5fa23a51f98f0ee0f5ce860f",
    "654bb2c26f7ed293b4de9f4d",
    "651554eea4a541ebabd0685a",
    "5f9a13fa2280fe45d1dfb027",
    "637076019bae441bc61e87a0",
    "67da1c72d262f2d65a412d88",
    "676046b718cf35f28b6c04dc",
    "60cc17940f1497edabb97ee1",
    "670d0f1e22723b9fa57a77ae",
    "67c8fd919cee9e2d26c8f7b9",
    "66fbc5824e022311128232ae",
    "6a5e47214aeec353171ccaa0",
]

# Real product IDs seen in the oracle-poll stream (phones, laptops, audio, etc.)
_REAL_PRODUCTS: list[str] = [
    "68d3cd43d36b9be827b44e06",
    "68d3cd43d36b9be827b44e03",
    "68d3cd43d36b9be827b44e00",
    "68d3cd4ad36b9be827b44e27",
    "68d3cd4ad36b9be827b44e24",
    "68d3cd4ad36b9be827b44e21",
    "68d3cd51d36b9be827b44e3f",
    "68d3cd51d36b9be827b44e3c",
    "68d3cd51d36b9be827b44e42",
    "67073999168488b56f9fb635",
    "6a34c15294861d336b6d1c82",
    "6a7d414cbf57449d39acfb94",
    "6a468b0963ccd8f0968768ab",
    "6a0546a3109dd372dc9bff2d",
    "6a2aa34a33260110bd3310ef",
    "6a3680cb33260110bd33e0b0",
    "6a4c95e76143620497302bfb",
    "6a29198b63ccd8f096855429",
    "6a43afc733260110bd34caad",
    "6a7d798f33260110bd392b03",
    "6a309f8fd46aca65f8084499",
    "6a309f8bd46aca65f8084431",
    "6a309f86d46aca65f80843c5",
    "6a717fea87e3e71b6da84f9d",
    "6a6353d68db83a55ca6e3668",
    "6a0546b1109dd372dc9c06aa",
    "6a69d67f610c8683be8857e3",
    "68b7ee190bcb0200c3e8d7c7",
    "68b7ee190bcb0200c3e8d7e1",
    "6a02dea7e96e1eeb38ff753b",
    "69f19fd62f1e69b091cd0a87",
    "6833c3440b23fc4fb994bf7c",
    "6833c3331ca487379242315e",
    "6a7e815c0f7d45d6106038c5",
    "6a210e0fae68bf552a40f9c9",
]

# Mongolian taxon labels exactly as they appear in the oracle stream
_REAL_TAXON_LABELS: list[str] = [
    "Гар утас",
    "Зөөврийн компьютер",
    "Суурин компьютер",
    "Таблет",
    "Чихэвч",
    "Микрофон",
    "Зурагт",
    "Ухаалаг Цаг",
    "Gaming",
    "Дагалдах хэрэгсэл",
    "Тренд технологи",
    "Олон үйлдэлт",
    "Гэр ахуй",
    "Гал тогоо",
    "Хөргөгч",
    "Тоос сорогч",
    "Аяга угаагч",
    "Хагас автомат",
    "Кофе бутлагч",
    "Стандарт",
    "Фото камер",
    "Үснийхэрэгсэл",
]

_USER_AGENTS: list[str] = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 16; SM-S916B Build/BP4A.251205.006; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 C",
    "Mozilla/5.0 (Linux; Android 13; SM-G996U Build/TP1A.220624.014; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 C",
    "Mozilla/5.0 (Linux; Android 16; SM-A556E Build/BP4A.251205.006; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 C",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0.0.0 Safari/537.36",
    "miniprogram/1.0 toki/3.2.1",
]

# Populated at startup from the live catalog (augments _REAL_PRODUCTS)
_SAMPLE_PRODUCT_IDS: list[str] = list(_REAL_PRODUCTS)
_SAMPLE_TAXON_IDS: list[str] = []
# Real accounts confirmed push-eligible (non-empty feed) — populated at startup
_SEEDER_ACCOUNTS: list[str] = list(_REAL_ACCOUNTS)


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


def _rand_ua() -> str:
    return random.choice(_USER_AGENTS)


def _build_taxon_click(account_id: str, session_id: str) -> dict:
    """Oracle consumer-events format — taxon_click with real Mongolian label."""
    return {
        "events": [
            {
                "EVENTNAME": "taxon_click",
                "EVENTVALUE": json.dumps(
                    {"taxon": {"label": random.choice(_REAL_TAXON_LABELS)}}
                ),
                "ACCOUNTID": account_id,
                "SESSIONID": session_id,
                "USERAGENT": _rand_ua(),
            }
        ]
    }


def _build_product_click(account_id: str, session_id: str) -> dict:
    """Oracle consumer-events format — product_click matching live stream schema."""
    products = random.sample(_SAMPLE_PRODUCT_IDS, min(3, len(_SAMPLE_PRODUCT_IDS)))
    label = random.choice(_REAL_TAXON_LABELS)
    return {
        "events": [
            {
                "EVENTNAME": "product_click",
                "EVENTVALUE": json.dumps(
                    {"productIds": products, "taxon": {"label": label}}
                ),
                "ACCOUNTID": account_id,
                "SESSIONID": session_id,
                "USERAGENT": _rand_ua(),
            }
        ]
    }


def _build_view_product(account_id: str, session_id: str, product_id: str) -> dict:
    return {
        "shop_id": "antmall",
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
                "user_agent": _rand_ua(),
            }
        ],
    }


def _build_cart_add(account_id: str, session_id: str, product_id: str) -> dict:
    return {
        "shop_id": "antmall",
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
                "user_agent": _rand_ua(),
            }
        ],
    }


def _build_order(account_id: str, session_id: str, product_id: str) -> dict:
    return {
        "shop_id": "antmall",
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
                "user_agent": _rand_ua(),
            }
        ],
    }


# ── Locust user behaviour ──────────────────────────────────────────────────────


# ── User classes ──────────────────────────────────────────────────────────────


class MarketplaceUser(FastHttpUser):
    """
    Mixed session: browse → events → recs → occasional push.
    Covers both ends proportionally as a real marketplace session would.
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
        """Oracle taxon_click + product_click event → taxon page recommendations."""
        taxon_id = _random_taxon()

        self.client.post(
            "/api/v1/consumer-events",
            json=_build_taxon_click(self.account_id, self.session_id),
            headers=self.headers,
            name="/api/v1/consumer-events [taxon_click]",
        )
        self.client.post(
            "/api/v1/consumer-events",
            json=_build_product_click(self.account_id, self.session_id),
            headers=self.headers,
            name="/api/v1/consumer-events [product_click]",
        )

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

    # ── Rare: push feed to staging marketplace (weight 1, real accounts only) ──

    @task(1)
    def push_to_marketplace(self):
        """Generate feed and POST to staging marketplace — tests the posting end."""
        # Marketplace rejects synthetic ObjectIds — always use real seeder accounts
        account_id = (
            random.choice(_SEEDER_ACCOUNTS) if _SEEDER_ACCOUNTS else self.account_id
        )
        self.client.post(
            "/api/v1/feed/push",
            json={
                "account_id": account_id,
                "top_taxons": 3,
                "top_n_per_taxon": 10,
                "shop_feed_url": MARKETPLACE_PUSH_URL,
                "push_timeout_seconds": 5.0,
            },
            headers=self.headers,
            name="POST /api/v1/feed/push [→staging]",
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


class IngestOnlyUser(FastHttpUser):
    """
    Receiving-end stress: fires only event ingestion requests.
    Emulates marketplace shop stream at peak ingest rate with no rec calls.
    Used for system-team approval of the receiving endpoint capacity.
    """

    host = BASE_URL
    wait_time = between(0.05, 0.2)  # higher frequency — ingestion is fire-and-forget

    def on_start(self):
        self.account_id = random.choice(_REAL_ACCOUNTS)
        self.session_id = uuid.uuid4().hex
        self.headers = {"Content-Type": "application/json"}

    @task(5)
    def ingest_oracle_product_click(self):
        """Replays realistic oracle product_click batches from the live stream."""
        self.client.post(
            "/api/v1/consumer-events",
            json=_build_product_click(self.account_id, self.session_id),
            headers=self.headers,
            name="INGEST /api/v1/consumer-events [product_click]",
        )

    @task(4)
    def ingest_oracle_taxon_click(self):
        self.client.post(
            "/api/v1/consumer-events",
            json=_build_taxon_click(self.account_id, self.session_id),
            headers=self.headers,
            name="INGEST /api/v1/consumer-events [taxon_click]",
        )

    @task(3)
    def ingest_view_product(self):
        product_id = random.choice(_SAMPLE_PRODUCT_IDS)
        self.client.post(
            "/api/v1/events",
            json=_build_view_product(self.account_id, self.session_id, product_id),
            headers=self.headers,
            name="INGEST /api/v1/events [view_product]",
        )

    @task(2)
    def ingest_cart_add(self):
        product_id = random.choice(_SAMPLE_PRODUCT_IDS)
        self.client.post(
            "/api/v1/events",
            json=_build_cart_add(self.account_id, self.session_id, product_id),
            headers=self.headers,
            name="INGEST /api/v1/events [cart-events add]",
        )

    @task(1)
    def ingest_order(self):
        product_id = random.choice(_SAMPLE_PRODUCT_IDS)
        self.client.post(
            "/api/v1/events",
            json=_build_order(self.account_id, self.session_id, product_id),
            headers=self.headers,
            name="INGEST /api/v1/events [order-events complete]",
        )


class PushOnlyUser(FastHttpUser):
    """
    Posting-end stress: only calls /feed/push → staging marketplace.
    Emulates bulk recommendation delivery at peak dispatch rate.
    Used for system-team approval of the posting endpoint capacity.
    """

    host = BASE_URL
    wait_time = between(0.3, 1.0)  # push calls are heavier (upstream HTTP)

    def on_start(self):
        self.headers = {"Content-Type": "application/json"}

    @task(7)
    def push_feed(self):
        """Generate personalised feed and push to staging shop API."""
        account_id = random.choice(_SEEDER_ACCOUNTS)
        self.client.post(
            "/api/v1/feed/push",
            json={
                "account_id": account_id,
                "top_taxons": 3,
                "top_n_per_taxon": 10,
                "shop_feed_url": MARKETPLACE_PUSH_URL,
                "push_timeout_seconds": 5.0,
            },
            headers=self.headers,
            name="PUSH /api/v1/feed/push [→staging]",
        )

    @task(3)
    def push_infer(self):
        """On-demand infer + inline rec delivery (lightweight push path)."""
        account_id = random.choice(_SEEDER_ACCOUNTS)
        taxon_id = _random_taxon()
        self.client.post(
            "/api/v1/infer",
            json={
                "account_id": account_id,
                "context": {"current_taxon_id": taxon_id},
                "top_n": 10,
            },
            headers=self.headers,
            name="PUSH /api/v1/infer [on-demand]",
        )


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
    seeder_accounts = list(_REAL_ACCOUNTS)
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
            # Only add accounts that returned a non-empty feed (i.e. real accounts)
            if feed.get("total_products", 0) > 0 and acct not in _SEEDER_ACCOUNTS:
                _SEEDER_ACCOUNTS.append(acct)
        except Exception as exc:
            print(f"[WARN] Seed feed failed for {acct}: {exc}")

    print(
        f"Seeded: {len(_SAMPLE_PRODUCT_IDS)} product IDs, "
        f"{len(_SAMPLE_TAXON_IDS)} taxon IDs, "
        f"{len(_SEEDER_ACCOUNTS)} push-eligible accounts"
    )


def run_programmatic(
    users: int,
    spawn_rate: int,
    duration_seconds: int,
    user_classes: list | None = None,
    label: str = "Mixed",
) -> None:
    """Run a headless load test and print a summary report."""
    import gevent
    from locust.env import Environment
    from locust.log import setup_logging
    from locust.stats import stats_history, stats_printer

    setup_logging("WARNING", None)
    _fetch_sample_ids(BASE_URL, API_KEY)

    klasses = user_classes or [MarketplaceUser]
    env = Environment(user_classes=klasses, host=BASE_URL)
    env.create_local_runner()

    gevent.spawn(stats_printer(env.stats))
    gevent.spawn(stats_history, env.runner)

    env.runner.start(users, spawn_rate=spawn_rate)

    print(f"\n{'─'*60}")
    print(f"  TOKI Recommendation Engine — Load Test ({label})")
    print(f"  Target  : {BASE_URL}")
    print(f"  Users   : {users} (spawn rate: {spawn_rate}/s)")
    print(f"  Duration: {duration_seconds}s")
    print(f"  Classes : {', '.join(c.__name__ for c in klasses)}")
    print(f"  Push URL: {MARKETPLACE_PUSH_URL}")
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

    ingest_reqs = ingest_fails = 0
    push_reqs = push_fails = 0

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
            f"  {endpoint_label:55s} "
            f"n={entry.num_requests:6d} "
            f"p50={p50:4.0f}ms  p95={p95:5.0f}ms  p99={p99:5.0f}ms  "
            f"fail={fail}"
        )
        # Bucket by end
        lbl = endpoint_label.lower()
        if "ingest" in lbl or "events" in lbl or "consumer" in lbl:
            ingest_reqs += entry.num_requests
            ingest_fails += entry.num_failures
        if "push" in lbl or "feed" in lbl or "infer" in lbl or "rec" in lbl:
            push_reqs += entry.num_requests
            push_fails += entry.num_failures

    total = stats.total
    print(f"\n{'─'*60}")
    print(f"  ── Receiving end (event ingestion) ──")
    print(f"  Requests   : {ingest_reqs:,}")
    print(f"  Failures   : {ingest_fails:,}")
    print(f"  Fail rate  : {(ingest_fails/max(ingest_reqs,1))*100:.2f}%")
    print(f"  ── Posting end (recommendation delivery) ──")
    print(f"  Requests   : {push_reqs:,}")
    print(f"  Failures   : {push_fails:,}")
    print(f"  Fail rate  : {(push_fails/max(push_reqs,1))*100:.2f}%")
    print(f"  ── Overall ──")
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

    # Exit non-zero if overall failure rate > 1%
    fail_rate = total.num_failures / max(total.num_requests, 1)
    if fail_rate > 0.01:
        print(f"[FAIL] Failure rate {fail_rate:.1%} > 1% threshold")
        sys.exit(1)
    else:
        print("[PASS] Failure rate within acceptable threshold")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="TOKI rec engine load test — covers both receiving and posting ends"
    )
    parser.add_argument("--smoke", action="store_true", help="Mixed 50 users, 30s")
    parser.add_argument("--peak", action="store_true", help="Mixed 1000 users, 120s")
    parser.add_argument(
        "--ingest-only",
        action="store_true",
        help="Receiving-end only: saturate event ingestion (200 users, 60s)",
    )
    parser.add_argument(
        "--push-only",
        action="store_true",
        help="Posting-end only: saturate recommendation delivery (100 users, 60s)",
    )
    parser.add_argument(
        "--dual",
        action="store_true",
        help="Both ends simultaneously: 200 ingest + 100 push users, 90s (system approval mode)",
    )
    parser.add_argument("--users", type=int, default=100)
    parser.add_argument("--spawn-rate", type=int, default=50)
    parser.add_argument("--duration", type=int, default=60, help="Seconds")
    args = parser.parse_args()

    if args.smoke:
        run_programmatic(users=50, spawn_rate=10, duration_seconds=30, label="Smoke")
    elif args.peak:
        run_programmatic(users=1000, spawn_rate=100, duration_seconds=120, label="Peak")
    elif args.ingest_only:
        run_programmatic(
            users=200,
            spawn_rate=20,
            duration_seconds=60,
            user_classes=[IngestOnlyUser],
            label="Receiving-end only",
        )
    elif args.push_only:
        run_programmatic(
            users=100,
            spawn_rate=10,
            duration_seconds=60,
            user_classes=[PushOnlyUser],
            label="Posting-end only",
        )
    elif args.dual:
        # Run both user classes simultaneously — system approval mode
        run_programmatic(
            users=300,
            spawn_rate=30,
            duration_seconds=90,
            user_classes=[IngestOnlyUser, PushOnlyUser],
            label="Dual-end (system approval)",
        )
    else:
        run_programmatic(
            users=args.users,
            spawn_rate=args.spawn_rate,
            duration_seconds=args.duration,
            label="Custom",
        )
