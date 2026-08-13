# TOKI Recommendation Engine — Integration & Testing Guide

> **Base URL:** `http://10.22.4.13:8018`
> **Swagger UI:** `http://10.22.4.13:8018/docs`
> **ReDoc:** `http://10.22.4.13:8018/redoc`
> **Version:** 4.2.0

---

## Table of Contents

1. [Network Access Setup](#1-network-access-setup)
2. [Authentication](#2-authentication)
3. [Event Stream Ingestion](#3-event-stream-ingestion)
4. [Recommendation Endpoints](#4-recommendation-endpoints)
5. [Health & Diagnostics](#5-health--diagnostics)
6. [Full cURL Test Suite](#6-full-curl-test-suite)
7. [Maintaining Public Access After Restart](#7-maintaining-public-access-after-restart)

---

## 1. Network Access Setup

### Problem

The application runs inside a Docker dev container. The gunicorn workers bind to `0.0.0.0:8018` **inside the container**, but that container has no published port on the host machine, so `http://10.22.4.13:8018/` is unreachable from external clients.

`localhost:8018` works only during a VS Code Remote SSH session because VS Code tunnels `forwardPorts` through the SSH connection — it is **not** a real open port on the server.

### Solution applied

A lightweight socat proxy container is started on the **host Docker daemon** to bridge the gap:

```bash
DOCKER_HOST=unix:///var/run/docker-host.sock \
docker run -d \
  --name toki-rec-proxy \
  --restart unless-stopped \
  -p 8018:8018 \
  alpine/socat:latest \
  TCP-LISTEN:8018,fork,reuseaddr TCP:172.17.0.3:8018
```

This binds port 8018 on the host's `0.0.0.0` and forwards every connection to the devcontainer's bridge IP (`172.17.0.3:8018`).

**Verify it works:**
```bash
curl http://10.22.4.13:8018/api/v1/health
```
Expected: `{"status": "ok", ...}`

---

## 2. Authentication

Every `/api/v1/*` request (except `/api/v1/health`) requires an API key.

### Sending the key

**Option A — HTTP Header (recommended):**
```
X-API-Key: <your-key>
```

**Option B — Query parameter:**
```
?api_key=<your-key>
```

### Key tiers

| Key | Tier | Rate limit |
|---|---|---|
| `toki-internal-key` | internal | 500 req/s |
| `toki-standard-key` | standard | 100 req/s |
| `toki-readonly-key` | readonly | 20 req/s |

Keys are configured via the `TOKI_API_KEYS` environment variable on the running server:
```
TOKI_API_KEYS="toki-internal-key:internal,toki-standard-key:standard,toki-readonly-key:readonly"
```

**Use `toki-internal-key`** for backend services and batch pipelines.
**Use `toki-standard-key`** for shop frontend clients.
**Use `toki-readonly-key`** for analytics/monitoring.

---

## 3. Event Stream Ingestion

There are **two** event ingestion endpoints depending on the source system.

---

### 3.1 `POST /api/v1/events` — customer_activities stream

Use this for events from the Oracle `customer_activities` table (shop stream format).

**URL:** `http://10.22.4.13:8018/api/v1/events`
**Method:** POST
**Auth required:** Yes
**Batch size:** 1–500 events per request

#### Request schema

```json
{
  "shop_id": "antmall",
  "events": [
    {
      "event_id": "optional-idempotency-key",
      "account_id": "<24-hex MongoDB ObjectID>",
      "session_id": "sess_abc123",
      "activity_name": "<see table below>",
      "activity_data": { ... },
      "user_agent": "Mozilla/5.0 ...",
      "timestamp": "2026-08-12T09:00:00Z"
    }
  ]
}
```

#### `activity_name` values and intent weights

| `activity_name` | Intent weight | Notes |
|---|---|---|
| `order-events` | 4.0 – 5.0 | Sub-action `complete`=5.0, `placed`=4.5 |
| `limit-events` | 4.0 | User checked lease/credit limit |
| `cart-events` | −1.5 to 3.5 | `add`=3.5, `remove`=−1.5 |
| `wishlist-events` | 3.0 | `add`=3.0, `remove`=−0.5 |
| `view_product` | 1.0 | Product detail page view |
| `product_click` | 1.5 | Explicit card click |
| `taxon_click` | 0.5 | Category browse |

#### `activity_data` field format

Accepted as:
- a JSON **object**
- a JSON **string** (will be parsed)
- an Oracle Python-literal string (e.g. `{'productid': '...'}`—will be parsed)

Common fields inside `activity_data`:

| Field | Description |
|---|---|
| `productid` | Product MongoDB ObjectID |
| `accountid` | User MongoDB ObjectID |
| `action` | `add`, `remove`, `complete`, `placed`, `check`, `view` |
| `quantity` | Integer, used for cart events |
| `taxon_id` | Category ObjectID |

#### cURL example — single view_product event

```bash
curl -X POST http://10.22.4.13:8018/api/v1/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: toki-internal-key" \
  -d '{
    "shop_id": "antmall",
    "events": [{
      "event_id": "evt-001",
      "account_id": "6a5e47214aeec353171ccaa0",
      "session_id": "sess_abc123",
      "activity_name": "view_product",
      "activity_data": {
        "accountid": "6a5e47214aeec353171ccaa0",
        "productid": "6989774f3516dac1b3e979ee",
        "action": "view"
      },
      "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X)",
      "timestamp": "2026-08-12T09:00:00Z"
    }]
  }'
```

#### cURL example — mixed batch (cart add + limit check)

```bash
curl -X POST http://10.22.4.13:8018/api/v1/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: toki-internal-key" \
  -d '{
    "shop_id": "antmall",
    "events": [
      {
        "account_id": "6a5e47214aeec353171ccaa0",
        "session_id": "sess_abc123",
        "activity_name": "cart-events",
        "activity_data": {
          "accountid": "6a5e47214aeec353171ccaa0",
          "productid": "6989774f3516dac1b3e979ee",
          "action": "add",
          "quantity": 2
        },
        "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X)"
      },
      {
        "account_id": "66fbc5824e022311128232ae",
        "session_id": "sess_xyz456",
        "activity_name": "limit-events",
        "activity_data": {
          "accountid": "66fbc5824e022311128232ae",
          "action": "check"
        },
        "user_agent": "Mozilla/5.0 (Linux; Android 14)"
      }
    ]
  }'
```

#### Response schema

```json
{
  "status": "accepted",
  "processed": 2,
  "failed": 0,
  "recommendations": [
    {
      "id": "6a5e47214aeec353171ccaa0",
      "taxon_id": "69fbef9bda75a61ceadc7607",
      "recommendations": ["pid1", "pid2", "pid3"],
      "strategy": "hybrid",
      "intent_score": 3.5,
      "device": "mobile",
      "count": 12,
      "served_at": "2026-08-12T09:47:01.694717"
    }
  ]
}
```

> The response returns **inline recommendations** for each affected user immediately. Events for multiple users may be mixed in one batch.

---

### 3.2 `POST /api/v1/consumer-events` — Oracle consumer_events rows

Use this for rows from the Oracle `consumer_events` table directly. Field names match Oracle column names; uppercase aliases are accepted.

**URL:** `http://10.22.4.13:8018/api/v1/consumer-events`
**Method:** POST
**Auth required:** Yes
**Batch size:** 1–500 events per request

#### Request schema

```json
{
  "shop_id": "antmall",
  "events": [
    {
      "EVENTNAME": "product_click",
      "EVENTVALUE": "{'productIds': ['...'], 'taxon': {'label': 'Үснийхэрэгсэл'}}",
      "ACCOUNTID": "66fbc5824e022311128232ae",
      "SESSIONID": "jPAaTyDWFjD1JsHyR0ux3hewNYRvNvRy",
      "TIMESTAMP_": "2026-08-12T08:21:23.894Z",
      "USERAGENT": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X)"
    }
  ]
}
```

#### Supported event names

| `EVENTNAME` | What it does |
|---|---|
| `product_click` | Extracts `productIds[]` array and Mongolian taxon label from `EVENTVALUE` |
| `taxon_click` | Extracts Mongolian taxon label from `EVENTVALUE` → resolves to `taxon_id` |

**Taxon label resolution:** Both Mongolian display names (`Үснийхэрэгсэл`) and slugs (`hair-care`) resolve to `taxon_id` automatically. Unresolved labels are accepted and the session is still updated.

#### cURL example

```bash
curl -X POST http://10.22.4.13:8018/api/v1/consumer-events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: toki-internal-key" \
  -d '{
    "events": [
      {
        "EVENTNAME": "product_click",
        "EVENTVALUE": "{\"productIds\": [\"69fc469bab34c8d11412ec79\"], \"taxon\": {\"label\": \"Үснийхэрэгсэл\"}}",
        "ACCOUNTID": "66fbc5824e022311128232ae",
        "SESSIONID": "jPAaTyDWFjD1JsHyR0ux3hewNYRvNvRy",
        "TIMESTAMP_": "2026-08-12T09:21:23.894Z",
        "USERAGENT": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X)"
      },
      {
        "EVENTNAME": "taxon_click",
        "EVENTVALUE": "{\"taxon\": {\"label\": \"Гар утас\"}}",
        "ACCOUNTID": "5ff870ee4f636263bd482270",
        "SESSIONID": "2xI3rxpGJbBOeY4vnY_1EBSkTuAL8Pf9",
        "TIMESTAMP_": "2026-08-12T09:22:19.413Z",
        "USERAGENT": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_6 like Mac OS X)"
      }
    ]
  }'
```

---

## 4. Recommendation Endpoints

### 4.1 `POST /api/v1/recommendations/taxon` — Category page grid

Personalized product list for a taxon/category page. Blends CBF + CF + popularity via RRF, then filters to the requested taxon.

**URL:** `http://10.22.4.13:8018/api/v1/recommendations/taxon`

#### Request

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "taxon_id": "69fbef9bda75a61ceadc7607",
  "top_n": 20,
  "exclude_product_ids": [],
  "require_in_stock": true
}
```

#### cURL example

```bash
curl -X POST http://10.22.4.13:8018/api/v1/recommendations/taxon \
  -H "Content-Type: application/json" \
  -H "X-API-Key: toki-standard-key" \
  -d '{
    "account_id": "6a5e47214aeec353171ccaa0",
    "taxon_id": "69fbef9bda75a61ceadc7607",
    "top_n": 20
  }'
```

---

### 4.2 `POST /api/v1/recommendations/product` — Product detail page (PDP)

"You may also like" / similar products panel. CBF is the primary signal; CF adds social proof.

**URL:** `http://10.22.4.13:8018/api/v1/recommendations/product`

#### Request

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "product_id": "6989774f3516dac1b3e979ee",
  "top_n": 12,
  "exclude_product_ids": ["6989774f3516dac1b3e979ee"]
}
```

#### cURL example

```bash
curl -X POST http://10.22.4.13:8018/api/v1/recommendations/product \
  -H "Content-Type: application/json" \
  -H "X-API-Key: toki-standard-key" \
  -d '{
    "account_id": "6a5e47214aeec353171ccaa0",
    "product_id": "6989774f3516dac1b3e979ee",
    "top_n": 12
  }'
```

---

### 4.3 `POST /api/v1/recommendations/basket` — Basket/cart cross-sell

Cross-sell recommendations for the cart page. Products already in the cart are automatically excluded.

**URL:** `http://10.22.4.13:8018/api/v1/recommendations/basket`

#### Request

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "basket_product_ids": ["6989774f3516dac1b3e979ee", "698977503516dac1b3e97a6c"],
  "top_n": 8
}
```

#### cURL example

```bash
curl -X POST http://10.22.4.13:8018/api/v1/recommendations/basket \
  -H "Content-Type: application/json" \
  -H "X-API-Key: toki-standard-key" \
  -d '{
    "account_id": "6a5e47214aeec353171ccaa0",
    "basket_product_ids": ["6989774f3516dac1b3e979ee"],
    "top_n": 8
  }'
```

---

### 4.4 `POST /api/v1/infer` — On-demand single-taxon inference

Trigger an inference for a user with explicit context (taxon, cart, device). Does not require a prior event to be posted.

**URL:** `http://10.22.4.13:8018/api/v1/infer`

#### Request

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "session_id": "sess_abc123",
  "context": {
    "current_taxon_id": "69fbef9bda75a61ceadc7607",
    "cart_product_ids": [],
    "limit_checked": false,
    "device_type": "mobile"
  },
  "top_n": 10,
  "exclude_product_ids": []
}
```

#### cURL example

```bash
curl -X POST http://10.22.4.13:8018/api/v1/infer \
  -H "Content-Type: application/json" \
  -H "X-API-Key: toki-standard-key" \
  -d '{
    "account_id": "6a5e47214aeec353171ccaa0",
    "context": {
      "current_taxon_id": "69fbef9bda75a61ceadc7607",
      "device_type": "mobile"
    },
    "top_n": 10
  }'
```

---

### 4.5 `POST /api/v1/feed` — Multi-taxon personalized feed

Returns recommendations grouped by a user's top taxons (categories with highest intent scores).

**URL:** `http://10.22.4.13:8018/api/v1/feed`

#### Request

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "top_taxons": 3,
  "top_n_per_taxon": 10,
  "extra_taxon_ids": [],
  "exclude_product_ids": []
}
```

| Field | Default | Description |
|---|---|---|
| `top_taxons` | 3 | Number of top taxons to serve (max 10) |
| `top_n_per_taxon` | 10 | Products per taxon (max 30) |
| `extra_taxon_ids` | `[]` | Force-include these taxons even if not in history |

#### cURL example

```bash
curl -X POST http://10.22.4.13:8018/api/v1/feed \
  -H "Content-Type: application/json" \
  -H "X-API-Key: toki-standard-key" \
  -d '{
    "account_id": "6a5e47214aeec353171ccaa0",
    "top_taxons": 3,
    "top_n_per_taxon": 10
  }'
```

#### Response

```json
{
  "id": "6a5e47214aeec353171ccaa0",
  "taxon_feeds": [
    {
      "taxon_id": "69fbef9bda75a61ceadc7607",
      "taxon_name": "video-game-racing-wheel",
      "recommendations": ["pid1", "pid2", "..."],
      "count": 10,
      "score": 3.5
    }
  ],
  "total_products": 10,
  "strategy": "multi_taxon_hybrid",
  "intent_score": 3.5,
  "device": "mobile",
  "served_at": "2026-08-12T09:47:22.191273"
}
```

---

### 4.6 `POST /api/v1/feed/push` — Generate feed AND push to shop API

Generates a multi-taxon feed and POSTs it to the shop's feed endpoint in one call.

**URL:** `http://10.22.4.13:8018/api/v1/feed/push`

#### Request

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "top_taxons": 3,
  "top_n_per_taxon": 10,
  "shop_feed_url": "https://api.your-shop.com/feed/6a5e47214aeec353171ccaa0",
  "push_timeout_seconds": 3.0
}
```

If `shop_feed_url` is omitted, the engine uses the `MARKETPLACE_API_BASE_URL/{account_id}` configured on the server.

---

## 5. Health & Diagnostics

These endpoints do **not** require an API key.

### `GET /api/v1/health` — Liveness check

```bash
curl http://10.22.4.13:8018/api/v1/health
```

```json
{
  "status": "ok",
  "app": "TOKI Marketplace Recommendation System v2",
  "version": "4.2.0",
  "environment": "production",
  "catalog_ready": true,
  "timestamp": "2026-08-12T09:46:14.222841+00:00"
}
```

### `GET /api/v1/catalog/status` — Feature store diagnostics

```bash
curl http://10.22.4.13:8018/api/v1/catalog/status \
  -H "X-API-Key: toki-readonly-key"
```

Returns catalog product count, TF-IDF matrix dimensions, user session count, and last sync timestamp.

### `GET /api/v1/catalog/sync` — Manual catalog re-sync

```bash
curl http://10.22.4.13:8018/api/v1/catalog/sync \
  -H "X-API-Key: toki-internal-key"
```

Triggers an immediate catalog re-sync from PostgreSQL in the background. The API continues serving while the sync runs.

---

## 6. Full cURL Test Suite

Copy and paste this block to verify all endpoints end-to-end:

```bash
#!/usr/bin/env bash
set -e

BASE="http://10.22.4.13:8018"
KEY="toki-internal-key"
USER1="6a5e47214aeec353171ccaa0"
USER2="66fbc5824e022311128232ae"
PRODUCT1="6989774f3516dac1b3e979ee"
TAXON1="69fbef9bda75a61ceadc7607"

echo "────────────────────────────────────"
echo " 1. Health check (no auth required)"
echo "────────────────────────────────────"
curl -s "$BASE/api/v1/health" | python3 -m json.tool

echo ""
echo "────────────────────────────────────"
echo " 2. Event ingestion — view_product"
echo "────────────────────────────────────"
curl -s -X POST "$BASE/api/v1/events" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"shop_id\": \"antmall\",
    \"events\": [{
      \"account_id\": \"$USER1\",
      \"session_id\": \"sess_test_001\",
      \"activity_name\": \"view_product\",
      \"activity_data\": {
        \"accountid\": \"$USER1\",
        \"productid\": \"$PRODUCT1\",
        \"action\": \"view\"
      },
      \"user_agent\": \"Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X)\",
      \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
    }]
  }" | python3 -m json.tool

echo ""
echo "────────────────────────────────────"
echo " 3. Event ingestion — cart-events add"
echo "────────────────────────────────────"
curl -s -X POST "$BASE/api/v1/events" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"events\": [{
      \"account_id\": \"$USER1\",
      \"activity_name\": \"cart-events\",
      \"activity_data\": {
        \"accountid\": \"$USER1\",
        \"productid\": \"$PRODUCT1\",
        \"action\": \"add\",
        \"quantity\": 1
      }
    }]
  }" | python3 -m json.tool

echo ""
echo "────────────────────────────────────"
echo " 4. Oracle consumer-events"
echo "────────────────────────────────────"
curl -s -X POST "$BASE/api/v1/consumer-events" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"events\": [{
      \"EVENTNAME\": \"product_click\",
      \"EVENTVALUE\": \"{\\\"productIds\\\": [\\\"69fc469bab34c8d11412ec79\\\"], \\\"taxon\\\": {\\\"label\\\": \\\"Үснийхэрэгсэл\\\"}}\",
      \"ACCOUNTID\": \"$USER2\",
      \"SESSIONID\": \"sess_oracle_001\",
      \"TIMESTAMP_\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
      \"USERAGENT\": \"Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X)\"
    }]
  }" | python3 -m json.tool

echo ""
echo "────────────────────────────────────"
echo " 5. Taxon page recommendations"
echo "────────────────────────────────────"
curl -s -X POST "$BASE/api/v1/recommendations/taxon" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"account_id\": \"$USER1\",
    \"taxon_id\": \"$TAXON1\",
    \"top_n\": 10
  }" | python3 -m json.tool

echo ""
echo "────────────────────────────────────"
echo " 6. Product page recommendations"
echo "────────────────────────────────────"
curl -s -X POST "$BASE/api/v1/recommendations/product" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"account_id\": \"$USER1\",
    \"product_id\": \"$PRODUCT1\",
    \"top_n\": 8
  }" | python3 -m json.tool

echo ""
echo "────────────────────────────────────"
echo " 7. Basket cross-sell recommendations"
echo "────────────────────────────────────"
curl -s -X POST "$BASE/api/v1/recommendations/basket" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"account_id\": \"$USER1\",
    \"basket_product_ids\": [\"$PRODUCT1\"],
    \"top_n\": 6
  }" | python3 -m json.tool

echo ""
echo "────────────────────────────────────"
echo " 8. Multi-taxon feed"
echo "────────────────────────────────────"
curl -s -X POST "$BASE/api/v1/feed" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"account_id\": \"$USER1\",
    \"top_taxons\": 3,
    \"top_n_per_taxon\": 8
  }" | python3 -m json.tool

echo ""
echo "────────────────────────────────────"
echo " 9. On-demand infer"
echo "────────────────────────────────────"
curl -s -X POST "$BASE/api/v1/infer" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"account_id\": \"$USER1\",
    \"context\": {
      \"current_taxon_id\": \"$TAXON1\",
      \"device_type\": \"mobile\"
    },
    \"top_n\": 10
  }" | python3 -m json.tool

echo ""
echo "All tests complete."
```

Save the block above as `tests/api_test.sh`, make it executable (`chmod +x tests/api_test.sh`), and run it from the server.

---

## 7. Maintaining Public Access After Restart

The socat proxy container has `--restart unless-stopped`, so it will auto-start if the host Docker daemon restarts (e.g. after a server reboot).

However, if the **devcontainer** itself is recreated (which assigns a new bridge IP), the proxy target IP must be updated. Find the new IP and re-run the proxy:

```bash
# Step 1 — find devcontainer's new bridge IP
docker inspect <devcontainer-id> \
  --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'

# Step 2 — remove old proxy
DOCKER_HOST=unix:///var/run/docker-host.sock docker rm -f toki-rec-proxy

# Step 3 — recreate proxy with new IP
DOCKER_HOST=unix:///var/run/docker-host.sock \
docker run -d \
  --name toki-rec-proxy \
  --restart unless-stopped \
  -p 8018:8018 \
  alpine/socat:latest \
  TCP-LISTEN:8018,fork,reuseaddr TCP:<NEW_IP>:8018
```

### Permanent fix: rebuild devcontainer with published ports

The cleanest long-term fix is to rebuild the devcontainer so Docker actually publishes port 8018 on the host (the `-p 8018:8018` in `devcontainer.json` → `runArgs` already configures this). Do this when you next have a maintenance window:

1. In VS Code: `Ctrl+Shift+P` → **Dev Containers: Rebuild Container**
2. After rebuild, verify: `DOCKER_HOST=unix:///var/run/docker-host.sock docker ps` — the container should show `0.0.0.0:8018->8018/tcp` in the PORTS column.
3. The socat proxy will no longer be needed.

---

## Reference: Strategy Values

| Strategy string | Meaning |
|---|---|
| `hybrid` | CBF + CF + session context |
| `cbf+cf+pop` | All three pipelines merged via RRF |
| `cbf+pop` | Content-based + popularity |
| `cf+pop` | Collaborative + popularity |
| `popular` | Popularity fallback only (new/cold-start user) |
| `multi_taxon_hybrid` | Per-taxon hybrid, grouped by top intent taxons |

## Reference: Device Result Counts

Device type is auto-detected from `User-Agent`:

| Device | Default `top_n` |
|---|---|
| `mobile` | 12 |
| `desktop` | 20 |
| `miniprogram` | 8 |

## Reference: Intent Score Thresholds

| Range | Interpretation |
|---|---|
| 0 – 2.0 | Low intent — browsing |
| 2.0 – 5.0 | Medium intent — engaged |
| 5.0 – 8.0 | High intent — shopping |
| > 8.0 | Very high intent — purchase-ready |
