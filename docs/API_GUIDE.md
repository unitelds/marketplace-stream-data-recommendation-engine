# TOKI Recommendation Engine — API Integration Guide

> **Version:** 4.2.0 | **Base URL:** `http://<host>:8018` | **Swagger UI:** `/docs`

---

## Table of Contents

1. [Overview](#1-overview)
2. [Quick Start](#2-quick-start)
3. [Event Ingestion Endpoints](#3-event-ingestion-endpoints)
   - [POST /api/v1/events](#post-apiv1events)
   - [POST /api/v1/consumer-events](#post-apiv1consumer-events)
4. [UI Placement Recommendation Endpoints](#4-ui-placement-recommendation-endpoints)
   - [POST /api/v1/recommendations/taxon](#post-apiv1recommendationstaxon)
   - [POST /api/v1/recommendations/product](#post-apiv1recommendationsproduct)
   - [POST /api/v1/recommendations/basket](#post-apiv1recommendationsbasket)
5. [Feed & Inference Endpoints](#5-feed--inference-endpoints)
   - [POST /api/v1/infer](#post-apiv1infer)
   - [POST /api/v1/feed](#post-apiv1feed)
   - [POST /api/v1/feed/push](#post-apiv1feedpush)
6. [Health & Catalog Endpoints](#6-health--catalog-endpoints)
7. [Recommendation Strategy Reference](#7-recommendation-strategy-reference)
8. [Intent Score Reference](#8-intent-score-reference)
9. [Integration Patterns by UI Area](#9-integration-patterns-by-ui-area)
10. [Error Handling](#10-error-handling)
11. [Performance Characteristics](#11-performance-characteristics)

---

## 1. Overview

The TOKI Recommendation Engine ingests user engagement events in real-time and produces personalized product recommendations through three co-operating retrieval pipelines:

| Pipeline | Module | What it does |
|---|---|---|
| **Content-Based Filtering (CBF)** | `content_based.py` | TF-IDF cosine similarity over 30,000-feature product text vectors |
| **Item-Based CF** | `collaborative.py` | Co-interaction scoring: users who touched product A also touched B |
| **Popularity** | `feature_store.py` | Interaction-weighted product counts, taxon-scoped |

All three are merged via **Reciprocal Rank Fusion (RRF)** with placement-tuned weights, then passed through five context re-rankers (session taxon boost, basket cross-sell, limit-check price filter, intent adjustment, shop diversity cap).

### Key concepts

- **`account_id`** — MongoDB ObjectID (24 hex characters) identifying the user
- **`taxon_id`** — MongoDB ObjectID identifying a product category
- **`intent_score`** — cumulative session-level buying intent (sum of weighted events); above 8.0 = high-intent path
- **`strategy`** — how recommendations were generated (see §7)
- **`device`** — detected from User-Agent; controls result count (mobile=12, desktop=20, miniprogram=8)

---

## 2. Quick Start

### Step 1 — Send an engagement event

```bash
curl -X POST http://localhost:8018/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "events": [{
      "account_id": "6a5e47214aeec353171ccaa0",
      "activity_name": "view_product",
      "activity_data": {
        "accountid": "6a5e47214aeec353171ccaa0",
        "productid": "6989774f3516dac1b3e979ee",
        "action": "view"
      },
      "user_agent": "Mozilla/5.0 (iPhone; ...)"
    }]
  }'
```

### Step 2 — Get taxon page recommendations

```bash
curl -X POST http://localhost:8018/api/v1/recommendations/taxon \
  -H "Content-Type: application/json" \
  -d '{
    "account_id": "6a5e47214aeec353171ccaa0",
    "taxon_id": "698c3ed3e783dbd39ed224f0",
    "top_n": 20
  }'
```

### Step 3 — Check system status

```bash
curl http://localhost:8018/api/v1/health
curl http://localhost:8018/api/v1/catalog/status
```

---

## 3. Event Ingestion Endpoints

### `POST /api/v1/events`

**Purpose:** Ingest events from the `customer_activities` Oracle table (shop stream format).
**When to call:** On every meaningful user action — view, cart add/remove, order, limit check.
**Returns:** Inline single-taxon recommendations for all affected users.

#### Request

```json
{
  "shop_id": "antmall",
  "events": [
    {
      "event_id": "optional-idempotency-key",
      "account_id": "6a5e47214aeec353171ccaa0",
      "session_id": "sess_abc123",
      "activity_name": "cart-events",
      "activity_data": {
        "accountid": "6a5e47214aeec353171ccaa0",
        "productid": "6989774f3516dac1b3e979ee",
        "action": "add",
        "quantity": 1
      },
      "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 ...)",
      "timestamp": "2026-07-31T08:00:00Z"
    }
  ]
}
```

**`activity_name` values:**

| Value | Intent weight | Notes |
|---|---|---|
| `order-events` | 4.0–5.0 | Sub-action `complete`=5.0, `placed`=4.5 |
| `limit-events` | 4.0 | User checked lease/credit limit |
| `cart-events` | −1.5 to 3.5 | `add`=3.5, `remove`=−1.5 |
| `wishlist-events` | 3.0 | `add`=3.0, `remove`=−0.5 |
| `view_product` | 1.0 | Product detail page view |
| `product_click` | 1.5 | Explicit card click |
| `taxon_click` | 0.5 | Category browse |

**`activity_data`** may be a JSON object, a JSON string, or an Oracle Python-literal string — all three formats are parsed automatically.

#### Response

```json
{
  "status": "accepted",
  "processed": 1,
  "failed": 0,
  "recommendations": [
    {
      "id": "6a5e47214aeec353171ccaa0",
      "taxon_id": "69853d6e5d7fc1ee35bab068",
      "recommendations": ["pid1", "pid2", "pid3"],
      "strategy": "hybrid",
      "intent_score": 3.5,
      "device": "mobile",
      "count": 12,
      "served_at": "2026-07-31T08:00:01.123Z"
    }
  ]
}
```

#### Batch support

Up to **500 events** per request. Events for multiple users may be mixed in one batch — the engine groups by `account_id` and generates per-user recommendations.

---

### `POST /api/v1/consumer-events`

**Purpose:** Ingest rows directly from Oracle `consumer_events` table.
**Handles:** `product_click` (extracts `productIds[]` array and Mongolian taxon label) and `taxon_click`.
**Field names match Oracle column names** — uppercase aliases are accepted.

#### Request

```json
{
  "events": [
    {
      "EVENTNAME": "product_click",
      "EVENTVALUE": "{'productIds': ['69fc469bab34c8d11412ec79'], 'taxon': {'label': 'Үснийхэрэгсэл'}}",
      "ACCOUNTID": "66fbc5824e022311128232ae",
      "SESSIONID": "jPAaTyDWFjD1JsHyR0ux3hewNYRvNvRy",
      "TIMESTAMP_": "2026-07-31T08:21:23.894Z",
      "USERAGENT": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 ...)"
    },
    {
      "EVENTNAME": "taxon_click",
      "EVENTVALUE": "{'taxon': {'label': 'Гар утас'}}",
      "ACCOUNTID": "5ff870ee4f636263bd482270",
      "SESSIONID": "2xI3rxpGJbBOeY4vnY_1EBSkTuAL8Pf9",
      "TIMESTAMP_": "2026-07-31T08:22:19.413Z",
      "USERAGENT": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_6 ...)"
    }
  ]
}
```

**Taxon label resolution:** Mongolian display names (`Гар утас`) and slugs (`mobile-phone`) are both resolved to `taxon_id` at query time using the label map built from the catalog at sync time. Unresolved labels are accepted without a `taxon_id`; the session is still updated.

Response schema is identical to `/events`.

---

## 4. UI Placement Recommendation Endpoints

These three endpoints map directly to the three recommendation panels on the shop frontend. Each uses a distinct blend of CBF + CF + popularity pipelines tuned for its specific placement context.

---

### `POST /api/v1/recommendations/taxon`

**Placement:** Taxon / category page product grid
**When to call:** When a user navigates to or scrolls a category listing page
**Pipeline weights:** CBF 45% + CF 30% + Popularity 25%

```
┌─────────────────────────────────────────────────────┐
│  Category: Laptop & Gaming                          │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐    │
│  │ rec1 │ │ rec2 │ │ rec3 │ │ rec4 │ │ rec5 │ …  │
│  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘    │
└─────────────────────────────────────────────────────┘
```

#### Request

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "taxon_id": "698c3ed3e783dbd39ed224f0",
  "top_n": 20,
  "exclude_product_ids": ["pid_already_shown_1", "pid_already_shown_2"],
  "require_in_stock": true
}
```

| Field | Required | Default | Description |
|---|---|---|---|
| `account_id` | ✓ | — | User account ID |
| `taxon_id` | ✓ | — | The category being browsed |
| `top_n` | — | `20` | Max products (1–60) |
| `exclude_product_ids` | — | `[]` | Products already visible on page |
| `require_in_stock` | — | `true` | Filter out OOS products |

#### Response

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "placement": "taxon_page",
  "recommendations": ["pid1", "pid2", "pid3"],
  "strategy": "cbf+cf+pop",
  "intent_score": 3.5,
  "device": "mobile",
  "count": 20,
  "context_taxon_id": "698c3ed3e783dbd39ed224f0",
  "context_product_id": null,
  "served_at": "2026-07-31T08:00:01.123Z"
}
```

#### How it works

1. **CBF seeds** — user's top-scored products from interaction history → TF-IDF similarity → filter to requested taxon
2. **CF candidates** — co-interaction scores for user → filter to requested taxon
3. **Taxon popularity** — products in taxon sorted by cumulative interaction score
4. **3-way RRF merge** → session taxon boost → basket-aware rerank → device truncation

**Cold-start:** if user has no history, falls back to taxon-scoped popularity (`strategy: "popular"`).

---

### `POST /api/v1/recommendations/product`

**Placement:** Product detail page (PDP) — "You may also like" panel below the product
**When to call:** When a user opens a product page
**Pipeline weights:** CBF 60% + CF 40%

```
┌─────────────────────────────────────────────────────┐
│  [Product: ASUS ROG Laptop  ₮4,100,000]             │
│  Description / Specs / Add to Cart                  │
│                                                     │
│  ── You may also like ──────────────────────────── │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐    │
│  │ rec1 │ │ rec2 │ │ rec3 │ │ rec4 │ │ rec5 │ …  │
│  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘    │
└─────────────────────────────────────────────────────┘
```

#### Request

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "product_id": "6989774f3516dac1b3e979ee",
  "top_n": 10,
  "exclude_product_ids": [],
  "require_in_stock": true
}
```

| Field | Required | Default | Description |
|---|---|---|---|
| `account_id` | ✓ | — | User account ID (used for session context re-ranking) |
| `product_id` | ✓ | — | The product currently being viewed |
| `top_n` | — | `10` | Max similar products (1–30) |
| `exclude_product_ids` | — | `[]` | Products to exclude from results |
| `require_in_stock` | — | `true` | Filter out OOS products |

#### Response

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "placement": "product_page",
  "recommendations": ["pid1", "pid2", "pid3"],
  "strategy": "cbf+cf_similarity",
  "intent_score": 1.5,
  "device": "mobile",
  "count": 10,
  "context_taxon_id": "698c3ed3e783dbd39ed224f0",
  "context_product_id": "6989774f3516dac1b3e979ee",
  "served_at": "2026-07-31T08:00:01.123Z"
}
```

#### How it works

1. **CBF** — TF-IDF cosine similarity using the anchor product's content vector as the query
2. **CF item similarity** — find all users who interacted with `product_id`; aggregate what else they interacted with (co-purchase / co-view)
3. **2-way RRF merge** → session boost → basket cross-sell → device truncation
4. **Fallback** — if CBF and CF both empty, returns taxon-scoped popularity for the anchor product's taxon

`context_taxon_id` in the response is the anchor product's taxon (useful for breadcrumb display).

---

### `POST /api/v1/recommendations/basket`

**Placement:** Basket / cart page — "Complete your purchase" cross-sell panel
**When to call:** When a user opens or updates their cart
**Pipeline weights:** CBF 40% + CF 35% + Popularity 25%

```
┌─────────────────────────────────────────────────────┐
│  Your Cart                                          │
│  ┌─────────────────────────────────────────────┐   │
│  │ ASUS ROG Laptop          ×1  ₮4,100,000     │   │
│  │ Gaming Mouse             ×1    ₮120,000      │   │
│  └─────────────────────────────────────────────┘   │
│                                                     │
│  ── Complete your purchase ────────────────────── │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐    │
│  │ rec1 │ │ rec2 │ │ rec3 │ │ rec4 │ │ rec5 │ …  │
│  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘    │
└─────────────────────────────────────────────────────┘
```

#### Request

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "basket_product_ids": [
    "6989774f3516dac1b3e979ee",
    "69fc469bab34c8d11412ec79"
  ],
  "top_n": 10,
  "exclude_product_ids": [],
  "require_in_stock": true
}
```

| Field | Required | Default | Description |
|---|---|---|---|
| `account_id` | ✓ | — | User account ID |
| `basket_product_ids` | ✓ | — | All product IDs currently in the basket (1+ required) |
| `top_n` | — | `10` | Max cross-sell products (1–30) |
| `exclude_product_ids` | — | `[]` | Additional exclusions beyond basket contents |
| `require_in_stock` | — | `true` | Filter out OOS products |

#### Response

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "placement": "basket_page",
  "recommendations": ["pid1", "pid2", "pid3"],
  "strategy": "basket_cbf+cf+pop",
  "intent_score": 8.5,
  "device": "mobile",
  "count": 10,
  "context_taxon_id": null,
  "context_product_id": null,
  "served_at": "2026-07-31T08:00:01.123Z"
}
```

#### How it works

1. **CBF** — TF-IDF similarity using all basket items as a blended seed vector
2. **CF aggregate** — for each basket item (up to 5), run item CF co-interaction; sum scores
3. **Global popularity** — fallback coverage for sparse CF/CBF
4. **3-way RRF merge**
5. **Basket-aware reranker** (applied with stronger settings than other placements):
   - Same `product_category` as any basket item → ×0.6 penalty (avoid duplicates)
   - Different taxon from all basket items → ×1.15 boost (cross-sell)
6. **Limit-check filter** — if user checked their financial limit this session, products priced above `mid` tier are demoted
7. **Tight diversity cap** — max 3 products per shop (vs 5 elsewhere)

---

## 5. Feed & Inference Endpoints

### `POST /api/v1/infer`

**Purpose:** On-demand single-taxon inference with optional context override.
**When to use:** When you need recommendations for a specific taxon with full context control.

#### Request

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "session_id": "sess_abc123",
  "context": {
    "current_taxon_id": "698c3ed3e783dbd39ed224f0",
    "cart_product_ids": ["6989774f3516dac1b3e979ee"],
    "limit_checked": false,
    "device_type": "mobile"
  },
  "top_n": 10,
  "exclude_product_ids": []
}
```

The `context` object seeds the ranker with explicit basket and device state, overriding in-memory session state. Useful for stateless clients.

#### Response

```json
{
  "id": "6a5e47214aeec353171ccaa0",
  "taxon_id": "698c3ed3e783dbd39ed224f0",
  "recommendations": ["pid1", "pid2", "pid3"],
  "strategy": "hybrid",
  "intent_score": 3.5,
  "device": "mobile",
  "count": 10,
  "served_at": "2026-07-31T08:00:01.123Z"
}
```

---

### `POST /api/v1/feed`

**Purpose:** Multi-taxon recommendation feed — homepage or personalised section.
**Returns:** Products grouped by the user's top taxons, cross-deduplicated.
**Oracle history:** On first request for a user with no in-memory history, Oracle `consumer_events` is queried in background (up to 3s timeout) to bootstrap the interaction profile.

#### Request

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "top_taxons": 3,
  "top_n_per_taxon": 10,
  "extra_taxon_ids": ["698c3ed3e783dbd39ed224f0"],
  "exclude_product_ids": []
}
```

| Field | Default | Description |
|---|---|---|
| `top_taxons` | `3` | Number of taxon sections (1–10) |
| `top_n_per_taxon` | `10` | Products per section (1–30) |
| `extra_taxon_ids` | `[]` | Force-include additional taxons (e.g. promoted categories) |
| `exclude_product_ids` | `[]` | Products already shown elsewhere on the page |

#### Response

```json
{
  "id": "6a5e47214aeec353171ccaa0",
  "taxon_feeds": [
    {
      "taxon_id": "698c3ed3e783dbd39ed224f0",
      "taxon_name": "computer-laptop-gaming",
      "recommendations": ["pid1", "pid2", "pid3"],
      "count": 10,
      "score": 8.5
    },
    {
      "taxon_id": "69853d6e5d7fc1ee35bab068",
      "taxon_name": "computer-gaming-gear-accessory",
      "recommendations": ["pid4", "pid5", "pid6"],
      "count": 10,
      "score": 5.0
    }
  ],
  "total_products": 20,
  "strategy": "multi_taxon_hybrid",
  "intent_score": 8.5,
  "device": "mobile",
  "served_at": "2026-07-31T08:00:01.123Z"
}
```

**Taxon ordering:** session taxon path (most recent first) → interaction-weighted historical taxons → `extra_taxon_ids`. Products are cross-deduplicated: each `product_id` appears in at most one taxon section.

---

### `POST /api/v1/feed/push`

**Purpose:** Generate multi-taxon feed AND synchronously POST it to the shop's configured feed endpoint.
**Same request schema as `/feed`** with two additional optional fields:

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "top_taxons": 3,
  "top_n_per_taxon": 10,
  "shop_feed_url": "https://your-shop.example.com/api/recommendations",
  "push_timeout_seconds": 3.0
}
```

**Push payload sent to shop:**

```json
{
  "id": "6a5e47214aeec353171ccaa0",
  "taxon_feeds": [
    { "taxon_id": "...", "taxon_name": "...", "recommendations": ["pid", ...] }
  ]
}
```

**Response adds push status fields:**

```json
{
  "...feed fields...",
  "push_status": "ok",
  "push_url": "https://your-shop.example.com/api/recommendations",
  "push_error": null
}
```

`push_status`: `"ok"` | `"failed"` | `"not_attempted"`. Recommendations are always returned regardless of push outcome.

---

## 6. Health & Catalog Endpoints

### `GET /api/v1/health`

Liveness check — always returns `200`.

```json
{
  "status": "ok",
  "app": "TOKI Marketplace Recommendation System v2",
  "version": "4.2.0",
  "environment": "production",
  "catalog_ready": true,
  "timestamp": "2026-07-31T08:00:01.123Z"
}
```

### `GET /api/v1/catalog/status`

Full diagnostic snapshot of the feature store and catalog state.

```json
{
  "catalog_ready": true,
  "catalog_size": 4511,
  "catalog_synced_at": "2026-07-31T09:23:26.745Z",
  "catalog_age_minutes": 9.6,
  "tfidf_shape": [4511, 30000],
  "taxon_label_map_size": 165,
  "taxon_name_map_size": 79,
  "taxons_with_products": 77,
  "active_sessions": 3,
  "tracked_users": 2,
  "popularity_entries": 3
}
```

`catalog_ready: false` means the initial sync has not completed yet — recommendation endpoints will return empty results until it becomes `true` (typically within 3–5 seconds of startup).

### `POST /api/v1/catalog/sync`

Trigger a forced catalog re-sync from PostgreSQL (runs automatically every 10 min).

```json
{
  "status": "sync_triggered",
  "message": "Catalog re-sync started in background. Check /catalog/status for progress.",
  "timestamp": "2026-07-31T08:00:01.123Z"
}
```

---

## 7. Recommendation Strategy Reference

The `strategy` field in every response indicates how the recommendations were generated.

| Strategy value | Meaning |
|---|---|
| `hybrid` | CBF seeds + RRF merge with popularity (general endpoints) |
| `cbf+cf+pop` | Three-way RRF: CBF + item CF + popularity (taxon page) |
| `cbf+pop` | CBF + popularity (CF had no data) |
| `cf+pop` | CF + popularity (no CBF seeds) |
| `cbf+cf_similarity` | CBF cosine + item CF co-interaction (product page) |
| `cbf_similarity` | CBF cosine only, CF had no data (product page fallback) |
| `basket_cbf+cf+pop` | Three-way basket-tuned RRF (basket page) |
| `basket_cbf+pop` | Basket CBF + popularity (basket page, CF sparse) |
| `multi_taxon_hybrid` | Per-taxon hybrid, cross-deduplicated (feed) |
| `taxon_cbf` | Taxon-scoped CBF, no user history |
| `taxon_popular_fallback` | Taxon popularity only (no CBF/CF data) |
| `popular` | Global or taxon popularity (cold-start) |
| `catalog_not_ready` | Catalog sync in progress — empty result |

---

## 8. Intent Score Reference

The `intent_score` in responses reflects cumulative session buying intent. Each event contributes a time-decayed weight:

$$S = \sum_{e} w_e \cdot 0.5^{\,\Delta t / 7}$$

where $\Delta t$ is days since the event and the half-life is 7 days.

| Score range | Interpretation | Ranker behaviour |
|---|---|---|
| 0.0 – 0.5 | Discovery / browsing | Diversity-broadened results |
| 0.5 – 2.0 | Low intent | Extra taxon diversity injected |
| 2.0 – 8.0 | Normal engagement | Standard hybrid ranking |
| ≥ 8.0 | **High intent** | Conversion-optimised ordering |

**Score accumulation example:**

```
taxon_click       → +0.5  (intent = 0.5)
view_product      → +1.0  (intent = 1.5)
cart-events add   → +3.5  (intent = 5.0)
limit-events      → +4.0  (intent = 9.0) ← crosses HIGH_INTENT_THRESHOLD
order-events      → +5.0  (intent = 14.0)
```

---

## 9. Integration Patterns by UI Area

### Pattern A — Taxon page (category listing)

**Call sequence:**

```
User opens category "Laptops"
  → frontend: POST /api/v1/recommendations/taxon
              { account_id, taxon_id, top_n: 20 }
  → display returned product_ids as the category grid

User scrolls (load more)
  → frontend: POST /api/v1/recommendations/taxon
              { ..., top_n: 20, exclude_product_ids: [already_shown_ids] }
  → append new products to grid
```

**Simultaneously send the taxon_click event:**

```
  → fire-and-forget: POST /api/v1/consumer-events
                     { EVENTNAME: "taxon_click", EVENTVALUE: "{'taxon': {'label': 'Laptop'}}" ... }
```

> You can pipeline these: fire the event and fetch recommendations in parallel. The event is used for the *next* request's personalization, not the current one.

---

### Pattern B — Product detail page (PDP)

**Call sequence:**

```
User opens product "ASUS ROG Laptop" (product_id = X)
  → fire-and-forget: POST /api/v1/events
                     { activity_name: "view_product", activity_data: {productid: X} }

  → simultaneously: POST /api/v1/recommendations/product
                    { account_id, product_id: X, top_n: 10 }
  → display in "You may also like" panel below product details
```

**If user adds to cart (same page):**

```
  → POST /api/v1/events
         { activity_name: "cart-events", activity_data: {productid: X, action: "add"} }
  → use returned inline recommendations to refresh the panel
```

---

### Pattern C — Basket / cart page

**Call sequence:**

```
User opens cart (basket = [pid_A, pid_B, pid_C])
  → POST /api/v1/recommendations/basket
         { account_id, basket_product_ids: [pid_A, pid_B, pid_C], top_n: 10 }
  → display in "Complete your purchase" panel below cart items

User adds another item (pid_D) to cart
  → POST /api/v1/events { activity_name: "cart-events", ... action: "add" }
  → re-call POST /api/v1/recommendations/basket with updated basket_product_ids
  → refresh the cross-sell panel
```

---

### Pattern D — Homepage personalised feed

```
User opens homepage (or personalised section)
  → POST /api/v1/feed
         { account_id, top_taxons: 3, top_n_per_taxon: 10 }
  → render one horizontal scroll section per taxon_feed item

  OR use /api/v1/feed/push if your backend needs the engine to call
  your endpoint proactively (e.g. for push notifications or pre-rendered pages)
```

---

## 10. Error Handling

All endpoints return standard HTTP status codes:

| Code | Meaning |
|---|---|
| `200` | Success (even for partial failures — check `failed` count) |
| `422` | Validation error — request schema mismatch |
| `500` | Internal error — check server logs |

When the catalog is not yet ready (e.g. first few seconds after startup), recommendation endpoints return `200` with empty `recommendations` and `strategy: "catalog_not_ready"`. The client should either retry or display a loading state.

**Graceful degradation:** every retrieval pipeline fails silently and returns an empty list rather than raising an exception. The ranker cascades to the next available pipeline. You will always receive a valid JSON response.

---

## 11. Performance Characteristics

| Operation | Typical latency | Notes |
|---|---|---|
| Event ingestion (single) | < 5 ms | In-memory write only |
| Event ingestion (batch 100) | 10–30 ms | Parallel normalization |
| Taxon page recs | 5–20 ms | After catalog sync |
| Product page recs | 5–15 ms | TF-IDF dot product, sparse matrix |
| Basket page recs | 10–30 ms | Multiple CF lookups |
| Feed (3 taxons) | 15–50 ms | Per-taxon hybrid × 3 |
| Oracle history load (background) | 500ms–3s | First request only per user |
| Catalog sync | 2–4 s | ~4,500 products, TF-IDF rebuild |

**Catalog sync** runs automatically every 10 minutes as a background task and does not block any API requests.

**Device-adaptive result counts:**

| Device | Max results |
|---|---|
| `miniprogram` | 8 |
| `mobile` | 12 |
| `desktop` | 20 |
| `unknown` | 15 |

The device type is auto-detected from the `user_agent` field. You can override it explicitly via the `context.device_type` field in `/infer` requests.
