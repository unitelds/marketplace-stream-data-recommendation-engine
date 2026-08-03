# TOKI Marketplace Stream-Data Recommendation Engine

Real-time recommendation engine for the TOKI marketplace. Ingests engagement events from shop data streams, builds per-user intent profiles, and serves hybrid content-based + collaborative filtering + popularity recommendations across multiple product taxons and three distinct UI placement areas.

**Current version:** `4.2.0` | **Environment:** `production` | **Port:** `8018`

---

## Table of Contents

- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Engagement Signal Taxonomy](#engagement-signal-taxonomy)
- [Recommendation Pipelines](#recommendation-pipelines)
- [UI Placement Areas](#ui-placement-areas)
- [API Reference](#api-reference)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Running the Server](#running-the-server)
- [Docker Deployment](#docker-deployment)
- [Internet Exposure (Tunnel)](#internet-exposure-tunnel)
- [Development Notes](#development-notes)

---

## Architecture

```
Shop Data Stream
      │
      ▼ POST /api/v1/events
      │ POST /api/v1/consumer-events
      │
┌─────▼────────────────────────────────────────────────────────┐
│                    FastAPI Application                        │
│                                                              │
│  Event Normalization Pipeline                                │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ • Parse ACTIVITYDATA (Oracle Python-literal / JSON)    │  │
│  │ • product_click: extract productIds[] + Mongolian taxon│  │
│  │ • taxon_click: resolve Mongolian label → taxon_id      │  │
│  │ • Intent weight assignment + time decay                │  │
│  │ • Device type detection (USERAGENT)                    │  │
│  └────────────────────────────────────────────────────────┘  │
│                          │                                   │
│  In-Memory Feature Store (FeatureStore singleton)            │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ • User sessions (30-min sliding window)                │  │
│  │ • User–item implicit feedback scores                   │  │
│  │ • Session taxon browse path                            │  │
│  │ • Basket state / limit-check flag                      │  │
│  │ • Global popularity index                              │  │
│  └────────────────────────────────────────────────────────┘  │
│                          │                                   │
│  Catalog (PostgreSQL, synced every 10 min)                   │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ 4,511 products │ TF-IDF (4511 × 30,000) │ 77 taxons   │  │
│  │ taxon_id ↔ taxon_name maps (165 label entries)         │  │
│  └────────────────────────────────────────────────────────┘  │
│                          │                                   │
│  Three Retrieval Pipelines                                   │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ 1. Content-Based Filtering (CBF)                       │  │
│  │    TF-IDF cosine similarity over product text corpus   │  │
│  │ 2. Item-Based Collaborative Filtering (CF)             │  │
│  │    Co-interaction scoring on in-memory user-item matrix│  │
│  │ 3. Popularity Fallback                                 │  │
│  │    Interaction-weighted, taxon-scoped or global        │  │
│  └────────────────────────────────────────────────────────┘  │
│                          │                                   │
│  Hybrid Ranker (RRF merge + 5 re-rankers)                    │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ 1. RRF merge (placement-tuned weights)                 │  │
│  │ 2. Session taxon boost (+25%)                          │  │
│  │ 3. Basket-aware cross-sell / penalise duplicates       │  │
│  │ 4. Limit-check price-tier filter                       │  │
│  │ 5. Shop diversity cap (max 5/shop)                     │  │
│  │ 6. Device-adaptive top-N truncation                    │  │
│  └────────────────────────────────────────────────────────┘  │
│                          │                                   │
│         POST /api/v1/feed/push ──► Shop's Feed API           │
└──────────────────────────────────────────────────────────────┘
```

---

## Data Sources

### Oracle DB — `toki.marketplace_consumer_activities`

| Column | Type | Description |
|---|---|---|
| `ID_` | string | Event ID |
| `ACTIVITYNAME` | string | Event type (see signal taxonomy below) |
| `ACTIVITYDATA` | string | JSON / Python-literal payload with `accountid`, `productid`, `action`, etc. |
| `P_DATE` | string | Partition date `YYYYMMDD` |

### Oracle DB — `toki.marketplace_consumer_EVENTS`

| Column | Type | Description |
|---|---|---|
| `ID_` | string | Event ID |
| `EVENTNAME` | string | `product_click` or `taxon_click` |
| `EVENTVALUE` | string | `{'productIds': [...], 'taxon': {'label': 'Mongolian'}}` |
| `ACCOUNTID` | string | User account ID |
| `SESSIONID` | string | Browser / app session ID |
| `USERAGENT` | string | Used for device type detection |
| `TIMESTAMP_` | datetime | Event timestamp |

> **Taxon label normalisation:** `EVENTVALUE.taxon.label` is stored as a Mongolian display name (e.g. `Гар утас`). At catalog sync time, a slug → `taxon_id` map is built from `marketplace_catalog_data_extended_version3` and used to resolve all incoming labels. Unresolved labels are logged and accepted without a taxon (session still updated).

### PostgreSQL — `marketplace_catalog_data_extended_version3`

Rich product profile table. Key fields used by the engine:

| Field | Normalisation applied |
|---|---|
| `product_id` | Primary key |
| `price` | `"4100000 MNT"` → `float` |
| `stock` | string → `int`; OOS products excluded from results |
| `specifications` | JSON string → flattened `key value` text for TF-IDF |
| `keywords` | JSON list / comma-separated string → joined text |
| `details` | Plain text **or** JSON blob — text extracted, image URLs stripped |
| `description` | Mongolian unicode; included as-is (TF-IDF tokenises unicode) |
| `premium_grade` | `"premium"` / `"standard"` → ordinal `0–3` |
| `price_range` | `"budget"` / `"mid"` / `"high-end"` / `"luxury"` → ordinal `0–3` |
| `taxon_id` / `taxon_name` | Used to build label map and taxon → product index |

---

## Engagement Signal Taxonomy

All events are scored with a **buying intent weight** and a **7-day exponential time decay**:

$$S(u, p) = \sum_{e} w_e \cdot 0.5^{\,\Delta t_e / 7}$$

| Event (`ACTIVITYNAME`) | Sub-action | Weight | Signal meaning |
|---|---|---|---|
| `order-events` | `complete` | **5.0** | Purchase confirmed |
| `order-events` | `placed` | 4.5 | Order initiated |
| `limit-events` | any | **4.0** | User checked lease limit — strong purchase intent |
| `cart-events` | `add` | 3.5 | Added to basket |
| `wishlist-events` | `add` | 3.0 | Saved for later |
| `product_click` | any | 1.5 | Explicit product card click |
| `view_product` | any | 1.0 | Product detail page view |
| `taxon_click` | any | 0.5 | Category browse |
| `cart-events` | `remove` | **−1.5** | Basket abandon (negative) |

**Session intent threshold:** `≥ 8.0` = high-intent (conversion-optimised ranking); `≤ 2.0` = low-intent (diversity-broadened ranking).

---

## Recommendation Pipelines

### 1. Content-Based Filtering (CBF) — `src/module/content_based.py`

- **Vectoriser:** `TfidfVectorizer` with unigrams + bigrams, `max_features=30,000`, `sublinear_tf=True`
- **Text corpus per product:** manufacturer (×3) + product names (×2) + category hierarchy + keywords + specs + details + taxon slug + price range
- **Similarity:** L2-normalised rows → cosine = dot product → `scipy` sparse matrix multiply
- **Seed:** user's top-scored products from interaction history
- **`get_similar_products(seed_ids, top_k)`** — CBF from seed product vector
- **`get_taxon_products(taxon_id, top_k)`** — products in taxon ranked by popularity/quality
- **`embed_query_text(text)`** — vectorize free-text for future search integration

### 2. Item-Based Collaborative Filtering (CF) — `src/module/collaborative.py`

Uses the live in-memory user–item interaction matrix that grows with every event.

**Algorithm: co-interaction scoring**

$$\text{CF\_score}(p_{\text{cand}}) = \sum_{p_s \in \text{seeds}} \sum_{u \in \text{co-users}(p_s)} w_s \cdot \frac{c_u}{c_u + 1} \cdot I(u, p_{\text{cand}})$$

- **`get_cf_candidates(account_id, top_k)`** — for a user: find co-interactors of their seed products, aggregate what else those users liked
- **`get_item_similar_products(product_id, top_k)`** — for a product: find users who touched it, aggregate their other interactions (used by the PDP panel)
- **`cf_stats()`** — diagnostic: tracked users, interaction counts, coverage

### 3. Popularity Fallback — `FeatureStore._popularity`

- Global `product_id → cumulative_intent_score` counter (updated on every event)
- Scoped by `taxon_id` when context is available
- Used for cold-start users and as the third leg in 3-way RRF

### Hybrid Merge — Reciprocal Rank Fusion (RRF)

General formula for $k$ ranked lists:
$$\text{score}(p) = \sum_{i} \frac{w_i}{K + \text{rank}_i(p)}, \quad K = 60$$

Placement-tuned weights:

| Placement | CBF | CF | Popularity |
|---|---|---|---|
| General / feed | 0.60 | — | 0.40 |
| Taxon page | 0.45 | 0.30 | 0.25 |
| Product page | 0.60 | 0.40 | — |
| Basket page | 0.40 | 0.35 | 0.25 |

### Re-ranking Layers (applied after merge)

1. **Session taxon boost** — products in the user's last 3 browsed taxons get +25% score
2. **Basket-aware** — cross-category products boosted ×1.15; same `product_category` as basket items penalised ×0.6
3. **Limit-check filter** — if `limit-events` fired this session, products above price tier `mid` are demoted ×0.5
4. **Intent adjustment** — high-intent (≥8.0): conversion order preserved; low-intent (≤2.0): extra taxon diversity injected
5. **Shop diversity cap** — max 5 products per shop per response (max 3 in basket panel)
6. **Device top-N** — miniprogram: 8 / mobile: 12 / desktop: 20

### Cold-Start Strategy

| Situation | Strategy |
|---|---|
| New user, no events | Global popularity by taxon |
| User has `taxon_click` only | Taxon-scoped popularity |
| User has `product_click` | CBF from clicked products |
| User has order/cart history | Full hybrid CBF + CF + popularity |
| CF data sparse | CBF-only or CBF+pop fallback |

---

## UI Placement Areas

The engine serves three distinct recommendation panels on the shop frontend.

### 1. Taxon / Category Page — `POST /api/v1/recommendations/taxon`

```
┌─────────────────────────────────────────────────────┐
│  Category: Laptop & Gaming                          │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐    │
│  │ pid1 │ │ pid2 │ │ pid3 │ │ pid4 │ │ pid5 │ …  │
│  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘    │
└─────────────────────────────────────────────────────┘
```

Called when a user navigates to or scrolls a category page. Returns products filtered to the requested `taxon_id`, personalized by the user's CBF seeds and CF co-interactions within that category.

### 2. Product Detail Page — `POST /api/v1/recommendations/product`

```
┌─────────────────────────────────────────────────────┐
│  [Product Detail: ASUS ROG Laptop]                  │
│  Price / Add to Cart / ...                          │
│                                                     │
│  ── You may also like ──────────────────────────── │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐    │
│  │ pid1 │ │ pid2 │ │ pid3 │ │ pid4 │ │ pid5 │ …  │
│  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘    │
└─────────────────────────────────────────────────────┘
```

Called when a user opens a product page. Returns products similar to the anchor product using TF-IDF cosine similarity (CBF) blended with item-based CF co-purchase/co-view signals.

### 3. Basket / Cart Page — `POST /api/v1/recommendations/basket`

```
┌─────────────────────────────────────────────────────┐
│  Your Cart                                          │
│  ┌────────────────────────────────────────────────┐│
│  │ pid_A — ASUS ROG Laptop          ×1  ₮4,100,000││
│  │ pid_B — Gaming Mouse             ×1    ₮120,000 ││
│  └────────────────────────────────────────────────┘│
│                                                     │
│  ── Complete your purchase ─────────────────────── │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐    │
│  │ pid1 │ │ pid2 │ │ pid3 │ │ pid4 │ │ pid5 │ …  │
│  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘    │
└─────────────────────────────────────────────────────┘
```

Called when a user opens their cart. Returns complementary products from categories not already in the basket (cross-sell). Basket-aware reranker aggressively penalises same-category items and boosts cross-category complements.

---

## API Reference

Base URL: `http://0.0.0.0:8018` (local) | Interactive docs: `/docs`

### `GET /api/v1/health`

Liveness check. Always returns `200`.

```json
{ "status": "ok", "catalog_ready": true, "version": "4.1.0" }
```

### `GET /api/v1/catalog/status`

Returns TF-IDF index dimensions, taxon map sizes, and session counters.

### `POST /api/v1/catalog/sync`

Triggers a background catalog re-sync from PostgreSQL (runs every 10 min automatically).

---

### `POST /api/v1/events`

Ingest `customer_activities` stream events. Accepts both dict payloads and Oracle Python-literal strings.

```json
{
  "shop_id": "antmall",
  "events": [
    {
      "account_id": "6a5e47214aeec353171ccaa0",
      "session_id": "sess_abc123",
      "activity_name": "order-events",
      "activity_data": {
        "accountid": "6a5e47214aeec353171ccaa0",
        "productid": "698977503516dac1b3e97a6c",
        "action": "complete",
        "quantity": 1
      },
      "user_agent": "Mozilla/5.0 (iPhone; ...)",
      "timestamp": "2026-07-23T08:00:00Z"
    }
  ]
}
```

**`activity_name` values:** `order-events` · `limit-events` · `cart-events` · `wishlist-events` · `view_product` · `taxon_click` · `product_click`

**Response:**
```json
{
  "status": "accepted",
  "processed": 1,
  "failed": 0,
  "recommendations": [
    {
      "id": "6a5e47214aeec353171ccaa0",
      "taxon_id": "69fbef9bda75a61ceadc7607",
      "recommendations": ["pid1", "pid2", "..."],
      "strategy": "hybrid",
      "intent_score": 5.0,
      "device": "mobile",
      "count": 12,
      "served_at": "2026-07-23T08:00:01Z"
    }
  ]
}
```

---

### `POST /api/v1/consumer-events`

Ingest Oracle `consumer_events` rows directly. Field names match the Oracle table (uppercase). Handles `product_click` (extracts `productIds[]` array) and `taxon_click` (resolves Mongolian label).

```json
{
  "events": [
    {
      "ID_": "6a44ce39ce31add3c347e3d6",
      "EVENTNAME": "product_click",
      "EVENTVALUE": "{'productIds': ['69fc469bab34c8d11412ec79'], 'taxon': {'label': 'Үснийхэрэгсэл'}}",
      "ACCOUNTID": "66fbc5824e022311128232ae",
      "SESSIONID": "jPAaTyDWFjD1JsHyR0ux3hewNYRvNvRy",
      "TIMESTAMP_": "2026-07-01T08:21:23.894Z",
      "USERAGENT": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 ...)"
    },
    {
      "ID_": "6a44ce3b420fe633e02e2e78",
      "EVENTNAME": "taxon_click",
      "EVENTVALUE": "{'taxon': {'label': 'Гар утас'}}",
      "ACCOUNTID": "5ff870ee4f636263bd482270",
      "SESSIONID": "2xI3rxpGJbBOeY4vnY_1EBSkTuAL8Pf9",
      "TIMESTAMP_": "2026-07-01T08:22:19.413Z",
      "USERAGENT": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_6 ...)"
    }
  ]
}
```

---

### `POST /api/v1/infer`

On-demand single-taxon inference. Accepts optional session context to seed the ranker.

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "context": {
    "current_taxon_id": "69fb126eae2e0da5c8bca3a0",
    "cart_product_ids": ["6a309f8bd46aca65f8084431"],
    "limit_checked": true,
    "device_type": "mobile"
  },
  "top_n": 10,
  "exclude_product_ids": []
}
```

---

### `POST /api/v1/feed`

Multi-taxon recommendation feed. Returns products organised by the user's top taxons, cross-deduplicated.

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "top_taxons": 3,
  "top_n_per_taxon": 10,
  "extra_taxon_ids": [],
  "exclude_product_ids": []
}
```

**Response:**
```json
{
  "id": "6a5e47214aeec353171ccaa0",
  "taxon_feeds": [
    {
      "taxon_id": "69fb126eae2e0da5c8bca3a0",
      "taxon_name": "household-appliances-multi-purpose-vacuum",
      "recommendations": ["pid1", "pid2", "..."],
      "count": 10,
      "score": 9.5
    },
    {
      "taxon_id": "69fbef9bda75a61ceadc7607",
      "taxon_name": "video-game-racing-wheel",
      "recommendations": ["pid3", "pid4", "..."],
      "count": 10,
      "score": 5.0
    }
  ],
  "total_products": 24,
  "strategy": "multi_taxon_hybrid",
  "intent_score": 9.5,
  "device": "mobile",
  "served_at": "2026-07-23T08:00:01Z"
}
```

---

### `POST /api/v1/feed/push`

Generates the multi-taxon feed **and** POSTs it to the shop's configured endpoint. The main response always returns recommendations regardless of push success or failure.

```json
{
  "account_id": "6a5e47214aeec353171ccaa0",
  "top_taxons": 3,
  "top_n_per_taxon": 10,
  "shop_feed_url": "https://your-shop.example.com/api/recommendations",
  "push_timeout_seconds": 3.0
}
```

If `shop_feed_url` is omitted, uses `config.MARKETPLACE_API_BASE_URL/{account_id}`.

**Push payload sent to shop:**
```json
{
  "id": "<account_id>",
  "taxon_feeds": [
    { "taxon_id": "...", "taxon_name": "...", "recommendations": ["pid", ...] }
  ]
}
```

**Response includes `push_status`:** `"ok"` | `"failed"` | `"not_attempted"`

---

## Project Structure

```
marketplace-stream-data-recommendation-engine/
│
├── config.py                       # Centralised config (ports, DB URLs, table names, weights)
├── requirements.txt                # Production Python dependencies
├── environment.yml                 # Conda environment spec
├── Dockerfile                      # Production image (miniconda3, Oracle instant client)
├── script.py                       # Legacy entry point (preserved from template)
├── PLAN.md                         # System architecture plan
│
├── src/
│   ├── main.py                     # Uvicorn entry point (`python -m src.main`)
│   ├── database.py                 # PostgreSQL helpers (pgsql_import, pgsql_export)
│   │
│   ├── api/
│   │   ├── app.py                  # FastAPI factory, lifespan (startup sync, periodic re-sync)
│   │   ├── schemas/
│   │   │   └── event.py            # Pydantic v2 request/response models (all endpoints)
│   │   ├── routes/
│   │   │   ├── events.py           # Stream ingestion + feed endpoints
│   │   │   ├── recommendations.py  # Placement endpoints (taxon/product/basket)  ← NEW
│   │   │   └── health.py           # /health, /catalog/status, /catalog/sync
│   │   └── middleware/             # (reserved for auth / rate limiting)
│   │
│   └── module/
│       ├── intent_scorer.py        # Event weights, time-decay formula
│       ├── event_processor.py      # Normalization: ACTIVITYDATA parser, product_click,
│       │                           #   taxon label resolver, device detection
│       ├── feature_store.py        # In-memory singleton: sessions, user-item scores,
│       │                           #   popularity, taxon maps
│       ├── catalog_sync.py         # PG fetch, field normalization, TF-IDF build,
│       │                           #   taxon maps, scheduled re-sync
│       ├── content_based.py        # CBF: TF-IDF cosine similarity search
│       ├── collaborative.py        # CF: item-based co-interaction scoring  ← NEW
│       ├── hybrid_ranker.py        # RRF merge, context re-rankers, recommend(),
│       │                           #   recommend_multi_taxon()
│       ├── settings.py             # Oracle credentials via pydantic-settings / .env
│       ├── database.py             # Oracle import/export/execute
│       ├── data_minification.py    # Pandas memory reducer (int/float downcast)
│       └── helper.py               # @timeit, @logging_timer decorators
│
├── docs/
│   └── API_GUIDE.md                # Comprehensive API integration guide  ← NEW
│
├── notebooks/
│   ├── test.ipynb                  # Data exploration + live API integration cells
│   ├── model.ipynb                 # Model development
│   ├── engagement.ipynb            # Engagement analysis
│   ├── stream.ipynb                # Streaming experiments
│   ├── product_profile.ipynb       # Catalog profiling
│   └── post.ipynb                  # API testing
│
├── models/                         # Model artifacts (als_model, faiss index, embeddings)
├── data/                           # Local data files
├── meta/
│   ├── marketplace.env             # Environment variables template
│   └── pg.cred                     # PostgreSQL credentials (host/user/pass, one per line)
└── reports/                        # Error analysis, evaluation reports
```

---

## Configuration

All settings are in `config.py`. Override any value with the corresponding environment variable.

| Variable | Env override | Default | Description |
|---|---|---|---|
| `ENVIRONMENT` | `TOKI_ENV` | `production` | `production` or `staging` |
| `APP_PORT` | `TOKI_APP_PORT` | `8018` | API server port |
| `DB_HOST` | `TOKI_DB_HOST` | `10.21.67.188` | PostgreSQL host |
| `CATALOG_TABLE` | — | auto (env-based) | `marketplace_catalog_data_extended_version3` |
| `SYNC_INTERVAL_MINUTES` | — | `10` | Catalog re-sync interval |
| `RECOMMENDATION_CACHE_TTL` | `TOKI_REC_CACHE_TTL` | `14400` | 4 hours |
| `WORKERS` | `TOKI_WORKERS` | `24` | Gunicorn worker count |
| `MARKETPLACE_API_BASE_URL` | — | `http://10.21.60.94:9000/marketplace` | Shop feed push target |

### PostgreSQL credentials (`meta/pg.cred`)

```
username
password
host_ip_address
```

### Oracle credentials (`.env` or environment)

```bash
oracle_username=...
oracle_password=...
oracle_service=...
oracle_hostname=...
oracle_port=1521
```

---

## Running the Server

### Development (hot-reload)

```bash
TOKI_ENV=staging uvicorn src.api.app:app --host 0.0.0.0 --port 8018 --reload
```

### Direct Python entry point

```bash
python -m src.main
```

### Production (Gunicorn + Uvicorn workers)

```bash
gunicorn src.api.app:app \
  -k uvicorn.workers.UvicornWorker \
  -b 0.0.0.0:8018 \
  -w 4 \
  --timeout 120
```

### Verify

```bash
curl http://localhost:8018/api/v1/health
curl http://localhost:8018/docs         # Interactive Swagger UI
```

---

## Docker Deployment

The image uses `continuumio/miniconda3`, installs Oracle Instant Client, and runs the app.

### Build and run (existing Dockerfile)

```bash
# Build
docker build -t toki-rec-engine:v4.1.0 .

# Run with env file
docker run --rm \
  --env-file=$HOME/envs/marketplace.env \
  -p 8018:8018 \
  toki-rec-engine:v4.1.0
```

### Dockerfile update needed

The current `Dockerfile` runs `script.py`. To serve the API, change the final CMD:

```dockerfile
# Replace:
CMD ["python3", "script.py"]

# With:
COPY config.py /myapp
CMD ["gunicorn", "src.api.app:app", \
     "-k", "uvicorn.workers.UvicornWorker", \
     "-b", "0.0.0.0:8018", "-w", "4"]
```

Also add the new dependencies to `requirements.txt`:

```
fastapi
uvicorn[standard]
gunicorn
scikit-learn
numpy
httpx
```

### Create Release (auto-build CI)

```bash
git tag v4.1.0
git push origin v4.1.0
```

---

## Internet Exposure (Tunnel)

For sharing with shop developers during development, use Cloudflare Quick Tunnel:

```bash
# Download once
curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 \
  -o /tmp/cloudflared && chmod +x /tmp/cloudflared

# Start tunnel (prints a public *.trycloudflare.com URL)
/tmp/cloudflared tunnel --url http://localhost:8018 --no-autoupdate
```

The public URL (`https://<random>.trycloudflare.com`) gives shop developers full access to:
- Swagger UI: `<public_url>/docs`
- All API endpoints

> **Note:** Quick Tunnels are ephemeral and for testing only. For production, create a named tunnel with a Cloudflare account: `cloudflared tunnel create toki-rec-engine`

---

## Development Notes

### Adding a new event type

1. Add its weight to `INTENT_WEIGHTS` in `src/module/intent_scorer.py`
2. If it comes from `consumer_events`, extend `normalize_consumer_event()` in `event_processor.py`
3. If it carries product interactions, handle `is_basket_add` / `is_basket_remove` / `is_limit_check` flags

### Catalog sync

- Runs automatically every 10 minutes in the background
- Force a re-sync: `POST /api/v1/catalog/sync`
- The TF-IDF matrix is rebuilt in-process (~2 sec for 4,121 products)

### Delivery log

Delivered recommendations are persisted asynchronously (fire-and-forget) to the `rec_engine_delivery_log` table in the write database. The table is auto-created on first write.

### Notebooks

The `notebooks/test.ipynb` notebook contains:
- Oracle data exploration (`consumer_events`, `customer_activities`)
- Live API integration cells (catalog status, `/consumer-events`, `/feed`, `/feed/push`)
- Sample payloads matching the exact Oracle column format
