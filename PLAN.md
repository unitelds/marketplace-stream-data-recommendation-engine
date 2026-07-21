# Marketplace Recommendation Engine — Comprehensive Development Plan

> **Project:** TOKI Marketplace Stream-Data Recommendation Engine
> **Date:** 2026-07-21
> **Stack:** Python · FastAPI · PostgreSQL · Oracle · Redis · Kafka (or async queue)

---

## Table of Contents

1. [System Architecture Overview](#1-system-architecture-overview)
2. [Data Layer & Product Profile Sync](#2-data-layer--product-profile-sync)
3. [Engagement Signal Taxonomy & Buying Intent Scoring](#3-engagement-signal-taxonomy--buying-intent-scoring)
4. [Feature Engineering](#4-feature-engineering)
5. [Recommendation Models](#5-recommendation-models)
6. [Streaming Ingestion Endpoint](#6-streaming-ingestion-endpoint)
7. [Real-Time Inference Unit](#7-real-time-inference-unit)
8. [Model Training, Evaluation & Validation Pipeline](#8-model-training-evaluation--validation-pipeline)
9. [Response Format & Delivery](#9-response-format--delivery)
10. [Project Folder Structure](#10-project-folder-structure)
11. [Development Phases & Milestones](#11-development-phases--milestones)
12. [Open Issues & Decisions](#12-open-issues--decisions)

---

## 1. System Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           TOKI RECOMMENDATION ENGINE                        │
│                                                                             │
│  ┌──────────────┐    POST /events     ┌────────────────────────────────┐   │
│  │  Shop Data   │ ──────────────────► │  Stream Ingestion API (FastAPI) │   │
│  │  Streams     │                     │  /api/v1/events  (async)       │   │
│  └──────────────┘                     └──────────────┬─────────────────┘   │
│                                                       │ publish              │
│  ┌──────────────┐                     ┌──────────────▼─────────────────┐   │
│  │  Oracle DB   │                     │     Event Queue / Kafka Topic  │   │
│  │  consumer_   │────────────────────►│     marketplace.engagement     │   │
│  │  events /    │  (batch pull)        └──────────────┬─────────────────┘   │
│  │  activities  │                                      │ consume              │
│  └──────────────┘                     ┌──────────────▼─────────────────┐   │
│                                        │   Data Processing Unit (DPU)   │   │
│  ┌──────────────┐                     │   - Event normalization         │   │
│  │  PostgreSQL  │                     │   - Taxon ID resolution         │   │
│  │  master_     │────────────────────►│   - Buying intent scoring       │   │
│  │  catalog_    │  (scheduled sync)    │   - Session aggregation         │   │
│  │  profile     │                     └──────────────┬─────────────────┘   │
│  └──────────────┘                                     │                     │
│                                        ┌──────────────▼─────────────────┐   │
│  ┌──────────────┐                     │        Feature Store (Redis)    │   │
│  │  Redis Cache │◄────────────────────│   user_vectors / item_vectors   │   │
│  │  TTL-backed  │                     │   session_context / intent_score│   │
│  └──────┬───────┘                     └──────────────┬─────────────────┘   │
│         │                                             │                     │
│  ┌──────▼───────────────────────────────────────────▼─────────────────┐   │
│  │                    INFERENCE UNIT                                    │   │
│  │   ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐ │   │
│  │   │ Collab Filter│  │ Content-Based│  │  Context-Aware / Session  │ │   │
│  │   │   (ALS/NCF)  │  │  (Embeddings)│  │     Basket Re-rank        │ │   │
│  │   └──────┬───────┘  └──────┬───────┘  └────────────┬─────────────┘ │   │
│  │          └─────────────────┴──────────────────────── ┘             │   │
│  │                        Hybrid Ranker + Intent Re-ranker             │   │
│  └──────────────────────────────────────────────────────────────────── ┘   │
│                                        │                                    │
│                          POST response ▼                                    │
│              { "id": "<accountid>",                                         │
│                "taxon_id": "<taxon>",                                       │
│                "recommendations": ["<product_id>", ...] }                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Data Layer & Product Profile Sync

### 2.1 Product Profile (`master_catalog_profile`)

**Source:** PostgreSQL — `marketplace_catalog_data_extended_version3`

| Field | Usage |
|---|---|
| `product_id` | Primary key for all recommendation lookups |
| `taxon_id` / `taxon_name` | Taxonomy linkage to consumer_events |
| `main_category`, `sub_category`, `product_category` | Category hierarchy features |
| `manufacturer`, `generic_name` | Brand & type features |
| `specifications` (JSON) | Structured product attributes |
| `price_range`, `premium_grade` | Price-tier features |
| `keywords` | TF-IDF content features |
| `details`, `description` | NLP embedding source |
| `best_used_for` | Use-case clustering |
| `stock` | Availability filter (exclude OOS) |
| `group_id` | Product variant grouping |

**Sync Strategy:**
- **Scheduled pull:** every `SYNC_INTERVAL_MINUTES = 10` (already in `config.py`)
- **Change detection:** compare `CHECKSUM` field; only re-embed products where checksum changed
- **In-memory cache:** keep the full catalog vector matrix in Redis (`PRODUCT_CACHE_TTL = 7200`)
- **Embedding rebuild:** trigger async background task when > N products changed (threshold TBD)
- **Cold-start guard:** if catalog is empty on startup, block inference until first sync completes

```python
# src/module/catalog_sync.py (to be created)
async def sync_product_profiles():
    """Pull catalog from PG, diff against cached checksums,
       re-embed changed products, push to feature store."""
```

### 2.2 Taxon Normalization

`consumer_events.EVENTVALUE` when `EVENTNAME = 'taxon_click'` returns Mongolian-label dict:
```json
{"taxon": {"label": "<mongolian_taxon_name>"}}
```

**Resolution plan:**
1. Build a `taxon_label → taxon_id` lookup table from `master_catalog_profile` at sync time
2. Store in Redis hash `taxon:label_map`
3. DPU resolves label → `taxon_id` before feature encoding
4. Unresolvable labels → log to `ERROR_ANALYSIS_TABLE`, skip event

---

## 3. Engagement Signal Taxonomy & Buying Intent Scoring

### 3.1 Event Source Mapping

| Source Table | Event Field | Values | Meaning |
|---|---|---|---|
| `customer_activities` | `ACTIVITYNAME` | `order-events` | Purchase placed / completed |
| `customer_activities` | `ACTIVITYNAME` | `limit-events` | User checked lease/credit limit → strong purchase intent |
| `customer_activities` | `ACTIVITYNAME` | `wishlist-events` | Saved to wishlist |
| `customer_activities` | `ACTIVITYNAME` | `cart-events` | Cart add / remove / modify |
| `customer_activities` | `ACTIVITYNAME` | `view_product` | Product detail page view |
| `consumer_events` | `EVENTNAME` | `taxon_click` | Category/taxon browsing |

### 3.2 `ACTIVITYDATA` Schema (from `customer_activities`)

The JSON payload inside `ACTIVITYDATA` contains at minimum:
```json
{
  "accountid": "<user_id>",
  "productid": "<product_id>",
  "sessionid": "<session>",
  "timestamp": "...",
  "quantity": 1,
  "action": "add|remove|complete|..."
}
```
**Parsing:** use `ast.literal_eval()` (already confirmed working in test notebook).

### 3.3 Buying Intent Score

Each event contributes a weighted score to a user–product interaction signal:

| Event | Sub-action | Intent Weight | Rationale |
|---|---|---|---|
| `order-events` | `complete` | **5.0** | Highest signal — purchase done |
| `order-events` | `place` / `initiate` | 4.5 | Strong intent |
| `limit-events` | any | **4.0** | Checked financial limit = ready to buy |
| `cart-events` | `add` | 3.5 | Active basket intent |
| `wishlist-events` | `add` | 3.0 | Desire signal |
| `view_product` | any | 1.0 | Interest signal |
| `taxon_click` | any | 0.5 | Discovery/browsing |
| `cart-events` | `remove` | **−1.5** | Negative signal — abandon |

**Aggregate user–product score:**
$$S(u, p) = \sum_{e \in \text{events}(u,p)} w_e \cdot \gamma^{(t_{\text{now}} - t_e) / T_{\text{half}}}$$

where $\gamma$ is a decay factor (e.g. 0.9) and $T_{\text{half}}$ is a half-life window (e.g. 7 days). This produces a time-decayed implicit feedback matrix suitable for ALS.

### 3.4 Session-Level Context

From `consumer_events`:
- `SESSIONID` → group events into a session window (30-min inactivity = new session)
- `USERAGENT` → parse device type: `mobile` / `desktop` / `miniprogram` — used as a context feature
- Session sequence of `taxon_click` → reveals browse intent path (e.g. Electronics → Gaming → Racing Wheels)

---

## 4. Feature Engineering

### 4.1 User Features (Collaborative Signal)

| Feature | Source | Method |
|---|---|---|
| Implicit feedback matrix $(u, p, S(u,p))$ | `customer_activities` | ALS input |
| Top-N interacted taxons | `consumer_events` | frequency rank |
| Preferred price range | `customer_activities` + catalog | weighted avg |
| Preferred brands | order/wishlist events | frequency |
| Device type | `consumer_events.USERAGENT` | categorical |
| Activity recency | timestamp of last event | float (days ago) |
| Cart-to-order conversion rate | `cart-events` vs `order-events` | ratio |

### 4.2 Product Features (Content Signal)

| Feature | Source | Method |
|---|---|---|
| Category hierarchy vector | `main_category` → `product_category` | one-hot / label encoding |
| Price tier | `price_range` | ordinal: budget/mid/high-end/luxury |
| Premium grade | `premium_grade` | binary/ordinal |
| Specification embedding | `specifications` JSON | flatten → TF-IDF or sentence embedding |
| Keyword embedding | `keywords` list | TF-IDF |
| Description embedding | `details` + `description` | Sentence-BERT (`paraphrase-multilingual`) — supports Mongolian |
| Taxon vector | `taxon_id` | learned embedding |
| Availability | `stock > 0` | boolean gate |
| Manufacturer | `manufacturer` | categorical embedding |

### 4.3 Context Features

| Feature | Source | Method |
|---|---|---|
| Session taxon sequence | `consumer_events` | last-N taxons in session |
| Current basket contents | `cart-events` (active) | product_id list → aggregate taxon/brand/price profile |
| Time of day | event timestamp | hour bucket (morning/afternoon/evening/night) |
| Day of week | event timestamp | categorical |
| Limit-check flag | `limit-events` in session | binary — boosts budget-tier products |
| Session length | event count in session | binned |

---

## 5. Recommendation Models

### 5.1 Collaborative Filtering — Implicit ALS

**Library:** `implicit` (Python, CPU/GPU ALS)

- **Input:** sparse user–item matrix with decayed intent scores $S(u, p)$
- **Factors:** 128–256 latent dimensions
- **Regularization:** λ = 0.01–0.1 (tuned via cross-validation)
- **Confidence:** $c_{ui} = 1 + \alpha \cdot S(u, p)$ where $\alpha = 40$
- **Output:** user embedding $\mathbf{u} \in \mathbb{R}^d$, item embedding $\mathbf{p} \in \mathbb{R}^d$
- **Retrieval:** approximate nearest-neighbor search via `faiss` (HNSW index)
- **Cold-start:** fall back to content-based for users with < 3 interactions

```python
# src/module/collaborative.py
# train_als(interaction_matrix) → user_factors, item_factors
# get_cf_candidates(user_id, k=50) → List[product_id]
```

### 5.2 Content-Based Filtering

**Approach:** Product vector similarity via pre-computed embeddings

- **Text embeddings:** `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`
  - Handles Mongolian descriptions natively
  - 384-dim output, quantized to float16 for storage
- **Structured features:** concatenate category one-hot + price tier + brand embedding
- **Final product vector:** `[text_embed | struct_features]` → normalized L2
- **Index:** `faiss.IndexFlatIP` (inner product = cosine on normalized vectors)
- **Usage:** given a seed product or user's top-interacted products → retrieve similar items

```python
# src/module/content_based.py
# embed_catalog(catalog_df) → np.ndarray  [N_products × D]
# get_cbf_candidates(seed_product_ids, k=50) → List[product_id]
```

### 5.3 Context-Aware Re-ranking

Applied **after** CF + CBF candidate generation (typically top-50 from each):

**a) Session Context Re-ranker**
- Extract current session's taxon sequence from Redis
- Boost products matching the last 2–3 taxons in the session path
- Formula: $\text{score}_{ctx}(p) = \text{base\_score}(p) \times (1 + \beta \cdot \mathbb{1}[\text{taxon}(p) \in \text{session\_path}])$

**b) Basket-Aware Re-ranker**
- Products currently in cart define a "basket profile" (avg taxon, price range, brand)
- Boost complementary products (cross-sell: e.g., vacuum → vacuum bags, gaming wheel → gaming chair)
- Penalize exact-same-category duplicates (no duplicate racquet if one is in cart)
- **Association rules:** mine frequent itemsets from `order-events` using `mlxtend.apriori` offline, load rules into Redis

**c) Limit-Request Boost**
- If `limit-events` was fired in the current session:
  - Re-rank to prioritize products **within** the user's checked limit amount
  - Demote products clearly above the limit by 30%

**d) Buying Intent Boost**
- Global session intent score $I_s = \sum_{e \in \text{session}} w_e$
- High intent ($I_s > 8$): boost `order-events`-correlated products (viewed-then-ordered historically)
- Low intent ($I_s < 2$): broaden diversity — include discovery items from adjacent taxons

**e) Device Context**
- `miniprogram` (WeChat mini-program): return max 6–8 products, prefer image-heavy premium items
- `mobile`: return 10 products, balanced mix
- `desktop`: return 20 products, full grid

### 5.4 Hybrid Combiner

Merge CF candidates + CBF candidates using a weighted reciprocal rank fusion (RRF):

$$\text{RRF\_score}(p) = \frac{\lambda_{\text{CF}}}{k + \text{rank}_{\text{CF}}(p)} + \frac{\lambda_{\text{CBF}}}{k + \text{rank}_{\text{CBF}}(p)}$$

Default weights: $\lambda_{\text{CF}} = 0.6$, $\lambda_{\text{CBF}} = 0.4$, $k = 60$

After RRF → apply context re-rankers in sequence → final top-N list.

### 5.5 Cold-Start Strategies

| Scenario | Strategy |
|---|---|
| New user (0 events) | Global popularity by taxon + trending products |
| New user + taxon_click | CBF from clicked taxon's top products |
| New product (< 5 interactions) | CBF only, inject into CF candidates via item embedding interpolation |
| Known user, new session | Use historical user embedding + live session context overlay |

---

## 6. Streaming Ingestion Endpoint

### 6.1 API Design

**Base path:** `/api/v1/`

#### `POST /api/v1/events` — Receive shop stream events

```
Request Headers:
  Authorization: Bearer <shop_api_key>
  Content-Type: application/json

Request Body:
{
  "shop_id": "string",
  "events": [
    {
      "event_id": "string",
      "account_id": "string",            // user ID
      "session_id": "string",
      "activity_name": "cart-events",    // or order-events, limit-events, etc.
      "activity_data": {
        "product_id": "string",
        "action": "add|remove|complete",
        "quantity": 1,
        "price": 4100000,
        "taxon_id": "string",
        "timestamp": "2026-07-21T10:00:00Z"
      },
      "user_agent": "string",
      "p_date": "20260721"
    }
  ]
}

Response 200:
{
  "status": "accepted",
  "processed": 5,
  "recommendations": [
    {
      "id": "<account_id>",
      "taxon_id": "<resolved_taxon_id>",
      "recommendations": ["<product_id_1>", "<product_id_2>", ...]
    }
  ]
}
```

#### `POST /api/v1/infer` — On-demand inference for a known user

```
Request Body:
{
  "account_id": "string",
  "session_id": "string",
  "context": {
    "current_taxon_id": "string",         // optional: current page context
    "cart_product_ids": ["string"],        // optional: basket contents
    "limit_checked": false,               // optional: was limit-events fired
    "device_type": "mobile|desktop|miniprogram"
  },
  "top_n": 10
}

Response 200:
{
  "id": "<account_id>",
  "taxon_id": "<context_taxon_id>",
  "recommendations": ["<product_id>", ...],
  "strategy": "hybrid|cf|cbf|popular",   // which strategy was used
  "intent_score": 4.5
}
```

#### `GET /api/v1/health` — Health check

#### `GET /api/v1/metrics` — Prometheus metrics endpoint (port `MONITORING_PORT`)

### 6.2 Async Processing Flow

```
POST /events received
   │
   ├── Validate auth (API key per shop)
   ├── Validate payload schema (Pydantic)
   ├── Persist raw events → Oracle / PG (async, non-blocking)
   │
   ├── For each event:
   │     ├── Parse ACTIVITYDATA
   │     ├── Resolve taxon label → taxon_id (Redis lookup)
   │     ├── Compute intent score contribution
   │     ├── Update user session state in Redis (EXPIRE = 30 min)
   │     └── Update user–item score in Redis (ZINCRBY)
   │
   └── Trigger real-time inference for account_ids in batch
         └── Return recommendations in response body
```

### 6.3 Rate Limiting & Security

- Per-shop API key in `Authorization: Bearer` header
- Rate limit: 1000 events/sec per shop (token bucket in Redis)
- Payload size limit: 5 MB per request, max 500 events per batch
- Input validation: all fields validated via Pydantic v2 models
- SQL injection prevention: parameterized queries only (SQLAlchemy ORM)
- Secrets: all credentials from environment variables, never hardcoded

---

## 7. Real-Time Inference Unit

### 7.1 Inference Pipeline (per request)

```
1. Load user embedding from Redis (or compute from interaction matrix)
2. Load session context from Redis (taxon path, basket, intent score)
3. Run CF retrieval:  faiss.search(user_vector, k=50)  → candidate_cf
4. Run CBF retrieval: faiss.search(session_seed_vector, k=50) → candidate_cbf
5. Merge via RRF → merged_candidates (up to 80 unique products)
6. Filter: remove OOS (stock=0), remove already-purchased (from order history)
7. Context re-rank:
     a. Session taxon boost
     b. Basket-aware (cross-sell rules)
     c. Limit-event price filter
     d. Intent-level boost
8. Device-level top-N truncation
9. Log delivered recommendations → DELIVERED_RECOMMENDATIONS_TABLE
10. Return response
```

### 7.2 Latency Budget

| Step | Target |
|---|---|
| Redis lookup (user embed + session) | < 2ms |
| FAISS ANN search (CF + CBF) | < 5ms |
| Re-ranking logic | < 3ms |
| DB async write (fire-and-forget) | non-blocking |
| **Total P95** | **< 20ms** |

### 7.3 Fallback Chain

```
Has user embedding?
  YES → Hybrid (CF + CBF + context)
  NO  →
    Has session taxon_clicks?
      YES → CBF from taxon seed + popularity
      NO  → Global popularity (top products by taxon, refreshed every 10 min)
```

---

## 8. Model Training, Evaluation & Validation Pipeline

### 8.1 Data Split Strategy

- **Temporal split** (mandatory — no random split to prevent leakage):
  - Train: all events before `T - 14 days`
  - Validation: events in `[T - 14, T - 7]`
  - Test: events in `[T - 7, T]`
- Stratify split by user activity level (active / moderate / light users)

### 8.2 Offline Evaluation Metrics

| Metric | Description | Target |
|---|---|---|
| **Recall@10** | % of held-out items recovered in top 10 | > 0.25 |
| **NDCG@10** | Normalized discounted cumulative gain | > 0.18 |
| **Hit Rate@5** | At least 1 relevant item in top 5 | > 0.40 |
| **MRR** | Mean reciprocal rank of first relevant item | > 0.20 |
| **Coverage** | % of catalog recommended at least once | > 30% |
| **Intra-list diversity** | Avg pairwise distance between recommended items | > 0.45 |

### 8.3 Training Pipeline

```
notebooks/model.ipynb  →  production script via src/module/trainer.py

Steps:
1. Load interaction matrix from Oracle + PG
2. Apply decay weighting → sparse CSR matrix
3. Train ALS model (implicit library)
4. Build product content embedding matrix (sentence-transformers)
5. Build FAISS index for CF factors + product embeddings
6. Evaluate on validation split
7. If metrics pass thresholds → serialize models to models/ directory
8. Push model artifacts to artifact store (local path or S3)
9. Reload inference unit with new models (zero-downtime hot-swap)
```

**Retraining schedule:** daily at 02:00 local time (low-traffic window)

### 8.4 Model Artifacts

```
models/
  als_model.npz               # user_factors, item_factors
  faiss_cf.index              # FAISS index over item factors
  faiss_cbf.index             # FAISS index over content embeddings
  product_embeddings.npy      # [N × D] float16 matrix
  product_id_index.json       # position → product_id mapping
  user_id_index.json          # position → account_id mapping
  taxon_label_map.json        # mongolian label → taxon_id
  association_rules.pkl       # mlxtend basket rules
  model_metadata.json         # training date, metrics, version
```

### 8.5 Online A/B Testing

- Split traffic: 80% current model / 20% challenger model
- Track: CTR, add-to-cart rate, order conversion rate per cohort
- Promotion: auto-promote challenger if CTR uplift > 5% over 48h window
- Log assignments to `DELIVERED_RECOMMENDATIONS_TABLE` (`strategy` column)

---

## 9. Response Format & Delivery

### 9.1 Synchronous Response (POST /events)

```json
{
  "id": "6a5e47214aeec353171ccaa0",
  "taxon_id": "69fbef9bda75a61ceadc7607",
  "recommendations": [
    "698977503516dac1b3e97a6c",
    "6a309f8bd46aca65f8084431",
    "69fc469bab34c8d11412ec79"
  ],
  "strategy": "hybrid",
  "intent_score": 7.5,
  "device": "mobile",
  "served_at": "2026-07-21T10:00:01.234Z"
}
```

### 9.2 Batch Pre-computed Recommendations

- Run nightly batch for all active users (active in last 30 days)
- Store to PostgreSQL: `USER_RECOMMENDATIONS_TABLE`
- Serve as fallback if real-time inference is unavailable

### 9.3 User Preference Store

```json
// USER_PREFERENCES_TABLE — keyed by account_id
{
  "account_id": "...",
  "top_taxons": ["taxon_id_1", "taxon_id_2"],
  "preferred_price_range": "high-end",
  "preferred_brands": ["DYSON", "THRUSTMASTER"],
  "intent_profile": "buyer",         // buyer | browser | researcher
  "last_updated": "2026-07-21T..."
}
```

---

## 10. Project Folder Structure

```
marketplace-stream-data-recommendation-engine/
├── config.py                          # ✅ Centralized config (exists)
├── PLAN.md                            # ✅ This document
├── requirements.txt                   # extend with new deps below
│
├── src/
│   ├── main.py                        # ✅ Entry point (extend)
│   ├── database.py                    # ✅ PG connector (exists)
│   ├── api/
│   │   ├── __init__.py
│   │   ├── app.py                     # FastAPI app factory
│   │   ├── routes/
│   │   │   ├── events.py              # POST /events, POST /infer
│   │   │   ├── health.py              # GET /health, GET /metrics
│   │   │   └── __init__.py
│   │   ├── schemas/
│   │   │   ├── event.py               # Pydantic models for request/response
│   │   │   └── __init__.py
│   │   └── middleware/
│   │       ├── auth.py                # API key validation
│   │       └── rate_limit.py          # Redis token bucket
│   │
│   ├── module/
│   │   ├── __init__.py                # ✅ exists
│   │   ├── settings.py                # ✅ exists
│   │   ├── helper.py                  # ✅ exists
│   │   ├── data_minification.py       # ✅ exists
│   │   ├── database.py                # ✅ Oracle connector (exists)
│   │   │
│   │   ├── catalog_sync.py            # Product profile sync + embedding rebuild
│   │   ├── event_processor.py         # DPU: parse, normalize, score events
│   │   ├── feature_store.py           # Redis interface: user/item vectors, session
│   │   ├── collaborative.py           # ALS training + CF retrieval (FAISS)
│   │   ├── content_based.py           # Sentence-BERT embedding + CBF retrieval
│   │   ├── hybrid_ranker.py           # RRF combiner + context re-rankers
│   │   ├── intent_scorer.py           # Buying intent score computation
│   │   ├── session_manager.py         # Session windowing + taxon path tracking
│   │   ├── basket_analyzer.py         # Cart analysis + association rules
│   │   └── trainer.py                 # Offline training pipeline
│   │
│   └── README.md
│
├── notebooks/
│   ├── test.ipynb                     # ✅ exploration (exists)
│   ├── model.ipynb                    # ✅ model dev (exists)
│   ├── engagement.ipynb               # ✅ engagement analysis (exists)
│   ├── stream.ipynb                   # ✅ streaming experiments (exists)
│   ├── product_profile.ipynb          # ✅ catalog profiling (exists)
│   └── post.ipynb                     # ✅ API testing (exists)
│
├── models/                            # ✅ model artifact storage (exists)
├── data/                              # ✅ local data files (exists)
├── meta/                              # ✅ credentials (exists)
└── Dockerfile                         # ✅ extend for FastAPI + Redis
```

### New Dependencies to Add to `requirements.txt`

```
# API
fastapi
uvicorn[standard]
gunicorn

# ML / Recommendation
implicit              # ALS collaborative filtering
faiss-cpu             # ANN search (use faiss-gpu if GPU available)
sentence-transformers # multilingual product embeddings
scikit-learn          # preprocessing, evaluation metrics
mlxtend               # association rules (basket analysis)
scipy                 # sparse matrix handling

# Feature store
redis[hiredis]        # Redis client

# Data processing
numpy
pyarrow               # efficient serialization

# Monitoring
prometheus-client

# Async DB
asyncpg               # async PostgreSQL driver
```

---

## 11. Development Phases & Milestones

### Phase 1 — Foundation (Week 1–2)
- [ ] Set up FastAPI app skeleton (`src/api/app.py`)
- [ ] Build `POST /events` endpoint with Pydantic validation
- [ ] Implement `event_processor.py`: parse `ACTIVITYDATA`, resolve taxon labels, compute intent score
- [ ] Set up Redis feature store interface (`feature_store.py`)
- [ ] Implement `catalog_sync.py`: scheduled pull from PG, checksum diff

### Phase 2 — Content-Based Model (Week 3)
- [ ] Implement `content_based.py`: embed catalog with multilingual sentence-transformers
- [ ] Build FAISS CBF index; validate recall on test queries
- [ ] Implement cold-start path: new user → taxon_click CBF
- [ ] Unit tests for embedding pipeline

### Phase 3 — Collaborative Filtering (Week 4)
- [ ] Build interaction matrix from `customer_activities` with decay weights
- [ ] Train ALS model; evaluate Recall@10 / NDCG@10 on temporal split
- [ ] Build FAISS CF index; integrate user embedding retrieval
- [ ] Implement `hybrid_ranker.py` with RRF merge

### Phase 4 — Context-Aware & Basket Logic (Week 5)
- [ ] Implement `session_manager.py`: session windowing, taxon path tracking
- [ ] Implement `basket_analyzer.py`: cart state extraction + association rule inference
- [ ] Add limit-event price filter, device-type truncation
- [ ] Integrate all re-rankers into hybrid pipeline

### Phase 5 — Inference API & Delivery (Week 6)
- [ ] Wire full inference pipeline to `POST /infer` and `POST /events` response
- [ ] Implement recommendation delivery logging
- [ ] Set up `GET /health` and Prometheus metrics
- [ ] Load and performance testing (target < 20ms P95)

### Phase 6 — Training Pipeline & A/B Testing (Week 7–8)
- [ ] Build `trainer.py` with full offline training workflow
- [ ] Schedule nightly retraining via cron / Airflow task
- [ ] Implement A/B traffic splitting and metric tracking
- [ ] Documentation and deployment runbook

---

## 12. Open Issues & Decisions

| # | Issue | Decision Needed |
|---|---|---|
| 1 | **Taxon label map completeness** — are all Mongolian taxon labels present in `master_catalog_profile`? | Audit before phase 1 — query distinct `taxon_name` from catalog vs events |
| 2 | **Event streaming transport** — use Kafka, Redis Streams, or async in-process queue? | Redis Streams recommended for simplicity (already in stack); migrate to Kafka if event volume > 10k/sec |
| 3 | **`ACTIVITYDATA` schema stability** — does the JSON structure vary per activity type? | Extract and document exact schema per `ACTIVITYNAME` from Oracle sample data |
| 4 | **GPU availability** — use `faiss-gpu` and `sentence-transformers` on GPU? | Confirm server hardware; `faiss-cpu` is sufficient for < 500k products |
| 5 | **Account ID linkage** — is `consumer_events.ACCOUNTID` always the same as `customer_activities.ACTIVITYDATA.accountid`? | Confirm join key before building interaction matrix |
| 6 | **Batch pre-computation scope** — which users get nightly batch recs? | Define "active user" threshold (e.g., at least 1 event in last 30 days) |
| 7 | **Association rules granularity** — item-level or taxon-level basket rules? | Start with taxon-level for sparsity reasons; promote to item-level when order history grows |
| 8 | **Shop API key management** — how are keys issued and rotated? | Define key issuance flow; store hashed keys in PG `shop_api_keys` table |
| 9 | **`consumer_events` mini-program vs web** — are miniprogram events in `consumer_events` or only in `MONGO_MINIPROGRAMUSERLOGS`? | Clarify data pipeline ownership; may need to unify event streams |
| 10 | **Recommendation diversity** — should the same shop's products dominate results? | Add shop-level diversity cap (max 30% from a single shop per response) |
