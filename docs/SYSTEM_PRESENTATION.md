# TOKI Marketplace · Recommendation Engine
### A Living, Learning Intelligence — Built for Our Marketplace

---

> **Who is this for?**
> Product owners, business directors and managers, and marketplace teams who want to understand *what* this system does, *where* data comes from, and *how* it shapes every shopping experience — without needing to read code.

---

## 01 · What Does It Do?

The recommendation engine watches what every shopper does — what they browse, add to cart, order, and wishlist — and uses that signal to surface the right products to the right person at the right moment.

It runs **24 / 7**, learns **in real-time**, and personalises across **five distinct placements** inside the marketplace — including a dedicated handset & device pipeline.

---

## 02 · System at a Glance

```mermaid
graph TB
    subgraph SOURCES["Data Sources"]
        S1["🛒 Shop Activity Stream\nCart - Orders - Wishlists - Views"]
        S2["🗄️ Oracle Event Warehouse\nHistorical consumer events, 30 days"]
        S3["📦 PostgreSQL Catalog\nProducts, Taxons, Stock, Prices"]
        S4["📱 Marketplace Catalogue API\nHandset device feed, per-user"]
        S5["🔗 Toki Shop Feed\nPre-computed taxon slots, per-user"]
    end

    subgraph ENGINE["Recommendation Engine"]
        FE["🧠 Feature Store\nIn-memory, Per-user, Real-time"]
        AL["⚙️ Algorithm Layer\nCBF, CF, Popularity, RRF Fusion"]
        RR["🎯 Re-ranker\nContext, Basket, Budget, Diversity"]
        FE --> AL --> RR
    end

    subgraph PLACEMENTS["Placements"]
        P1["🗂️ Category Page\nProduct Grid"]
        P2["🔍 Product Page\nYou May Also Like"]
        P3["🧺 Basket Page\nCross-Sell"]
        P4["📡 Feed Push\nMulti-taxon Feed"]
        P5["📱 Handset Page\nAccessories + Device Feed"]
    end

    S1 -->|real-time events| FE
    S2 -->|poll every 60s| FE
    S3 -->|sync every 10min| AL
    S4 -->|per request, 2s timeout| AL
    S5 -->|per request, 1s timeout| AL
    RR --> P1
    RR --> P2
    RR --> P3
    RR --> P4
    RR --> P5
```

---

## 03 · Where Data Comes From

```mermaid
flowchart LR
    subgraph SHOP["Marketplace Shop"]
        A1["Cart Events"]
        A2["Order Events"]
        A3["Wishlist Events"]
        A4["Product Views"]
        A5["Category Browsing"]
    end

    subgraph ORACLE["Oracle Warehouse"]
        B1["Consumer Event History\n30 days · millions of rows\npoll every 60 s · batch 500"]
    end

    subgraph POSTGRES["PostgreSQL Catalog"]
        C1["50,000+ Products\nPrices · Stock · Categories\nSpecs · Keywords · Grade"]
    end

    subgraph EXTERNAL["External Feed APIs"]
        D1["📱 Marketplace Catalogue API\nstaging-marketplace.toki.mn\nHandsets · Tablets · Watches\nEarphones · Accessories · CPE"]
        D2["🔗 Toki Shop Feed\n10.21.60.94:9000\nPre-built taxon product slots"]
    end

    A1 & A2 & A3 & A4 & A5 -->|POST /api/v1/events\nstream in real-time| ENG
    B1 -->|background poller| ENG
    C1 -->|sync every 10 min| ENG
    D1 -->|handset & accessory endpoints| ENG
    D2 -->|category page endpoint| ENG

    ENG(["🧠 Engine\nv4.2.0"])
```

### Signal Weighting — Not All Events Are Equal

| Signal | Weight | Meaning |
|--------|--------|---------|
| ✅ Order Completed | **5.0** | Strongest purchase intent |
| 📋 Lease Limit Checked | **4.0** | Serious buyer signal |
| 🛒 Add to Cart | **3.5** | High active interest |
| ❤️ Add to Wishlist | **3.0** | Notable interest |
| 👆 Product Click | **1.5** | Mild interest |
| 👁️ Product View | **1.0** | Passive browsing |
| 🗂️ Category Click | **0.5** | Exploration only |
| ❌ Cart Remove | **−1.5** | Negative signal |

> **Recency matters.** All signals decay with a **7-day half-life** — an order from yesterday matters far more than one from three weeks ago.

---

## 04 · How the Algorithms Work

Three independent retrieval engines run in **parallel**, then their ranked lists get fused.

```mermaid
flowchart TD
    USER(["👤 User\naccount_id"])

    USER --> CBF
    USER --> CF
    USER --> POP

    CBF["📄 Content-Based Filtering\n\nFinds products whose text description\n(name · specs · keywords · category)\nis most similar to what you've engaged\nwith — using TF-IDF cosine similarity\n30,000-token vocabulary · bigrams"]

    CF["👥 Collaborative Filtering\n\nFinds other shoppers who touched\nthe same products as you, then\nsurfaces what they also bought\nor liked — item-based co-interaction\nMax 200 co-users per seed product"]

    POP["🔥 Popularity Fallback\n\nGlobal + category-scoped\ntrending products ranked by\ntotal interaction weight\nCold-start safety net"]

    CBF -->|ranked list| RRF
    CF -->|ranked list| RRF
    POP -->|ranked list| RRF

    RRF["⚖️ RRF Fusion\nReciprocal Rank Fusion · K = 60\n\nCombines all three lists into\none optimised ranking — rewards\nproducts that rank well across\nmultiple algorithms consistently"]
```

### RRF Algorithm Weights Per Placement

| Placement | CBF | CF | Popularity |
|-----------|-----|----|------------|
| 🗂️ Category page | 45 % | 30 % | 25 % |
| 🔍 Product detail | 60 % | 40 % | — |
| 🧺 Basket cross-sell | 40 % | 35 % | 25 % |
| 📱 Handset accessories | 60 % | 40 % | fallback only |
| 📡 Handset device feed | 45 % | — | 55 % |

---

## 05 · Context Re-ranking — The Final Touch

After candidate generation and RRF merge, five re-ranking passes shape every final result:

```mermaid
flowchart LR
    MERGED["Merged\nCandidate List"]

    MERGED --> R1["📍 Session Boost\n+25% for products whose\ncategory you just browsed\n(last 3 taxons in path)"]
    R1 --> R2["🧺 Basket Awareness\nSame-category penalty × 0.6\nDifferent-taxon boost × 1.15"]
    R2 --> R3["💰 Budget Filter\nLease limit checked?\nDemote price-tier > 1 by × 0.5"]
    R3 --> R4["🎲 Intent Diversity\nIntent ≥ 8.0 → stay focused\nIntent ≤ 2.0 → inject variety"]
    R4 --> R5["🏪 Shop Cap\nMax 5 products per shop\n(3 on basket panel)"]
    R5 --> FINAL["✅ Final\nRecommendations"]
```

---

## 06 · Five Placements, One Engine

```mermaid
graph LR
    ENG(["🧠 Engine\nv4.2.0"])

    ENG -->|Category page\nProduct grid| TP["🗂️ Taxon Page\n\nToki shop feed fills first\nCore CBF + CF + Popularity\nfills remainder\n45% · 30% · 25%"]

    ENG -->|Product detail page\n'You May Also Like'| PP["🔍 Product Page\n\nTF-IDF cosine similarity\n+ CF co-interaction\n60% · 40%"]

    ENG -->|Basket / Cart view\nCross-sell panel| BP["🧺 Basket Page\n\nCross-category boost\nCBF from all basket seeds\n40% · 35% · 25%"]

    ENG -->|Proactive push\nto shop feed URL| FP["📡 Feed Push\n\nMulti-taxon feed per user\nTop 3 categories · 10 products\nPOSTed to shop endpoint"]

    ENG -->|Phone · tablet · device page\nAccessories & device feed| HP["📱 Handset Pipeline\n\n① Compatible accessories\n   cases · chargers · earphones\n② Multi-taxon device feed\n   phones · tablets · watches"]
```

---

## 07 · The Handset & Device Pipeline (New)

The handset pipeline connects to the **external Marketplace Catalogue API** and layers it with the internal recommendation engine — giving device pages a dedicated, deeply personalised experience.

```mermaid
flowchart TD
    subgraph HSOURCES["Handset Data Sources"]
        HA["📱 Marketplace Catalogue API\nstaging-marketplace.toki.mn\nPer-user · cached 1 hour"]
        HB["🧠 Internal Engine\nCBF + CF + Popularity"]
    end

    subgraph HTAXONS["Device Category Slots"]
        T1["📱 handset-cellphone\nSmartphones"]
        T2["📟 tablet\nTablets"]
        T3["⌚ watch-and-smart-watches\nWearables"]
        T4["🎧 headphones-earphones\nEarphones & Headphones"]
        T5["🔌 handset-accessory\nCases · Chargers · Bands"]
        T6["📡 cpe\nRouters · Modems"]
    end

    subgraph HENDPOINTS["Endpoints"]
        E1["POST /recommendations/handset/accessories\nFor a specific phone product page\nReturns compatible accessories"]
        E2["POST /recommendations/handset/feed\nPer-user full device category feed\nAll 6 taxon slots"]
    end

    HA -->|API products fill slots first| T1 & T2 & T3 & T4 & T5 & T6
    HB -->|fills remaining slots| T1 & T2 & T3 & T4 & T5 & T6
    T1 & T2 & T3 & T4 & T5 & T6 --> E1
    T1 & T2 & T3 & T4 & T5 & T6 --> E2
```

**How accessory recommendations work for a phone page:**

1. Fetch this user's handset feed from the Marketplace Catalogue API (cached 1 h)
2. Pull accessory, earphone, and wearable slots → these products are shown first
3. Run TF-IDF CBF from the phone's content vector, **scoped to accessory taxons only**
4. Run CF co-purchase signals for that phone, scoped to accessories
5. RRF merge the internal results, apply re-rankers, fill remaining slots
6. Tag each result with source: `marketplace_api` / `internal` / `mixed`

---

## 08 · What the Responses Look Like

### Placement Response (category · product · basket · handset accessories)

```json
{
  "account_id":   "6800a429c127d95ecd882dd1",
  "placement":    "handset_accessories",
  "recommendations": [
    "68d3cd43d36b9be827b44e06",
    "68d3cd43d36b9be827b44e07",
    "68d3cd43d36b9be827b44e08"
  ],
  "strategy":          "marketplace_api+accessories_cbf+cf",
  "intent_score":      7.4,
  "device":            "mobile",
  "count":             10,
  "context_product_id": "68d3cd43d36b9be827b44e00",
  "served_at":         "2026-08-18T10:45:00Z"
}
```

### Handset Device Feed Response

```json
{
  "account_id": "6800a429c127d95ecd882dd1",
  "taxon_feeds": [
    { "taxon_slug": "handset-cellphone",  "count": 10, "source": "marketplace_api" },
    { "taxon_slug": "handset-accessory",  "count": 10, "source": "mixed"           },
    { "taxon_slug": "headphones-earphones","count": 8, "source": "internal"        }
  ],
  "total_products": 28,
  "strategy":       "marketplace_api+cbf+pop",
  "intent_score":   5.2,
  "device":         "miniprogram",
  "served_at":      "2026-08-18T10:45:00Z"
}
```

| Field | Meaning |
|-------|---------|
| `recommendations` | Ordered product IDs to display |
| `strategy` | Which pipelines contributed: `toki+cbf+cf+pop`, `marketplace_api+accessories_cbf+cf`, etc. |
| `intent_score` | User engagement level (sum of weighted events, 7-day decay) |
| `source` | Handset feed only: `marketplace_api` · `internal` · `mixed` |
| `device` | `mobile` · `desktop` · `miniprogram` · `unknown` |
| `count` | Product count — adapts per device type |

### Device-Aware Result Size

| Device | Products Returned |
|--------|------------------|
| 📱 Miniprogram | 8 |
| 📱 Mobile | 12 |
| 🖥️ Desktop | 20 |
| ❓ Unknown | 15 |

---

## 09 · All API Endpoints at a Glance

```mermaid
graph LR
    subgraph INGEST["Event Ingestion"]
        I1["POST /api/v1/events\nShop activity stream"]
        I2["POST /api/v1/consumer-events\nOracle consumer events"]
    end

    subgraph INFERENCE["On-Demand Inference"]
        N1["POST /api/v1/infer\nSingle-taxon recommendation"]
        N2["POST /api/v1/feed\nMulti-taxon feed"]
        N3["POST /api/v1/feed/push\nGenerate + push to shop"]
    end

    subgraph PLACEMENTS2["Placement Endpoints"]
        L1["POST /recommendations/taxon\nCategory page grid"]
        L2["POST /recommendations/product\nProduct detail panel"]
        L3["POST /recommendations/basket\nCart cross-sell"]
        L4["POST /recommendations/handset/accessories\nPhone page accessories"]
        L5["POST /recommendations/handset/feed\nDevice category feed"]
    end

    subgraph OPS["Operations"]
        O1["GET /api/v1/health\nLiveness check"]
        O2["GET /api/v1/catalog/status\nSync state + TF-IDF stats"]
        O3["POST /api/v1/catalog/sync\nManual re-sync trigger"]
        O4["GET /api/v1/metrics\nAggregated worker metrics"]
    end
```

---

## 10 · Full Data Journey — End to End

```mermaid
sequenceDiagram
    actor Shopper
    participant Shop as 🛒 Marketplace Shop
    participant API as 🔌 Recommendation API
    participant FS as 🧠 Feature Store
    participant ALG as ⚙️ Algorithms
    participant EXT as 📱 External Feeds
    participant DB as 🗄️ PostgreSQL / Oracle

    Shopper->>Shop: Browse · Click · Add to cart
    Shop->>API: POST /events (real-time stream)
    API->>FS: Update user session & intent scores
    FS-->>API: Scores updated

    Note over API,ALG: Inline rec generation (≤ 8 s timeout)

    API->>ALG: Generate recommendations
    ALG->>FS: Fetch user top products (seeds)
    ALG->>FS: Fetch catalog TF-IDF index
    ALG->>FS: Fetch co-interaction data
    ALG->>EXT: Fetch Toki / Marketplace API feed
    EXT-->>ALG: Pre-built product slots
    ALG-->>API: Merged + re-ranked list
    API->>DB: Log delivery record (async, every 5 s)
    API-->>Shop: Recommendations response

    Note over DB,FS: Background jobs (always running)
    DB->>FS: Catalog sync every 10 min
    DB->>FS: Oracle poll every 60 s · batch 500 rows
```

---

## 11 · Infrastructure Snapshot

```mermaid
graph TD
    subgraph RUNTIME["App Server · Production  80GB RAM · 32 CPU · 1TB Storage"]
        GUN["Gunicorn\n24 workers · port 8018\nmax 5,000 requests/worker"]
        UVW["Uvicorn Workers\nasync I/O · 1,000 peak users"]
        MEM["In-Memory Feature Store\nSession state · TF-IDF index\n~8 MB sparse after compression"]
        GUN --> UVW
        UVW <--> MEM
    end

    subgraph PGSERVER["PostgreSQL Server  8 CPU · 16GB RAM · ~150GB used"]
        PGP["Production DB\nmarketplace\nCatalog v3 · Delivery logs"]
        PGS["Staging DB\nmarketplace_staging\nCatalog v3_staging · Test logs"]
    end

    subgraph EXTSTORAGE["External Data"]
        ORA["Oracle DB\nConsumer event history\n24 h lookback · MN timezone"]
    end

    subgraph EXTAPIS["External Feed APIs"]
        TF["Toki Shop Feed\n10.21.60.94:9000\nCache 120 s · 20,000 users"]
        MKT["Marketplace Catalogue API\nstaging-marketplace.toki.mn\nCache 3,600 s · 10,000 users"]
    end

    subgraph JOBS["Background Jobs (per worker)"]
        CS["Catalog Sync\nevery 10 min"]
        OP["Oracle Poller\nevery 60 s"]
        DQ["Delivery Log Writer\nevery 5 s · batch 200"]
    end

    UVW <-.->|timeout 1 s| TF
    UVW <-.->|timeout 2 s| MKT
    CS --> PGP
    DQ --> PGP
    OP --> ORA
    CS --> MEM
    OP --> MEM
```

| Component | Detail |
|-----------|--------|
| **Production Server** | 80 GB RAM · 32 CPU cores · 1 TB storage |
| **API Stack** | FastAPI + Gunicorn, 24 async Uvicorn workers |
| **PostgreSQL Server** | 8 CPU · 16 GB RAM · ~150 GB currently in use |
| **PostgreSQL Environments** | Single server, two environments — production (`marketplace`) and staging (`marketplace_staging`) separated by database name and table suffix (`_staging`, `_archived_*`) |
| **In-Memory Store** | TF-IDF matrix + all user sessions + popularity — ~8 MB compressed sparse |
| **Catalog** | 50,000+ products · 30,000-token vocabulary · bigrams |
| **External APIs** | Toki shop feed (1 s timeout) + Marketplace catalogue API (2 s timeout) — both gracefully degraded |
| **Cold Start** | Popularity-based fallback; catalog quality score for brand-new products |
| **Monitoring** | `/api/v1/health` · `/api/v1/catalog/status` · `/api/v1/metrics` |

---

## 12 · Key Business Metrics Tracked

| Metric | What It Tells Us |
|--------|-----------------|
| `recs_served` | Total recommendations delivered across all placements |
| `by_strategy` | How often each algorithm mix wins (hybrid vs pure CBF vs popular) |
| `by_endpoint` | Which placements drive the most calls (taxon vs handset vs basket) |
| `by_device` | Where shoppers are: mobile, miniprogram, desktop |
| `events_processed` | Volume of real-time behavioural signal being consumed |
| `consumer_events_processed` | Oracle historical events ingested per poll cycle |
| `infer_timeouts` | System pressure indicator — recs dropped due to time budget |
| `by_activity` | Breakdown of signal types: orders, carts, wishlists, views |

> Metrics are aggregated every 10 seconds across all 24 workers and available via `GET /api/v1/metrics`.

---

## Summary

```mermaid
mindmap
  root((TOKI Rec Engine v4.2))
    Data In
      Shop stream events
      Oracle event history
      PostgreSQL catalog
      Marketplace Catalogue API
      Toki Shop Feed
    Algorithms
      Content-Based Filtering
      Collaborative Filtering
      Popularity Fallback
      RRF Fusion
    Personalization
      Session taxon path
      Basket awareness
      Budget signals
      Intent level
      Device adaptation
    Placements
      Category page grid
      Product page panel
      Basket cross-sell
      Proactive feed push
      Handset accessories
      Device category feed
    Output
      Ordered product IDs
      Strategy metadata
      Intent score
      Source tagging
      Device-tuned count
```

---

*TOKI Marketplace Recommendation Engine · v4.2.0 · Internal Presentation · 2026-08-18*
