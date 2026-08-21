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
        S1["🛒 Shop Activity Stream<br/>Cart - Orders - Wishlists - Views"]
        S2["🗄️ Oracle Event Warehouse<br/>Historical consumer events, 30 days"]
        S3["📦 PostgreSQL Catalog<br/>Products, Taxons, Stock, Prices"]
        S4["📱 Marketplace Catalogue API<br/>Handset device feed, per-user"]
        S5["🔗 Toki Shop Feed<br/>Pre-computed taxon slots, per-user"]
    end

    subgraph ENGINE["Recommendation Engine"]
        FE["🧠 Feature Store<br/>In-memory, Per-user, Real-time"]
        AL["⚙️ Algorithm Layer<br/>CBF, CF, Popularity, RRF Fusion"]
        RR["🎯 Re-ranker<br/>Context, Basket, Budget, Diversity"]
        FE --> AL --> RR
    end

    subgraph PLACEMENTS["Placements"]
        P1["🗂️ Category Page<br/>Product Grid"]
        P2["🔍 Product Page<br/>You May Also Like"]
        P3["🧺 Basket Page<br/>Cross-Sell"]
        P4["📡 Feed Push<br/>Multi-taxon Feed"]
        P5["📱 Handset Page<br/>Accessories + Device Feed"]
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
        B1["Consumer Event History<br/>30 days · millions of rows<br/>poll every 60 s · batch 500"]
    end

    subgraph POSTGRES["PostgreSQL Catalog"]
        C1["4,914 Products · 79 taxons<br/>Prices · Stock · Categories<br/>Specs · Keywords · Grade"]
    end

    subgraph EXTERNAL["Upstream Feed APIs (10.21.60.94)"]
        D1["📱 Marketplace Catalog API :9000<br/>GET /marketplace/{accountId}<br/>Handsets · Tablets · Watches<br/>Earphones · Accessories · CPE"]
        D2["🔗 TOKI Shop Feed :8018<br/>GET /api/recommendations/{accountId}<br/>Legacy demographic model<br/>Full ~80-taxon coverage"]
    end

    A1 & A2 & A3 & A4 & A5 -->|"POST /api/v1/events<br/>stream in real-time"| ENG
    B1 -->|background poller| ENG
    C1 -->|sync every 10 min| ENG
    D1 -->|prebuilt device slots| ENG
    D2 -->|full taxonomy + cold-start seeds| ENG
    ENG -->|"POST /ms/catalogue/v1/recommendation<br/>Bearer auth"| OUT["🛍️ marketplace.toki.mn"]

    ENG(["🧠 Engine<br/>v4.3.0"])
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
    USER(["👤 User<br/>account_id"])

    USER --> CBF
    USER --> CF
    USER --> POP

    CBF["📄 Content-Based Filtering<br/><br/>Finds products whose text description<br/>(name · specs · keywords · category)<br/>is most similar to what you've engaged<br/>with — using TF-IDF cosine similarity<br/>30,000-token vocabulary · bigrams"]

    CF["👥 Collaborative Filtering<br/><br/>Finds other shoppers who touched<br/>the same products as you, then<br/>surfaces what they also bought<br/>or liked — item-based co-interaction<br/>Max 200 co-users per seed product"]

    POP["🔥 Popularity Fallback<br/><br/>Global + category-scoped<br/>trending products ranked by<br/>total interaction weight<br/>Cold-start safety net"]

    CBF -->|ranked list| RRF
    CF -->|ranked list| RRF
    POP -->|ranked list| RRF

    RRF["⚖️ RRF Fusion<br/>Reciprocal Rank Fusion · K = 60<br/><br/>Combines all three lists into<br/>one optimised ranking — rewards<br/>products that rank well across<br/>multiple algorithms consistently"]
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
    MERGED["Merged<br/>Candidate List"]

    MERGED --> R1["📍 Session Boost<br/>+25% for products whose<br/>category you just browsed<br/>(last 3 taxons in path)"]
    R1 --> R2["🧺 Basket Awareness<br/>Same-category penalty × 0.6<br/>Different-taxon boost × 1.15"]
    R2 --> R3["💰 Budget Filter<br/>Lease limit checked?<br/>Demote price-tier > 1 by × 0.5"]
    R3 --> R4["🎲 Intent Diversity<br/>Intent ≥ 8.0 → stay focused<br/>Intent ≤ 2.0 → inject variety"]
    R4 --> R5["🏪 Shop Cap<br/>Max 5 products per shop<br/>(3 on basket panel)"]
    R5 --> FINAL["✅ Final<br/>Recommendations"]
```

---

## 06 · Five Placements, One Engine

```mermaid
graph LR
    ENG(["🧠 Engine<br/>v4.2.0"])

    ENG -->|"Category page<br/>Product grid"| TP["🗂️ Taxon Page<br/><br/>Toki shop feed fills first<br/>Core CBF + CF + Popularity<br/>fills remainder<br/>45% · 30% · 25%"]

    ENG -->|"Product detail page<br/>'You May Also Like'"| PP["🔍 Product Page<br/><br/>TF-IDF cosine similarity<br/>+ CF co-interaction<br/>60% · 40%"]

    ENG -->|"Basket / Cart view<br/>Cross-sell panel"| BP["🧺 Basket Page<br/><br/>Cross-category boost<br/>CBF from all basket seeds<br/>40% · 35% · 25%"]

    ENG -->|"Proactive push<br/>to shop feed URL"| FP["📡 Feed Push<br/><br/>Multi-taxon feed per user<br/>Top 3 categories · 10 products<br/>POSTed to shop endpoint"]

    ENG -->|"Phone · tablet · device page<br/>Accessories & device feed"| HP["📱 Handset Pipeline<br/><br/>① Compatible accessories<br/>&nbsp;&nbsp;&nbsp;cases · chargers · earphones<br/>② Multi-taxon device feed<br/>&nbsp;&nbsp;&nbsp;phones · tablets · watches"]
```

---

## 07 · The Handset & Device Pipeline (New)

The handset pipeline connects to the **Marketplace Catalog API on port 9000** and layers it with the internal recommendation engine — giving device pages a dedicated, deeply personalised experience. Categories outside the six device taxons fall through to the **TOKI Shop Feed on port 8018**, which covers the whole catalogue.

Both upstreams return the same envelope:

```json
{ "userId": "...", "taxonRecommendations": { "handset-cellphone": ["<productId>", ...] } }
```

Every product ID from either feed is checked against the synced catalog before it
is served — roughly 9% of catalog-feed IDs reference delisted SKUs.

```mermaid
flowchart TD
    subgraph HSOURCES["Handset Data Sources"]
        HA["📱 Marketplace Catalog API<br/>10.21.60.94:9000<br/>Per-user · cached 1 hour"]
        HS["🔗 TOKI Shop Feed<br/>10.21.60.94:8018<br/>Per-user · cached 10 min"]
        HB["🧠 Internal Engine<br/>CBF + CF + Popularity"]
    end

    subgraph HTAXONS["Device Category Slots"]
        T1["📱 handset-cellphone<br/>Smartphones"]
        T2["📟 tablet<br/>Tablets"]
        T3["⌚ watch-and-smart-watches<br/>Wearables"]
        T4["🎧 headphones-earphones<br/>Earphones & Headphones"]
        T5["🔌 handset-accessory<br/>Cases · Chargers · Bands"]
        T6["📡 cpe<br/>Routers · Modems"]
    end

    subgraph HENDPOINTS["Endpoints"]
        E1["POST /recommendations/handset/accessories<br/>For a specific phone product page<br/>Returns compatible accessories"]
        E2["POST /recommendations/handset/feed<br/>Per-user full device category feed<br/>All 6 taxon slots"]
    end

    HA -->|fills slots first| T1 & T2 & T3 & T4 & T5 & T6
    HS -->|fills what :9000 missed| T1 & T2 & T3 & T4 & T5 & T6
    HB -->|fills remaining slots| T1 & T2 & T3 & T4 & T5 & T6
    T1 & T2 & T3 & T4 & T5 & T6 --> E1
    T1 & T2 & T3 & T4 & T5 & T6 --> E2
```

**How accessory recommendations work for a phone page:**

1. Fetch this user's device feed from the Marketplace Catalog API :9000 (cached 1 h)
2. Round-robin the accessory, earphone, and wearable slots → these are shown first
   (round-robin so one category cannot take every slot)
3. Run TF-IDF CBF from the phone's content vector, **scoped to accessory taxons only**
4. Run CF co-purchase signals for that phone, scoped to accessories
5. RRF merge the internal results, apply re-rankers, fill remaining slots
6. Tag each result with source: `catalog_api` / `shop_feed` / `catalog_api+shop_feed` / `internal` / `mixed`

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
        I1["POST /api/v1/events<br/>Shop activity stream"]
        I2["POST /api/v1/consumer-events<br/>Oracle consumer events"]
    end

    subgraph INFERENCE["On-Demand Inference"]
        N1["POST /api/v1/infer<br/>Single-taxon recommendation"]
        N2["POST /api/v1/feed<br/>Multi-taxon feed"]
        N3["POST /api/v1/feed/push<br/>Generate + push to shop"]
    end

    subgraph PLACEMENTS2["Placement Endpoints"]
        L1["POST /recommendations/taxon<br/>Category page grid"]
        L2["POST /recommendations/product<br/>Product detail panel"]
        L3["POST /recommendations/basket<br/>Cart cross-sell"]
        L4["POST /recommendations/handset/accessories<br/>Phone page accessories"]
        L5["POST /recommendations/handset/feed<br/>Device category feed"]
    end

    subgraph OPS["Operations"]
        O1["GET /api/v1/health<br/>Liveness check"]
        O2["GET /api/v1/catalog/status<br/>Sync state + TF-IDF stats"]
        O3["POST /api/v1/catalog/sync<br/>Manual re-sync trigger"]
        O4["GET /api/v1/metrics<br/>Aggregated worker metrics"]
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
        GUN["Gunicorn<br/>24 workers · port 8018<br/>max 5,000 requests/worker"]
        UVW["Uvicorn Workers<br/>async I/O · 1,000 peak users"]
        MEM["In-Memory Feature Store<br/>Session state · TF-IDF index<br/>~8 MB sparse after compression"]
        GUN --> UVW
        UVW <--> MEM
    end

    subgraph PGSERVER["PostgreSQL Server  8 CPU · 16GB RAM · ~150GB used"]
        PGP["Production DB<br/>marketplace<br/>Catalog v3 · Delivery logs"]
        PGS["Staging DB<br/>marketplace_staging<br/>Catalog v3_staging · Test logs"]
    end

    subgraph EXTSTORAGE["External Data"]
        ORA["Oracle DB<br/>Consumer event history<br/>24 h lookback · MN timezone"]
    end

    subgraph EXTAPIS["Upstream Feed APIs"]
        MKT["Marketplace Catalog API<br/>10.21.60.94:9000<br/>Cache 3,600 s · 20,000 users"]
        TF["TOKI Shop Feed<br/>10.21.60.94:8018<br/>Cache 600 s · 50,000 users"]
    end

    subgraph JOBS["Background Jobs (per worker)"]
        CS["Catalog Sync<br/>every 10 min"]
        OP["Oracle Poller<br/>every 60 s"]
        DQ["Delivery Log Writer<br/>every 5 s · batch 200"]
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
graph LR
    ROOT(["TOKI Rec Engine<br/>v4.2"])

    ROOT --> M1["Data In"]
    ROOT --> M2["Algorithms"]
    ROOT --> M3["Personalization"]
    ROOT --> M4["Placements"]
    ROOT --> M5["Output"]

    M1 --> M1A["Shop stream events"]
    M1 --> M1B["Oracle event history"]
    M1 --> M1C["PostgreSQL catalog"]
    M1 --> M1D["Marketplace Catalogue API"]
    M1 --> M1E["Toki Shop Feed"]

    M2 --> M2A["Content-Based Filtering"]
    M2 --> M2B["Collaborative Filtering"]
    M2 --> M2C["Popularity Fallback"]
    M2 --> M2D["RRF Fusion"]

    M3 --> M3A["Session taxon path"]
    M3 --> M3B["Basket awareness"]
    M3 --> M3C["Budget signals"]
    M3 --> M3D["Intent level"]
    M3 --> M3E["Device adaptation"]

    M4 --> M4A["Category page grid"]
    M4 --> M4B["Product page panel"]
    M4 --> M4C["Basket cross-sell"]
    M4 --> M4D["Proactive feed push"]
    M4 --> M4E["Handset accessories"]
    M4 --> M4F["Device category feed"]

    M5 --> M5A["Ordered product IDs"]
    M5 --> M5B["Strategy metadata"]
    M5 --> M5C["Intent score"]
    M5 --> M5D["Source tagging"]
    M5 --> M5E["Device-tuned count"]
```

---

*TOKI Marketplace Recommendation Engine · v4.2.0 · Internal Presentation · 2026-08-18*
