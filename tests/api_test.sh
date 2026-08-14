#!/usr/bin/env bash
# End-to-end API test for the TOKI Recommendation Engine.
# Usage: bash tests/api_test.sh [BASE_URL] [API_KEY]
#   BASE_URL defaults to http://10.22.4.13:8018
#   API_KEY  defaults to toki-internal-key
set -e

BASE="${1:-http://10.22.4.13:8018}"
KEY="${2:-toki-internal-key}"

USER1="6a5e47214aeec353171ccaa0"
USER2="66fbc5824e022311128232ae"
PRODUCT1="6989774f3516dac1b3e979ee"
TAXON1="69fbef9bda75a61ceadc7607"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

pass=0
fail=0

_check() {
    local label="$1"
    local code="$2"
    if [ "$code" = "200" ] || [ "$code" = "202" ]; then
        echo "  [PASS] $label (HTTP $code)"
        ((pass++)) || true
    else
        echo "  [FAIL] $label (HTTP $code)"
        ((fail++)) || true
    fi
}

echo "========================================"
echo " TOKI Recommendation Engine — API Tests"
echo " Base: $BASE"
echo "========================================"

# ── 1. Health (no auth) ────────────────────────────────────────────────────────
echo ""
echo "── 1. Health check ──"
RESP=$(curl -s -w "\n%{http_code}" "$BASE/api/v1/health")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
_check "GET /api/v1/health" "$CODE"
echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"

# ── 2. view_product event ─────────────────────────────────────────────────────
echo ""
echo "── 2. Event ingestion — view_product ──"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/v1/events" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"shop_id\": \"antmall\",
    \"events\": [{
      \"event_id\": \"test-view-001\",
      \"account_id\": \"$USER1\",
      \"session_id\": \"sess_test_001\",
      \"activity_name\": \"view_product\",
      \"activity_data\": {
        \"accountid\": \"$USER1\",
        \"productid\": \"$PRODUCT1\",
        \"action\": \"view\"
      },
      \"user_agent\": \"Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X)\",
      \"timestamp\": \"$NOW\"
    }]
  }")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
_check "POST /api/v1/events (view_product)" "$CODE"
echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"

# ── 3. cart-events add ────────────────────────────────────────────────────────
echo ""
echo "── 3. Event ingestion — cart-events (add) ──"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/v1/events" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"events\": [{
      \"account_id\": \"$USER1\",
      \"session_id\": \"sess_test_001\",
      \"activity_name\": \"cart-events\",
      \"activity_data\": {
        \"accountid\": \"$USER1\",
        \"productid\": \"$PRODUCT1\",
        \"action\": \"add\",
        \"quantity\": 1
      },
      \"user_agent\": \"Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X)\"
    }]
  }")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
_check "POST /api/v1/events (cart-events add)" "$CODE"
echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"

# ── 4. limit-events ───────────────────────────────────────────────────────────
echo ""
echo "── 4. Event ingestion — limit-events ──"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/v1/events" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"events\": [{
      \"account_id\": \"$USER2\",
      \"session_id\": \"sess_test_002\",
      \"activity_name\": \"limit-events\",
      \"activity_data\": {
        \"accountid\": \"$USER2\",
        \"action\": \"check\"
      },
      \"user_agent\": \"Mozilla/5.0 (Linux; Android 14)\"
    }]
  }")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
_check "POST /api/v1/events (limit-events)" "$CODE"
echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"

# ── 5. Oracle consumer-events ─────────────────────────────────────────────────
echo ""
echo "── 5. Oracle consumer-events ──"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/v1/consumer-events" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"events\": [
      {
        \"EVENTNAME\": \"product_click\",
        \"EVENTVALUE\": \"{\\\"productIds\\\": [\\\"69fc469bab34c8d11412ec79\\\"], \\\"taxon\\\": {\\\"label\\\": \\\"Үснийхэрэгсэл\\\"}}\",
        \"ACCOUNTID\": \"$USER2\",
        \"SESSIONID\": \"sess_oracle_001\",
        \"TIMESTAMP_\": \"$NOW\",
        \"USERAGENT\": \"Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X)\"
      },
      {
        \"EVENTNAME\": \"taxon_click\",
        \"EVENTVALUE\": \"{\\\"taxon\\\": {\\\"label\\\": \\\"Гар утас\\\"}}\",
        \"ACCOUNTID\": \"$USER1\",
        \"SESSIONID\": \"sess_oracle_002\",
        \"TIMESTAMP_\": \"$NOW\",
        \"USERAGENT\": \"Mozilla/5.0 (iPhone; CPU iPhone OS 18_6 like Mac OS X)\"
      }
    ]
  }")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
_check "POST /api/v1/consumer-events" "$CODE"
echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"

# ── 6. Taxon page recommendations ─────────────────────────────────────────────
echo ""
echo "── 6. Recommendation — taxon page ──"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/v1/recommendations/taxon" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"account_id\": \"$USER1\",
    \"taxon_id\": \"$TAXON1\",
    \"top_n\": 10
  }")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
_check "POST /api/v1/recommendations/taxon" "$CODE"
echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"

# ── 7. Product page recommendations ───────────────────────────────────────────
echo ""
echo "── 7. Recommendation — product page ──"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/v1/recommendations/product" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"account_id\": \"$USER1\",
    \"product_id\": \"$PRODUCT1\",
    \"top_n\": 8
  }")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
_check "POST /api/v1/recommendations/product" "$CODE"
echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"

# ── 8. Basket cross-sell ──────────────────────────────────────────────────────
echo ""
echo "── 8. Recommendation — basket cross-sell ──"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/v1/recommendations/basket" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"account_id\": \"$USER1\",
    \"basket_product_ids\": [\"$PRODUCT1\"],
    \"top_n\": 6
  }")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
_check "POST /api/v1/recommendations/basket" "$CODE"
echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"

# ── 9. Multi-taxon feed ───────────────────────────────────────────────────────
echo ""
echo "── 9. Multi-taxon feed ──"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/v1/feed" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"account_id\": \"$USER1\",
    \"top_taxons\": 3,
    \"top_n_per_taxon\": 8
  }")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
_check "POST /api/v1/feed" "$CODE"
echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"

# ── 10. On-demand infer ───────────────────────────────────────────────────────
echo ""
echo "── 10. On-demand infer ──"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/v1/infer" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $KEY" \
  -d "{
    \"account_id\": \"$USER1\",
    \"context\": {
      \"current_taxon_id\": \"$TAXON1\",
      \"device_type\": \"mobile\"
    },
    \"top_n\": 10
  }")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
_check "POST /api/v1/infer" "$CODE"
echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"

# ── 11. Catalog status ────────────────────────────────────────────────────────
echo ""
echo "── 11. Catalog status ──"
RESP=$(curl -s -w "\n%{http_code}" "$BASE/api/v1/catalog/status" \
  -H "X-API-Key: $KEY")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
_check "GET /api/v1/catalog/status" "$CODE"
echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"

# ── 12. Manual catalog sync ───────────────────────────────────────────────────
echo ""
echo "── 12. Manual catalog sync ──"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/v1/catalog/sync" \
  -H "X-API-Key: $KEY")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
_check "POST /api/v1/catalog/sync" "$CODE"
echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"

# ── 13. Auth guard — invalid key returns 401 ─────────────────────────────────
echo ""
echo "── 13. Auth guard ──"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/v1/events" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: invalid-key" \
  -d '{"events":[]}')
CODE=$(echo "$RESP" | tail -1)
if [ "$CODE" = "401" ]; then
    echo "  [PASS] Invalid key rejected (HTTP 401)"
    ((pass++)) || true
else
    echo "  [FAIL] Expected 401 for invalid key, got HTTP $CODE"
    ((fail++)) || true
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "========================================"
echo " Results: $pass passed, $fail failed"
echo "========================================"
[ "$fail" -eq 0 ] && exit 0 || exit 1
