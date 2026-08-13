"""
Monitoring dashboard endpoints.

GET /api/v1/metrics   — live JSON metrics snapshot (no auth required)
GET /dashboard        — self-contained HTML live-monitoring dashboard (no auth required)
"""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from src.module.feature_store import store
from src.module.metrics import aggregate_all_workers

router = APIRouter(tags=["monitoring"])

# ── JSON metrics endpoint ──────────────────────────────────────────────────────


@router.get("/api/v1/metrics", summary="Live metrics snapshot", response_model=dict)
async def metrics_json() -> dict:
    """Aggregated ingestion/recommendation metrics across all gunicorn workers."""
    from src.api.routes.events import _DELIVERY_QUEUE

    snap = aggregate_all_workers()
    snap["delivery_queue_depth"] = len(_DELIVERY_QUEUE)
    snap["system"] = store.stats()
    return snap


# ── HTML dashboard ─────────────────────────────────────────────────────────────

_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TOKI Rec Engine · Monitor</title>
<link rel="stylesheet"
  href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
  crossorigin="anonymous">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"
  crossorigin="anonymous"></script>
<style>
  body{background:#0d1b2a;color:#cdd9e5;font-family:"Inter",system-ui,sans-serif}
  .card{background:#162032;border:1px solid #1f3048;border-radius:10px}
  .stat-val{font-size:2rem;font-weight:700;line-height:1.1}
  .stat-lbl{font-size:.72rem;text-transform:uppercase;letter-spacing:.07em;color:#6b8ba4}
  .ch-wrap{position:relative;height:170px}
  .table{color:#cdd9e5;font-size:.83rem}
  .table td,.table th{border-color:#1f3048;padding:.3rem .5rem}
  .table tbody tr:hover{background:#1a2d44}
  .badge-ok{background:#0a5c3b;color:#4fd1a1}
  .badge-err{background:#5c1a0a;color:#f87171}
  .pulse{animation:blink 2s step-start infinite}
  @keyframes blink{50%{opacity:.3}}
  ::-webkit-scrollbar{width:6px}
  ::-webkit-scrollbar-track{background:#0d1b2a}
  ::-webkit-scrollbar-thumb{background:#1f3048;border-radius:3px}
  .test-inp{background:#0d1b2a!important;color:#cdd9e5!important;border-color:#1f3048!important}
  .test-inp:focus{background:#0d1b2a!important;color:#cdd9e5!important;border-color:#3b82f6!important;box-shadow:0 0 0 .15rem rgba(59,130,246,.25)!important}
  .test-inp option{background:#162032;color:#cdd9e5}
  .form-label{color:#6b8ba4!important}
  .test-divider{border-color:#1f3048;margin:5px 0}
</style>
</head>
<body>
<div class="container-fluid px-3 py-3">

  <!-- Header -->
  <div class="d-flex justify-content-between align-items-center mb-3">
    <div>
      <span class="fw-bold fs-6 text-white">TOKI Recommendation Engine</span>
      <span class="text-secondary ms-2 small">· Monitoring Dashboard</span>
    </div>
    <div class="small text-secondary">
      <span class="pulse text-success me-1">&#9679;</span>
      Live · refreshes every 5 s &nbsp;|&nbsp; last: <span id="last-refresh">—</span>
    </div>
  </div>

  <!-- Stat cards -->
  <div class="row g-2 mb-3">
    <div class="col-6 col-sm-4 col-lg-2">
      <div class="card p-3 text-center h-100">
        <div class="stat-lbl">Events Ingested</div>
        <div class="stat-val text-primary" id="s-events">—</div>
        <div class="small text-secondary mt-1"><span id="s-batches">—</span> batches</div>
      </div>
    </div>
    <div class="col-6 col-sm-4 col-lg-2">
      <div class="card p-3 text-center h-100">
        <div class="stat-lbl">Consumer Events</div>
        <div class="stat-val text-info" id="s-consumer">—</div>
        <div class="small text-secondary mt-1"><span id="s-consumer-batches">—</span> batches</div>
      </div>
    </div>
    <div class="col-6 col-sm-4 col-lg-2">
      <div class="card p-3 text-center h-100">
        <div class="stat-lbl">Recs Served</div>
        <div class="stat-val text-success" id="s-recs">—</div>
        <div class="small text-secondary mt-1">total products</div>
      </div>
    </div>
    <div class="col-6 col-sm-4 col-lg-2">
      <div class="card p-3 text-center h-100">
        <div class="stat-lbl">Events Failed</div>
        <div class="stat-val text-danger" id="s-failed">—</div>
        <div class="small text-secondary mt-1">parse / apply errors</div>
      </div>
    </div>
    <div class="col-6 col-sm-4 col-lg-2">
      <div class="card p-3 text-center h-100">
        <div class="stat-lbl">Infer Timeouts</div>
        <div class="stat-val text-warning" id="s-timeouts">—</div>
        <div class="small text-secondary mt-1">users skipped inline</div>
      </div>
    </div>
    <div class="col-6 col-sm-4 col-lg-2">
      <div class="card p-3 text-center h-100">
        <div class="stat-lbl">Uptime</div>
        <div class="stat-val text-white" style="font-size:1.2rem;padding-top:.4rem" id="s-uptime">—</div>
        <div class="small text-secondary mt-1">queue: <span id="s-queue">—</span></div>
      </div>
    </div>
  </div>

  <!-- Charts + system -->
  <div class="row g-2 mb-3">
    <div class="col-md-5">
      <div class="card p-3 h-100">
        <div class="stat-lbl mb-2">Events Ingested / 10 s &nbsp;<span class="text-primary">(last 10 min)</span></div>
        <div class="ch-wrap"><canvas id="ch-events"></canvas></div>
      </div>
    </div>
    <div class="col-md-5">
      <div class="card p-3 h-100">
        <div class="stat-lbl mb-2">Recommendations Served / 10 s &nbsp;<span class="text-success">(last 10 min)</span></div>
        <div class="ch-wrap"><canvas id="ch-recs"></canvas></div>
      </div>
    </div>
    <div class="col-md-2">
      <div class="card p-3 h-100">
        <div class="stat-lbl mb-2">System</div>
        <table class="table table-sm mb-0">
          <tbody>
            <tr><td class="text-secondary">Catalog</td><td id="sys-catalog">—</td></tr>
            <tr><td class="text-secondary">Products</td><td id="sys-products">—</td></tr>
            <tr><td class="text-secondary">Sync age</td><td id="sys-sync">—</td></tr>
            <tr><td class="text-secondary">Sessions</td><td id="sys-sessions">—</td></tr>
            <tr><td class="text-secondary">Users</td><td id="sys-users">—</td></tr>
            <tr><td class="text-secondary">Taxons</td><td id="sys-taxons">—</td></tr>
            <tr><td class="text-secondary">Popularity</td><td id="sys-pop">—</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- Breakdown tables -->
  <div class="row g-2">
    <div class="col-6 col-md-3">
      <div class="card p-3">
        <div class="stat-lbl mb-2">Events by Activity</div>
        <table class="table table-sm mb-0">
          <tbody id="t-activity"></tbody>
        </table>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="card p-3">
        <div class="stat-lbl mb-2">Recs by Strategy</div>
        <table class="table table-sm mb-0">
          <tbody id="t-strategy"></tbody>
        </table>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="card p-3">
        <div class="stat-lbl mb-2">Recs by Device</div>
        <table class="table table-sm mb-0">
          <tbody id="t-device"></tbody>
        </table>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="card p-3">
        <div class="stat-lbl mb-2">Recs by Endpoint</div>
        <table class="table table-sm mb-0">
          <tbody id="t-endpoint"></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- ── Live Logs ──────────────────────────────────────────────────────────── -->
  <div class="row g-2 mt-2">
    <div class="col-md-6">
      <div class="card p-3" style="max-height:260px;overflow:hidden;display:flex;flex-direction:column">
        <div class="d-flex justify-content-between align-items-center mb-1">
          <span class="stat-lbl">Ingest Log</span>
          <span id="ingest-total" class="small text-secondary"></span>
        </div>
        <div id="log-ingest" style="overflow-y:auto;flex:1;font-family:monospace;font-size:.7rem;color:#8babbf;line-height:1.55"></div>
      </div>
    </div>
    <div class="col-md-6">
      <div class="card p-3" style="max-height:260px;overflow:hidden;display:flex;flex-direction:column">
        <div class="d-flex justify-content-between align-items-center mb-1">
          <span class="stat-lbl">Push Log &mdash; Marketplace</span>
          <span id="push-total" class="small text-secondary"></span>
        </div>
        <div id="log-push" style="overflow-y:auto;flex:1;font-family:monospace;font-size:.7rem;color:#8babbf;line-height:1.55"></div>
      </div>
    </div>
  </div>

  <!-- ── Live API Testing ──────────────────────────────────────────────────── -->
  <div class="mt-3">
    <div class="d-flex flex-wrap align-items-center gap-3 mb-2">
      <span class="fw-bold text-white">Live API Testing</span>
      <span class="small text-secondary">Results appear inline · no page reload needed</span>
    </div>
    <div class="row g-2">

      <!-- Test 1: User Feed -->
      <div class="col-lg-4">
        <div class="card p-3">
          <div class="stat-lbl mb-2">&#9312; Multi-Taxon Feed &mdash; by User ID</div>
          <div class="mb-2">
            <label class="form-label small mb-1">User ID</label>
            <input id="t1-uid" class="form-control form-control-sm test-inp"
              placeholder="e.g. 66fbc5824e022311128232ae">
          </div>
          <div class="row g-2 mb-2">
            <div class="col">
              <label class="form-label small mb-1">Top Taxons</label>
              <input id="t1-taxons" class="form-control form-control-sm test-inp"
                type="number" value="3" min="1" max="10">
            </div>
            <div class="col">
              <label class="form-label small mb-1">Products / Taxon</label>
              <input id="t1-ppt" class="form-control form-control-sm test-inp"
                type="number" value="8" min="1" max="30">
            </div>
          </div>
          <button onclick="runFeedTest()" class="btn btn-sm btn-primary w-100 mb-2">Get Feed &#9654;</button>
          <div id="t1-result" style="display:none"></div>
        </div>
      </div>

      <!-- Test 2: Session / Consumer Event -->
      <div class="col-lg-4">
        <div class="card p-3">
          <div class="stat-lbl mb-2">&#9313; Consumer Event &mdash; by Session ID</div>
          <div class="mb-2">
            <label class="form-label small mb-1">Account ID</label>
            <input id="t2-uid" class="form-control form-control-sm test-inp"
              placeholder="e.g. 66fbc5824e022311128232ae">
          </div>
          <div class="mb-2">
            <label class="form-label small mb-1">Session ID</label>
            <input id="t2-sid" class="form-control form-control-sm test-inp"
              placeholder="e.g. jPAaTyDWFjD1JsHyR0ux3hewNYRvNvRy">
          </div>
          <div class="mb-2">
            <label class="form-label small mb-1">Event Name</label>
            <select id="t2-ename" class="form-select form-select-sm test-inp">
              <option>product_click</option>
              <option>taxon_click</option>
              <option>order-events</option>
              <option>wishlist-events</option>
              <option>cart-events</option>
              <option>limit-events</option>
            </select>
          </div>
          <div class="mb-2">
            <label class="form-label small mb-1">Product ID or Taxon Label</label>
            <input id="t2-eval" class="form-control form-control-sm test-inp"
              placeholder="e.g. 69fc469bab34c8d11412ec79">
          </div>
          <button onclick="runSessionTest()" class="btn btn-sm btn-info w-100 mb-2">Ingest &amp; Infer &#9654;</button>
          <div id="t2-result" style="display:none"></div>
        </div>
      </div>

      <!-- Test 3: Product Similarity -->
      <div class="col-lg-4">
        <div class="card p-3">
          <div class="stat-lbl mb-2">&#9314; Product Similarity &mdash; &ldquo;You May Also Like&rdquo;</div>
          <div class="mb-2">
            <label class="form-label small mb-1">Anchor Product ID</label>
            <input id="t3-pid" class="form-control form-control-sm test-inp"
              placeholder="e.g. 69fc469bab34c8d11412ec79">
          </div>
          <div class="mb-2">
            <label class="form-label small mb-1">Account ID <span class="text-secondary">(optional)</span></label>
            <input id="t3-uid" class="form-control form-control-sm test-inp"
              placeholder="leave blank for anonymous">
          </div>
          <div class="mb-2">
            <label class="form-label small mb-1">Top N</label>
            <input id="t3-topn" class="form-control form-control-sm test-inp"
              type="number" value="10" min="1" max="50">
          </div>
          <button onclick="runProductTest()" class="btn btn-sm btn-success w-100 mb-2">Get Similar &#9654;</button>
          <div id="t3-result" style="display:none"></div>
        </div>
      </div>

    </div>
  </div>

</div>

<script>
"use strict";

const POLL_MS = 5000;
let evChart, rcChart, initialized = false;

const $ = id => document.getElementById(id);
const fmt = n => (n == null ? "—" : Number(n).toLocaleString());

function uptime(s) {
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
  return h ? `${h}h ${m}m` : m ? `${m}m ${sec}s` : `${sec}s`;
}

function fillTable(id, obj) {
  const rows = Object.entries(obj || {})
    .sort((a, b) => b[1] - a[1])
    .map(([k, v]) => `<tr><td>${k}</td><td class="text-end fw-semibold">${fmt(v)}</td></tr>`)
    .join("");
  $(id).innerHTML = rows || '<tr><td colspan="2" class="text-secondary">No data yet</td></tr>';
}

function mkChart(id, label, color) {
  return new Chart($(id), {
    type: "line",
    data: {
      labels: [],
      datasets: [{
        label,
        data: [],
        borderColor: color,
        backgroundColor: color + "1a",
        fill: true,
        tension: 0.35,
        pointRadius: 2,
        borderWidth: 2,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: { legend: { display: false } },
      scales: {
        y: {
          beginAtZero: true,
          ticks: { precision: 0, color: "#6b8ba4" },
          grid: { color: "#1f3048" }
        },
        x: {
          ticks: { color: "#6b8ba4", maxRotation: 0, autoSkip: true, maxTicksLimit: 10 },
          grid: { color: "#1f3048" }
        }
      }
    }
  });
}

function updateCharts(ts) {
  const labels = ts.map((_, i) => {
    const age = (ts.length - 1 - i) * 10;
    return age === 0 ? "now" : `-${age}s`;
  });
  const evData = ts.map(s => s.events || 0);
  const rcData = ts.map(s => s.recs || 0);

  if (!initialized) {
    evChart = mkChart("ch-events", "Events/10s", "#3b82f6");
    rcChart = mkChart("ch-recs", "Recs/10s", "#22c55e");
    initialized = true;
  }
  evChart.data.labels = labels;
  evChart.data.datasets[0].data = evData;
  evChart.update("none");

  rcChart.data.labels = labels;
  rcChart.data.datasets[0].data = rcData;
  rcChart.update("none");
}

function render(d) {
  const ing = d.ingestion || {};
  const rec = d.recommendations || {};
  const sys = d.system || {};

  // Stat cards
  $("s-events").textContent    = fmt(ing.events_processed);
  $("s-batches").textContent   = fmt(ing.batches);
  $("s-consumer").textContent  = fmt(ing.consumer_events_processed);
  $("s-consumer-batches").textContent = fmt(ing.consumer_batches);
  $("s-recs").textContent      = fmt(rec.served_total);
  $("s-failed").textContent    = fmt(ing.events_failed);
  $("s-timeouts").textContent  = fmt(ing.infer_timeouts);
  $("s-uptime").textContent    = uptime(d.uptime_seconds || 0);
  $("s-queue").textContent     = fmt(d.delivery_queue_depth);

  // System
  const ready = sys.catalog_ready;
  $("sys-catalog").innerHTML = ready
    ? '<span class="badge badge-ok px-2 py-1">Ready</span>'
    : '<span class="badge badge-err px-2 py-1">Not Ready</span>';
  $("sys-products").textContent  = fmt(sys.catalog_size);
  $("sys-sync").textContent      = sys.catalog_age_minutes != null
    ? `${sys.catalog_age_minutes} min ago` : "—";
  $("sys-sessions").textContent  = fmt(sys.active_sessions);
  $("sys-users").textContent     = fmt(sys.tracked_users);
  $("sys-taxons").textContent    = fmt(sys.taxons_with_products);
  $("sys-pop").textContent       = fmt(sys.popularity_entries);

  // Charts
  updateCharts(d.timeseries || []);

  // Tables
  fillTable("t-activity", ing.by_activity);
  fillTable("t-strategy", rec.by_strategy);
  fillTable("t-device",   rec.by_device);
  fillTable("t-endpoint", rec.by_endpoint);

  $("last-refresh").textContent = new Date().toLocaleTimeString();
}

async function poll() {
  try {
    const r = await fetch("/api/v1/metrics");
    if (r.ok) render(await r.json());
  } catch (e) {
    $("last-refresh").textContent = "fetch error";
  }
  fetchLogs();
}

async function fetchLogs() {
  try {
    const [ri, rp] = await Promise.all([
      fetch("/api/v1/logs/ingest?limit=40"),
      fetch("/api/v1/logs/push?limit=40"),
    ]);
    if (ri.ok) renderIngestLog(await ri.json());
    if (rp.ok) renderPushLog(await rp.json());
  } catch (_) {}
}

function fmtTs(iso) {
  if (!iso) return "—";
  return iso.replace("T", " ").replace(/\.\d+/, "").replace("+00:00", " UTC");
}

function renderIngestLog(data) {
  $("ingest-total").textContent = `${data.total_stored} stored`;
  const rows = (data.entries || []).map(e => {
    const types = Object.entries(e.event_types || {}).map(([k, v]) => `${k}:${v}`).join(", ");
    const users = (e.users || []).length;
    const ok = e.failed === 0;
    const color = ok ? "#4ade80" : "#f87171";
    return `<div style="padding:1px 0;border-bottom:1px solid #1a2d44">` +
      `<span style="color:#6b8ba4">${fmtTs(e.ts)}</span> ` +
      `<span style="color:#38bdf8">[${e.source}]</span> ` +
      `<span style="color:${color}">+${e.processed}</span>` +
      (e.failed ? `<span style="color:#f87171"> fail:${e.failed}</span>` : "") +
      ` users:${users}` +
      (types ? ` <span style="color:#94a3b8">${types}</span>` : "") +
      `</div>`;
  });
  $("log-ingest").innerHTML = rows.join("") || '<span style="color:#6b8ba4">No ingest batches yet</span>';
}

function renderPushLog(data) {
  $("push-total").textContent = `${data.total_stored} stored`;
  const rows = (data.entries || []).map(e => {
    const ok = e.push_status === "ok";
    const color = ok ? "#4ade80" : "#f87171";
    return `<div style="padding:1px 0;border-bottom:1px solid #1a2d44">` +
      `<span style="color:#6b8ba4">${fmtTs(e.ts)}</span> ` +
      `<span style="color:${color}">[${e.push_status}]</span> ` +
      `<span style="color:#e2e8f0">${e.account_id || "—"}</span> ` +
      `products:${e.products_count}` +
      (e.strategy ? ` <span style="color:#94a3b8">${e.strategy}</span>` : "") +
      (e.push_error ? ` <span style="color:#f87171" title="${e.push_error}">⚠ err</span>` : "") +
      `</div>`;
  });
  $("log-push").innerHTML = rows.join("") || '<span style="color:#6b8ba4">No pushes yet</span>';
}

// ── Live test panel ─────────────────────────────────────────────────────────

function testHdrs() {
  return { "Content-Type": "application/json" };
}
function showResult(id, html, ok) {
  const el = $(id);
  el.style.cssText = "display:block;max-height:260px;overflow-y:auto;padding:8px;border-radius:6px;font-size:.8rem;" +
    (ok ? "background:#071c10;border:1px solid #0a5c3b;color:#cdd9e5" : "background:#1c0707;border:1px solid #5c1a0a;color:#f87171");
  el.innerHTML = html;
}
function pills(ids) {
  const shown = ids.slice(0, 24).map(p =>
    `<span style="display:inline-block;background:#1a2d44;padding:1px 5px;border-radius:3px;margin:1px;font-family:monospace;font-size:.7rem">${p}</span>`
  ).join("");
  return shown + (ids.length > 24 ? ` <span style="color:#6b8ba4">+${ids.length - 24} more</span>` : "");
}

async function runFeedTest() {
  const uid = $("t1-uid").value.trim();
  if (!uid) { showResult("t1-result", "&#9888; Enter a User ID", false); return; }
  showResult("t1-result", '<span style="color:#6b8ba4">Fetching&hellip;</span>', true);
  try {
    const r = await fetch("/api/v1/feed", {
      method: "POST", headers: testHdrs(),
      body: JSON.stringify({
        account_id: uid,
        top_taxons: parseInt($("t1-taxons").value) || 3,
        top_n_per_taxon: parseInt($("t1-ppt").value) || 8
      })
    });
    const d = await r.json();
    if (!r.ok) { showResult("t1-result", `Error ${r.status}: ${JSON.stringify(d)}`, false); return; }
    let html = `<b>Strategy:</b> ${d.strategy} &nbsp; <b>Intent:</b> ${d.intent_score} &nbsp; <b>Products:</b> ${d.total_products}<hr class="test-divider">`;
    for (const tf of (d.taxon_feeds || [])) {
      html += `<div class="mb-1"><span style="color:#38bdf8;font-weight:600">${tf.taxon_name || tf.taxon_id}</span> <span style="color:#6b8ba4">(${tf.count})</span></div>`;
      html += `<div class="mb-2">${pills(tf.recommendations)}</div>`;
    }
    showResult("t1-result", html, true);
  } catch (e) { showResult("t1-result", `Network error: ${e.message}`, false); }
}

async function runSessionTest() {
  const uid = $("t2-uid").value.trim();
  const sid = $("t2-sid").value.trim();
  const ename = $("t2-ename").value;
  const evalRaw = $("t2-eval").value.trim();
  if (!uid) { showResult("t2-result", "&#9888; Enter an Account ID", false); return; }
  showResult("t2-result", '<span style="color:#6b8ba4">Ingesting&hellip;</span>', true);
  let evalFinal;
  if (!evalRaw) {
    evalFinal = "";
  } else {
    try { evalFinal = JSON.stringify(JSON.parse(evalRaw)); }
    catch (_) {
      if (ename === "product_click") evalFinal = JSON.stringify({ productIds: [evalRaw] });
      else if (ename === "taxon_click") evalFinal = JSON.stringify({ taxon: { label: evalRaw } });
      else evalFinal = evalRaw;
    }
  }
  try {
    const r = await fetch("/api/v1/consumer-events", {
      method: "POST", headers: testHdrs(),
      body: JSON.stringify({
        events: [{
          ACCOUNTID: uid,
          SESSIONID: sid || (uid + "-dash"),
          EVENTNAME: ename,
          EVENTVALUE: evalFinal,
          TIMESTAMP_: new Date().toISOString()
        }]
      })
    });
    const d = await r.json();
    if (!r.ok) { showResult("t2-result", `Error ${r.status}: ${JSON.stringify(d)}`, false); return; }
    let html = `<b>Processed:</b> ${d.processed} &nbsp; <b>Failed:</b> ${d.failed}<hr class="test-divider">`;
    if (d.recommendations && d.recommendations.length) {
      for (const rec of d.recommendations) {
        html += `<div class="mb-1"><b>User:</b> ${rec.id} &nbsp; <b>Strategy:</b> ${rec.strategy} &nbsp; <b>Intent:</b> ${rec.intent_score}</div>`;
        html += `<div class="mb-2">${pills(rec.recommendations)}</div>`;
      }
    } else {
      html += '<span style="color:#6b8ba4">Event ingested &mdash; no inline recs (user may need more history).</span>';
    }
    showResult("t2-result", html, true);
  } catch (e) { showResult("t2-result", `Network error: ${e.message}`, false); }
}

async function runProductTest() {
  const pid = $("t3-pid").value.trim();
  const uid = $("t3-uid").value.trim() || "anonymous";
  if (!pid) { showResult("t3-result", "&#9888; Enter an Anchor Product ID", false); return; }
  showResult("t3-result", '<span style="color:#6b8ba4">Fetching&hellip;</span>', true);
  try {
    const r = await fetch("/api/v1/recommendations/product", {
      method: "POST", headers: testHdrs(),
      body: JSON.stringify({
        account_id: uid,
        product_id: pid,
        top_n: parseInt($("t3-topn").value) || 10
      })
    });
    const d = await r.json();
    if (!r.ok) { showResult("t3-result", `Error ${r.status}: ${JSON.stringify(d)}`, false); return; }
    let html = `<b>Strategy:</b> ${d.strategy} &nbsp; <b>Count:</b> ${d.count} &nbsp; <b>Device:</b> ${d.device}`;
    if (d.context_taxon_id) html += ` &nbsp; <b>Taxon:</b> <span style="color:#38bdf8">${d.context_taxon_id}</span>`;
    html += `<hr class="test-divider">${pills(d.recommendations)}`;
    showResult("t3-result", html, true);
  } catch (e) { showResult("t3-result", `Network error: ${e.message}`, false); }
}

poll();
setInterval(poll, POLL_MS);
</script>
</body>
</html>"""


@router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard() -> HTMLResponse:
    """Live monitoring dashboard — no API key required."""
    return HTMLResponse(content=_DASHBOARD_HTML)
