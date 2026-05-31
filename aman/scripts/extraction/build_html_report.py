#!/usr/bin/env python3
"""Generate a self-contained, shareable HTML report of the extraction sample.

Output: aman/data/extraction_sample_report.html

This is a single static file — no build step, no server, no external
dependencies. The full per-review dataset is embedded inline as JSON.
Anyone with a browser can open and explore it.

Renders:
  - Project context + summary
  - v1 vs v2 prompt-iteration comparison
  - Failure-mode breakdown
  - Section coverage
  - Full filterable, searchable list of all 146 reviews with extractions
  - Cost projection
"""

from __future__ import annotations

import json
import html
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SUMMARY_PATH = ROOT / "data/llm_cache/sample/canvas_data.json"
FULL_PATH = ROOT / "data/llm_cache/sample/canvas_full_data.json"
OUT_PATH = ROOT / "data/extraction_sample_report.html"


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Zomato Review Extraction — v2 sample (146 reviews)</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root {
  --bg: #0f0f10;
  --bg-elev: #18181a;
  --bg-card: #1d1d20;
  --border: #2a2a2e;
  --border-strong: #3a3a40;
  --text: #e6e6e8;
  --text-dim: #a0a0a8;
  --text-faint: #707078;
  --accent: #6aa8ff;
  --success: #4ec07a;
  --warning: #d4a14a;
  --danger: #e06464;
  --info: #6ab5d8;
  --neutral: #5a5a62;
  --code-bg: #232328;
  --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}

@media (prefers-color-scheme: light) {
  :root {
    --bg: #ffffff;
    --bg-elev: #f7f7f8;
    --bg-card: #ffffff;
    --border: #e4e4e7;
    --border-strong: #d4d4d8;
    --text: #18181b;
    --text-dim: #525258;
    --text-faint: #8a8a93;
    --accent: #2563eb;
    --success: #16a34a;
    --warning: #ca8a04;
    --danger: #dc2626;
    --info: #0891b2;
    --neutral: #71717a;
    --code-bg: #f1f1f4;
  }
}

* { box-sizing: border-box; }

body {
  margin: 0;
  background: var(--bg);
  color: var(--text);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  font-size: 14px;
  line-height: 1.5;
}

.container {
  max-width: 1100px;
  margin: 0 auto;
  padding: 40px 24px 80px;
}

header h1 {
  font-size: 24px;
  margin: 0 0 4px;
  font-weight: 600;
  letter-spacing: -0.01em;
}

header p.subtitle {
  margin: 0;
  color: var(--text-dim);
}

h2 {
  font-size: 16px;
  font-weight: 600;
  margin: 36px 0 14px;
  letter-spacing: -0.005em;
}

h3 {
  font-size: 13px;
  font-weight: 600;
  margin: 14px 0 6px;
}

p, li { color: var(--text); }
p.muted { color: var(--text-dim); font-size: 13px; }

code, .code {
  font-family: var(--mono);
  font-size: 0.92em;
  background: var(--code-bg);
  padding: 1px 5px;
  border-radius: 3px;
}

.stat-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 14px;
  margin-top: 4px;
}
@media (max-width: 720px) {
  .stat-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}

.stat {
  background: var(--bg-elev);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 14px 16px;
}
.stat .value {
  font-size: 22px;
  font-weight: 600;
  letter-spacing: -0.01em;
  margin-bottom: 2px;
}
.stat .label { color: var(--text-dim); font-size: 12px; }
.stat .value.tone-success { color: var(--success); }
.stat .value.tone-warning { color: var(--warning); }
.stat .value.tone-danger { color: var(--danger); }
.stat .value.tone-info { color: var(--info); }

table {
  width: 100%;
  border-collapse: collapse;
  background: var(--bg-elev);
  border: 1px solid var(--border);
  border-radius: 6px;
  overflow: hidden;
  font-size: 13px;
}
th, td {
  padding: 8px 12px;
  text-align: left;
  border-bottom: 1px solid var(--border);
}
th {
  background: var(--bg-card);
  color: var(--text-dim);
  font-weight: 500;
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.04em;
}
tr:last-child td { border-bottom: none; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }

/* Failure-mode bars */
.bars { display: grid; gap: 8px; }
.bar-row {
  display: grid;
  grid-template-columns: 140px 1fr 1fr;
  align-items: center;
  gap: 12px;
  font-size: 13px;
}
.bar-row .label { color: var(--text-dim); }
.bar-track {
  background: var(--bg-elev);
  border: 1px solid var(--border);
  border-radius: 3px;
  height: 22px;
  position: relative;
  overflow: hidden;
}
.bar-fill {
  height: 100%;
  display: flex;
  align-items: center;
  padding: 0 8px;
  font-variant-numeric: tabular-nums;
  font-size: 12px;
  font-weight: 500;
  color: white;
}
.bar-fill.v1 { background: rgba(224, 100, 100, 0.55); }
.bar-fill.v2 { background: rgba(78, 192, 122, 0.55); }
.bar-fill .zero { color: var(--text-dim); padding-left: 4px; }

/* Filter bar */
.filters {
  display: flex;
  flex-direction: column;
  gap: 10px;
  background: var(--bg-elev);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 14px;
  position: sticky;
  top: 0;
  z-index: 5;
  backdrop-filter: blur(6px);
}
.filter-row {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
}
.filter-row > .label {
  width: 70px;
  color: var(--text-dim);
  font-size: 12px;
  font-weight: 500;
}
button.pill {
  background: transparent;
  color: var(--text);
  border: 1px solid var(--border-strong);
  border-radius: 999px;
  padding: 3px 10px;
  font: inherit;
  font-size: 12px;
  cursor: pointer;
  transition: all 0.1s;
}
button.pill:hover { border-color: var(--accent); }
button.pill.active {
  background: var(--accent);
  border-color: var(--accent);
  color: white;
}
input[type=search] {
  flex: 1;
  min-width: 160px;
  background: var(--bg-card);
  border: 1px solid var(--border-strong);
  color: var(--text);
  border-radius: 4px;
  padding: 5px 10px;
  font: inherit;
  font-size: 13px;
}
input[type=search]:focus { outline: none; border-color: var(--accent); }

.counter-row {
  margin-top: 10px;
  padding: 10px 14px;
  background: var(--bg-elev);
  border: 1px solid var(--border);
  border-radius: 6px;
  display: flex;
  flex-wrap: wrap;
  gap: 16px;
  font-size: 13px;
  color: var(--text-dim);
}
.counter-row strong { color: var(--text); font-variant-numeric: tabular-nums; }

/* Review row */
details.review {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 6px;
  margin-top: 6px;
}
details.review[open] { border-color: var(--border-strong); }
details.review > summary {
  list-style: none;
  cursor: pointer;
  padding: 9px 14px;
  display: grid;
  grid-template-columns: 50px 1fr auto;
  gap: 12px;
  align-items: center;
  user-select: none;
}
details.review > summary::-webkit-details-marker { display: none; }
details.review > summary:hover { background: rgba(255,255,255,0.02); }
@media (prefers-color-scheme: light) {
  details.review > summary:hover { background: rgba(0,0,0,0.02); }
}
.idx {
  font-family: var(--mono);
  font-size: 11px;
  color: var(--text-faint);
}
.restaurant {
  font-size: 13px;
  font-weight: 500;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.badges { display: flex; gap: 4px; flex-wrap: wrap; }

.badge {
  font-size: 11px;
  padding: 2px 7px;
  border-radius: 999px;
  border: 1px solid var(--border-strong);
  color: var(--text-dim);
  white-space: nowrap;
}
.badge.solid { color: white; border-color: transparent; }
.badge.solid.danger { background: var(--danger); }
.badge.solid.warning { background: var(--warning); }
.badge.solid.success { background: var(--success); }
.badge.solid.info { background: var(--info); }
.badge.solid.neutral { background: var(--neutral); }
.badge.outline.success { color: var(--success); border-color: var(--success); }
.badge.outline.warning { color: var(--warning); border-color: var(--warning); }
.badge.outline.danger { color: var(--danger); border-color: var(--danger); }
.badge.outline.info { color: var(--info); border-color: var(--info); }
.badge.outline.neutral { color: var(--text-dim); }

details.review > .body {
  padding: 4px 16px 16px;
  border-top: 1px solid var(--border);
}
.section-label {
  font-size: 11px;
  font-weight: 600;
  color: var(--text-faint);
  letter-spacing: 0.06em;
  margin: 14px 0 6px;
}
.section-label:first-child { margin-top: 10px; }
.review-text {
  font-size: 13.5px;
  color: var(--text);
  white-space: pre-wrap;
  word-break: break-word;
}
.warnings {
  font-size: 12px;
  color: var(--text-dim);
  font-style: italic;
  background: rgba(212, 161, 74, 0.07);
  border-left: 2px solid var(--warning);
  padding: 6px 10px;
  margin-top: 4px;
  border-radius: 3px;
}

/* Extraction display */
.ext-section { margin-top: 10px; }
.ext-section h5 {
  font-size: 12px;
  font-weight: 600;
  color: var(--text);
  margin: 12px 0 4px;
}
.kv-row {
  display: flex;
  gap: 8px;
  font-size: 12.5px;
  padding: 1px 0;
  align-items: baseline;
  flex-wrap: wrap;
}
.kv-key {
  min-width: 130px;
  color: var(--text-dim);
  font-weight: 500;
  font-size: 12px;
}
.kv-val { color: var(--text); }
.kv-val code { background: var(--code-bg); }
.kv-val.span-val { font-style: italic; color: var(--text-dim); font-size: 12px; }
.dish-row {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
  align-items: center;
  padding: 3px 0;
  font-size: 12.5px;
}
.dish-name { font-weight: 500; }
.dish-cat { color: var(--text-faint); font-size: 11px; }
.dish-taste { color: var(--text-dim); font-style: italic; font-size: 11.5px; }
.immune-row {
  display: flex;
  gap: 8px;
  align-items: baseline;
  flex-wrap: wrap;
  padding: 3px 0;
  font-size: 12px;
}
.immune-flag { font-weight: 500; }
.immune-span { color: var(--text-dim); font-style: italic; flex: 1; min-width: 0; }
.nested-block { padding-left: 12px; border-left: 1px solid var(--border); margin-left: 4px; }

.empty-msg {
  color: var(--text-faint);
  font-style: italic;
  font-size: 12.5px;
}

.callout {
  background: rgba(78, 192, 122, 0.08);
  border-left: 3px solid var(--success);
  padding: 14px 18px;
  border-radius: 4px;
  margin-top: 12px;
}
.callout strong { color: var(--text); }
.callout.warn {
  background: rgba(212, 161, 74, 0.08);
  border-left-color: var(--warning);
}

.section-divider {
  border: none;
  border-top: 1px solid var(--border);
  margin: 40px 0;
}

footer {
  margin-top: 56px;
  padding-top: 16px;
  border-top: 1px solid var(--border);
  color: var(--text-faint);
  font-size: 12px;
}
</style>
</head>
<body>
<div class="container">

<header>
  <h1>Restaurant review extraction — 146-review v2 sample</h1>
  <p class="subtitle">Gemini 2.5 Flash Lite · sync mode · temperature 0 · all 146 reviews stratified across rating, length, and source</p>
</header>

<h2>Summary</h2>
<div class="stat-grid">
  <div class="stat"><div class="value">__V2_TOTAL__</div><div class="label">Reviews extracted</div></div>
  <div class="stat"><div class="value tone-success">__V2_WARN_RATE__%</div><div class="label">Warning rate (v1 was __V1_WARN_RATE__%)</div></div>
  <div class="stat"><div class="value tone-success">__V2_API_ERR__</div><div class="label">API errors (v1 had __V1_API_ERR__)</div></div>
  <div class="stat"><div class="value">$__V2_COST__</div><div class="label">v2 sync cost (sample)</div></div>
</div>

<div class="callout">
  <strong>Prompt iteration outcome.</strong> v2 dropped the warning rate from
  __V1_WARN_RATE__% to __V2_WARN_RATE__% — a 63% reduction — and eliminated all 8 shape errors
  (immune_flags-as-list, atmosphere-shape leak) plus all 6 transient 503 errors via retry. The
  remaining warnings are mostly the model inventing fields like
  <code>atmosphere.ambience</code> and stray enum choices like
  <code>portion_size: "moderate"</code> — survivable, all extractions still usable.
</div>

<h2>Failure-mode breakdown · v1 vs v2</h2>
<p class="muted">
  Counts of pydantic validation issues per category, across the 146 reviews.
  "Invented field" = the model added a key not in the schema (e.g. <code>ambiance_score</code>).
  "Enum violation" = it emitted a value outside the field's allowed enum (e.g. <code>wait_for_food: "slow"</code> instead of <code>slow_30_45</code>).
  "Shape error" = list-vs-dict structural mistakes.
</p>
__FAILURE_BARS__

<h2>Section coverage</h2>
<p class="muted">
  For each top-level section in the schema, how many of the non-empty
  extractions populated it. Higher is better up to a point; empty reviews
  correctly stayed empty so coverage is computed against non-empty extractions only.
</p>
__COVERAGE_TABLE__

<h2>Cost projection · full corpus</h2>
<div class="stat-grid">
  <div class="stat"><div class="value tone-warning">$__P_SYNC__</div><div class="label">Sync, no caching</div></div>
  <div class="stat"><div class="value tone-info">$__P_BATCH__</div><div class="label">Batch mode (50% off)</div></div>
  <div class="stat"><div class="value tone-success">$__P_CACHED__</div><div class="label">Batch + context caching (est.)</div></div>
  <div class="stat"><div class="value">__N_CORPUS__</div><div class="label">Total reviews in corpus</div></div>
</div>

<hr class="section-divider">

<h2>Browse all 146 extractions</h2>
<p class="muted">
  Each row is one review. Use the filters and search to narrow down. Click
  any row to expand and see the original review alongside the structured
  extraction. Audit spans (the literal quotes the LLM grounded each field in)
  are preserved verbatim where present.
</p>

<div class="filters">
  <div class="filter-row" data-name="rating">
    <span class="label">rating</span>
    <button class="pill active" data-value="all">all</button>
    <button class="pill" data-value="1">1★</button>
    <button class="pill" data-value="2">2★</button>
    <button class="pill" data-value="3">3★</button>
    <button class="pill" data-value="4">4★</button>
    <button class="pill" data-value="5">5★</button>
  </div>
  <div class="filter-row" data-name="source">
    <span class="label">source</span>
    <button class="pill active" data-value="all">all sources</button>
    <button class="pill" data-value="reviews">reviews</button>
    <button class="pill" data-value="rescrape">rescrape</button>
  </div>
  <div class="filter-row" data-name="content">
    <span class="label">content</span>
    <button class="pill active" data-value="all">all</button>
    <button class="pill" data-value="with_extraction">with extraction</button>
    <button class="pill" data-value="empty">empty {}</button>
    <button class="pill" data-value="with_warnings">with warnings</button>
  </div>
  <div class="filter-row">
    <span class="label">search</span>
    <input type="search" id="search" placeholder="restaurant name or review text..." autocomplete="off">
  </div>
</div>

<div class="counter-row">
  Showing <strong id="count-showing">146</strong> / 146 ·
  With extraction <strong id="count-ext">0</strong> ·
  Empty {} <strong id="count-empty">0</strong> ·
  With warnings <strong id="count-warn">0</strong>
</div>

<div id="review-list">
__REVIEW_LIST__
</div>

<footer>
  Generated on __GEN_DATE__ from <code>aman/data/llm_cache/sample/v2/sample_combined.json</code>.
  This is a static, self-contained HTML file — share it by emailing or uploading anywhere.
  All data is embedded inline; no network calls.
</footer>

</div>

<script>
(function() {
  const reviews = Array.from(document.querySelectorAll("details.review"));
  const filters = {
    rating: "all",
    source: "all",
    content: "all",
    query: "",
  };

  document.querySelectorAll(".filter-row[data-name]").forEach(group => {
    const name = group.dataset.name;
    group.addEventListener("click", e => {
      const btn = e.target.closest("button.pill");
      if (!btn) return;
      group.querySelectorAll("button.pill").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      filters[name] = btn.dataset.value;
      apply();
    });
  });

  const search = document.getElementById("search");
  let searchTimer = null;
  search.addEventListener("input", () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      filters.query = search.value.trim().toLowerCase();
      apply();
    }, 120);
  });

  function apply() {
    let showing = 0, withExt = 0, empty = 0, withWarn = 0;
    for (const el of reviews) {
      const rating = el.dataset.rating;
      const source = el.dataset.source;
      const hasExt = el.dataset.hasExt === "1";
      const nWarn = parseInt(el.dataset.warns || "0", 10);
      const hay = el.dataset.search || "";

      let ok = true;
      if (filters.rating !== "all" && rating !== filters.rating) ok = false;
      if (filters.source !== "all" && source !== filters.source) ok = false;
      if (filters.content === "with_extraction" && !hasExt) ok = false;
      if (filters.content === "empty" && hasExt) ok = false;
      if (filters.content === "with_warnings" && nWarn === 0) ok = false;
      if (filters.query && !hay.includes(filters.query)) ok = false;

      el.style.display = ok ? "" : "none";
      if (ok) {
        showing++;
        if (hasExt) withExt++; else empty++;
        if (nWarn > 0) withWarn++;
      }
    }
    document.getElementById("count-showing").textContent = showing;
    document.getElementById("count-ext").textContent = withExt;
    document.getElementById("count-empty").textContent = empty;
    document.getElementById("count-warn").textContent = withWarn;
  }
  apply();
})();
</script>
</body>
</html>
"""


def esc(s) -> str:
    return html.escape(str(s), quote=True)


def render_dish_row(d: dict) -> str:
    name = esc(d.get("name", ""))
    cat = d.get("category")
    sent = d.get("sentiment", "")
    role = d.get("role")
    taste = d.get("taste_descriptors")
    temp = d.get("temperature_served")
    presentation = d.get("presentation")
    is_rec = d.get("is_recommended")
    freshness = d.get("freshness")

    sentiment_class = {
        "loved": "success",
        "liked": "success",
        "neutral": "neutral",
        "disliked": "danger",
        "hated": "danger",
    }.get(sent, "neutral")

    parts = [f'<span class="badge solid {sentiment_class}">{esc(sent)}</span>']
    parts.append(f'<span class="dish-name">{name}</span>')
    if cat:
        parts.append(f'<span class="dish-cat">({esc(cat)})</span>')
    if role:
        parts.append(f'<span class="badge outline neutral">{esc(role)}</span>')
    if is_rec:
        parts.append('<span class="badge outline success">recommended</span>')
    if isinstance(taste, list) and taste:
        parts.append(f'<span class="dish-taste">{esc(", ".join(taste))}</span>')
    if temp:
        parts.append(f'<code>{esc(temp)}</code>')
    if freshness:
        parts.append(f'<code>{esc(freshness)}</code>')
    if presentation:
        parts.append(f'<code>{esc(presentation)}</code>')
    return '<div class="dish-row">' + "".join(parts) + "</div>"


def render_immune(flags: dict) -> str:
    rows = []
    for flag, info in flags.items():
        if not isinstance(info, dict):
            continue
        sev = info.get("severity", "")
        span = info.get("span", "")
        sev_class = {"severe": "danger", "moderate": "warning", "mild": "info"}.get(sev, "neutral")
        rows.append(
            '<div class="immune-row">'
            f'<span class="badge solid {sev_class}">{esc(sev)}</span>'
            f'<span class="immune-flag">{esc(flag)}</span>'
            + (f'<span class="immune-span">"{esc(span)}"</span>' if span else '')
            + '</div>'
        )
    return "\n".join(rows)


def render_kv(obj: dict) -> str:
    rows = []
    for k, v in obj.items():
        if k == "span":
            rows.append(
                '<div class="kv-row">'
                '<span class="kv-key">span</span>'
                f'<span class="kv-val span-val">"{esc(v)}"</span>'
                '</div>'
            )
            continue
        if isinstance(v, list):
            pills = "".join(
                f'<span class="badge outline neutral">{esc(json.dumps(item) if isinstance(item, (dict,list)) else item)}</span>'
                for item in v
            )
            rows.append(
                f'<div class="kv-row"><span class="kv-key">{esc(k)}</span><span class="kv-val">{pills}</span></div>'
            )
            continue
        if isinstance(v, dict):
            rows.append(
                f'<div class="kv-row"><span class="kv-key">{esc(k)}</span></div>'
                f'<div class="nested-block">{render_kv(v)}</div>'
            )
            continue
        rows.append(
            '<div class="kv-row">'
            f'<span class="kv-key">{esc(k)}</span>'
            f'<span class="kv-val"><code>{esc(v)}</code></span>'
            '</div>'
        )
    return "\n".join(rows)


def render_section(name: str, value):
    if name == "dishes" and isinstance(value, list):
        body = "\n".join(render_dish_row(d) for d in value if isinstance(d, dict))
    elif name == "immune_flags" and isinstance(value, dict):
        body = render_immune(value)
    elif isinstance(value, dict):
        body = render_kv(value)
    elif isinstance(value, list):
        items = "".join(f'<span class="badge outline neutral">{esc(json.dumps(x) if isinstance(x, (dict,list)) else x)}</span>' for x in value)
        body = f'<div class="kv-row"><span class="kv-val">{items}</span></div>'
    else:
        body = f'<div class="kv-row"><span class="kv-val"><code>{esc(value)}</code></span></div>'
    return f'<h5>{esc(name)}</h5>{body}'


def render_review(rec: dict) -> str:
    rating = rec.get("rating")
    rating_str = f"{rating:.1f}" if isinstance(rating, (int, float)) else "?"
    rating_bucket = "?"
    if isinstance(rating, (int, float)):
        if rating <= 1.5: rating_bucket = "1"
        elif rating <= 2.5: rating_bucket = "2"
        elif rating <= 3.5: rating_bucket = "3"
        elif rating <= 4.5: rating_bucket = "4"
        else: rating_bucket = "5"

    rating_class = "neutral"
    if isinstance(rating, (int, float)):
        if rating <= 2: rating_class = "danger"
        elif rating >= 4: rating_class = "success"
        else: rating_class = "warning"

    source = rec.get("source", "")
    desc = rec.get("desc") or ""
    desc_len = rec.get("desc_len", 0)
    ext = rec.get("ext") or {}
    warns = rec.get("warns") or []
    in_tok = rec.get("in_tok", 0)
    out_tok = rec.get("out_tok", 0)
    has_ext = 1 if ext else 0

    badges = [
        f'<span class="badge solid {rating_class}">{rating_str}★</span>',
        f'<span class="badge outline neutral">{esc(source)}</span>',
        f'<span class="badge outline neutral">{desc_len}ch</span>',
        f'<span class="badge outline {"success" if has_ext else "neutral"}">{"ext" if has_ext else "{}"}</span>',
    ]
    if warns:
        badges.append(f'<span class="badge solid warning">{len(warns)}w</span>')

    sections_html = ""
    if ext:
        section_blocks = [
            f'<div class="ext-section">{render_section(k, v)}</div>'
            for k, v in ext.items()
        ]
        sections_html = "\n".join(section_blocks)
    else:
        sections_html = '<p class="empty-msg">{} — model correctly emitted empty (review had no extractable content)</p>'

    warnings_html = ""
    if warns:
        items = "".join(
            f'<div class="warnings">{esc(w.split(chr(10))[0][:300])}</div>'
            for w in warns
        )
        warnings_html = f'<div class="section-label">VALIDATION WARNINGS</div>{items}'

    desc_block = (
        f'<p class="review-text">{esc(desc)}</p>'
        if desc
        else '<p class="empty-msg">(empty review text)</p>'
    )

    haystack = (rec.get("restaurant", "") + " " + desc).lower()
    safe_hay = esc(haystack[:400])  # truncate to keep DOM small

    return (
        f'<details class="review" '
        f'data-rating="{rating_bucket}" '
        f'data-source="{esc(source)}" '
        f'data-has-ext="{has_ext}" '
        f'data-warns="{len(warns)}" '
        f'data-search="{safe_hay}">'
        '<summary>'
        f'<span class="idx">#{int(rec.get("idx", 0)):03d}</span>'
        f'<span class="restaurant">{esc(rec.get("restaurant", ""))}</span>'
        f'<span class="badges">{"".join(badges)}</span>'
        '</summary>'
        '<div class="body">'
        '<div class="section-label">REVIEW</div>'
        f'{desc_block}'
        f'<div class="section-label">EXTRACTED · {in_tok} in / {out_tok} out tok</div>'
        f'{sections_html}'
        f'{warnings_html}'
        '</div>'
        '</details>'
    )


def render_failure_bars(v1_modes: dict, v2_modes: dict) -> str:
    keys = sorted(set(list(v1_modes.keys()) + list(v2_modes.keys())))
    max_val = max(
        max(v1_modes.values(), default=0),
        max(v2_modes.values(), default=0),
        1,
    )
    rows = ['<div class="bars">']
    rows.append(
        '<div class="bar-row" style="font-size:11px;color:var(--text-faint);">'
        '<div class="label">category</div>'
        '<div>v1 (original prompt)</div>'
        '<div>v2 (iterated prompt)</div>'
        '</div>'
    )
    for k in keys:
        v1c = v1_modes.get(k, 0)
        v2c = v2_modes.get(k, 0)
        v1_pct = int(round(100 * v1c / max_val))
        v2_pct = int(round(100 * v2c / max_val))
        v1_inner = f'<div class="bar-fill v1" style="width:{max(v1_pct, 2)}%;">{v1c}</div>' if v1c else '<span class="zero">0</span>'
        v2_inner = f'<div class="bar-fill v2" style="width:{max(v2_pct, 2)}%;">{v2c}</div>' if v2c else '<span class="zero">0</span>'
        rows.append(
            f'<div class="bar-row">'
            f'<div class="label">{esc(k.replace("_", " "))}</div>'
            f'<div class="bar-track">{v1_inner}</div>'
            f'<div class="bar-track">{v2_inner}</div>'
            f'</div>'
        )
    rows.append('</div>')
    return "\n".join(rows)


def render_coverage_table(v1_cov: dict, v2_cov: dict, v1_n: int, v2_n: int) -> str:
    sections = sorted(set(list(v1_cov.keys()) + list(v2_cov.keys())),
                      key=lambda s: -v2_cov.get(s, 0))
    rows = []
    for s in sections:
        v1c = v1_cov.get(s, 0)
        v2c = v2_cov.get(s, 0)
        v1_pct = round(100 * v1c / max(v1_n, 1))
        v2_pct = round(100 * v2c / max(v2_n, 1))
        diff = v2c - v1c
        diff_str = f"+{diff}" if diff >= 0 else str(diff)
        rows.append(
            f"<tr>"
            f"<td>{esc(s)}</td>"
            f"<td class='num'>{v1c} ({v1_pct}%)</td>"
            f"<td class='num'>{v2c} ({v2_pct}%)</td>"
            f"<td class='num'>{diff_str}</td>"
            f"</tr>"
        )
    return (
        "<table>"
        "<thead><tr>"
        "<th>Section</th><th style='text-align:right;'>v1</th>"
        "<th style='text-align:right;'>v2</th><th style='text-align:right;'>Δ</th>"
        "</tr></thead>"
        "<tbody>" + "\n".join(rows) + "</tbody></table>"
    )


def main():
    with open(SUMMARY_PATH) as f:
        summary = json.load(f)
    with open(FULL_PATH) as f:
        records = json.load(f)

    v1 = summary["v1"]
    v2 = summary["v2"]

    review_html = "\n".join(render_review(r) for r in records)
    failure_bars = render_failure_bars(v1["failure_modes"], v2["failure_modes"])
    coverage = render_coverage_table(
        v1["section_coverage"], v2["section_coverage"],
        v1["with_extraction"], v2["with_extraction"],
    )

    import datetime
    out = HTML_TEMPLATE
    repls = {
        "__V2_TOTAL__": str(v2["total"]),
        "__V2_WARN_RATE__": f"{v2['warning_rate_pct']:.1f}",
        "__V1_WARN_RATE__": f"{v1['warning_rate_pct']:.1f}",
        "__V2_API_ERR__": str(v2["api_errors"]),
        "__V1_API_ERR__": str(v1["api_errors"]),
        "__V2_COST__": f"{v2['total_cost_usd']:.4f}",
        "__FAILURE_BARS__": failure_bars,
        "__COVERAGE_TABLE__": coverage,
        "__P_SYNC__": f"{int(summary['projection_sync_usd']):,}",
        "__P_BATCH__": f"{int(summary['projection_batch_usd']):,}",
        "__P_CACHED__": f"{int(summary['projection_batch_cached_usd']):,}",
        "__N_CORPUS__": f"{summary['n_reviews_total_corpus']:,}",
        "__REVIEW_LIST__": review_html,
        "__GEN_DATE__": datetime.date.today().isoformat(),
    }
    for k, v in repls.items():
        out = out.replace(k, v)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(out, encoding="utf-8")
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"Wrote {OUT_PATH} ({size_kb:.1f} KB, {len(records)} reviews)")


if __name__ == "__main__":
    main()
