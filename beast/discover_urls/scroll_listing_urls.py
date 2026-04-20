#!/usr/bin/env python3
"""
Playwright infinite-scroll collector for Zomato listing pages (e.g. /bangalore/restaurants).

Scrolls until the set of normalized venue URLs stops growing for several consecutive rounds,
then optionally fuzzy-matches a master list (Name - Area per line) to discovered anchors.

Install Chromium (run **only** this on one line — no ``#`` comments after it)::

    python3 -m playwright install chromium

**Attach to your own browser** (skip launch + optional skip ``goto``): start Chrome with a debug port, open
Zomato in a tab, then run with ``--cdp-endpoint`` (see ``--help``). Use ``--single-pass`` to scrape the
current DOM only (no auto-scroll).

Collection merges three sources (toggle with flags):

1. **DOM** — real ``<a href>`` (including open shadow roots). Zomato often **virtualizes** the list, so only
   ~10–20 venue links exist in the DOM at once even after you scroll.
2. **SPA globals** (default **on**) — strings inside ``window.__PRELOADED_STATE__`` / ``__NEXT_DATA__`` /
   ``__NUXT__``. These are the **same URLs the web app loaded into memory**, not guessed from HTML shape.
3. **``--html-regex``** (off by default) — extra scan of raw HTML for URL-like strings (noisiest).

Outputs:
  - Discovered venues CSV: url, name, listing_url
  - Matched CSV (with --master): raw_line, name, area, url, discovered_name, match_score
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRAPER_DIR = REPO_ROOT / "zomato-scraper"
if str(SCRAPER_DIR) not in sys.path:
    sys.path.insert(0, str(SCRAPER_DIR))

from bulk_scraper import (  # noqa: E402
    is_junk_listing_anchor_name,
    normalize_restaurant_url,
)
from playwright_chromium import chromium_launch_kwargs  # noqa: E402


def _normalized_query_dict(query: str) -> dict[str, tuple[str, ...]]:
    """Stable dict for comparing query strings (order-independent)."""
    raw = parse_qs(query or "", keep_blank_values=True)
    return {k: tuple(sorted(v)) for k, v in sorted(raw.items())}


def _listing_urls_equivalent(current: str, target: str) -> bool:
    """Same listing page: host, path, and query (so ``?sort=cd`` ≠ bare ``/restaurants``)."""
    try:
        a = urlparse((current or "").strip())
        b = urlparse((target or "").strip())
        if not a.netloc or not b.netloc:
            return False
        if a.netloc.lower() != b.netloc.lower():
            return False
        pa = (a.path or "/").rstrip("/").lower()
        pb = (b.path or "/").rstrip("/").lower()
        if pa != pb:
            return False
        return _normalized_query_dict(a.query) == _normalized_query_dict(b.query)
    except Exception:
        return False


def _prime_listing_lazy_rows(page) -> None:
    """Scroll down so virtualized grids mount more venue anchors (after a fresh navigation)."""
    try:
        for _ in range(8):
            page.mouse.wheel(0, 950)
            page.wait_for_timeout(220)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(900)
    except Exception:
        pass


def _slug_display_name(normalized_url: str) -> str:
    slug = (normalized_url.rsplit("/", 1)[-1] or "").replace("-bangalore", "").replace("-", " ").strip()
    return slug.title() or "Restaurant"


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").lower().strip())


# Listing cards concatenate title + rating + cuisines + price + locality in one string.
_RE_CARD_RATING_BREAK = re.compile(r"\s+[0-5]\.\d\s+")


def _listing_card_title_for_match(disc_name: str, url: str) -> str:
    """Use a short title for fuzzy match; full card text destroys ``SequenceMatcher`` scores."""
    raw = (disc_name or "").replace("\r\n", "\n").strip()
    if not raw:
        return _slug_display_name(url)
    m = _RE_CARD_RATING_BREAK.search(raw)
    if m:
        head = raw[: m.start()].strip()
        if len(head) >= 3:
            return head
    first = raw.split("\n", 1)[0].strip()
    if len(first) > 120:
        return _slug_display_name(url)
    return first


def parse_master_text(text: str) -> list[dict[str, str]]:
    """
    One restaurant per line: ``Name - Area`` (split on last `` - ``).
    Skips obvious header lines and blanks.
    """
    rows: list[dict[str, str]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        low = line.lower()
        if low.startswith("complete & deduplicated"):
            continue
        if re.match(r"^\d+\s+restaurants\s+scraped", low):
            continue
        if " - " not in line:
            continue
        name, area = line.rsplit(" - ", 1)
        name = name.strip()
        area = area.strip()
        if not name or not area:
            continue
        rows.append({"raw_line": line, "name": name, "area": area})
    return rows


def _area_tokens(area: str) -> list[str]:
    parts = re.split(r"[,/&]|(?:\s+or\s+)", area, flags=re.I)
    out: list[str] = []
    for p in parts:
        for t in re.split(r"[\s\-]+", p.strip()):
            t = re.sub(r"[^a-z0-9]", "", t.lower())
            if len(t) > 2:
                out.append(t)
    return out


def _slug_blob(url: str) -> str:
    slug = (url.rsplit("/", 1)[-1] or "").lower()
    return slug


def match_score(master_name: str, master_area: str, disc_name: str, url: str) -> float:
    disc_cmp = _listing_card_title_for_match(disc_name, url)
    base = SequenceMatcher(None, _norm(master_name), _norm(disc_cmp)).ratio()
    slug = _slug_blob(url)
    toks = _area_tokens(master_area)
    if not toks:
        return base
    hits = sum(1 for t in toks if t in slug)
    bump = 0.22 * (hits / len(toks))
    return min(1.0, base + bump)


def collect_links_from_page(page, city: str) -> dict[str, str]:
    """
    Return map normalized_url -> best display name.

    Uses only **live** ``<a href>`` attributes from the page (what the browser would navigate to),
    walking **open shadow roots** so links inside web components are included.

    Listing cards often put long text inside ``<a>``; ``is_junk_listing_anchor_name`` treats
    ``len > 120`` as junk. We use the first line / truncation for junk checks only, then fall back
    to a slug-derived label when needed.
    """
    pairs = page.evaluate(
        """() => {
          const out = [];
          function collectAnchors(root) {
            root.querySelectorAll('a[href]').forEach((a) => {
              const href = a.getAttribute('href') || '';
              const raw = (a.innerText || a.textContent || '').replace(/\\s+/g, ' ').trim();
              const firstLine = raw.split(/\\r?\\n/)[0].trim();
              out.push({ href, text: firstLine || raw });
            });
          }
          function walk(root) {
            collectAnchors(root);
            root.querySelectorAll('*').forEach((el) => {
              if (el.shadowRoot) walk(el.shadowRoot);
            });
          }
          walk(document);
          return out;
        }"""
    )
    best: dict[str, str] = {}
    for row in pairs:
        href = row.get("href") or ""
        text = (row.get("text") or "").strip()
        nu = normalize_restaurant_url(href, city)
        if not nu:
            continue
        short = text[:120].strip() if text else ""
        if short and is_junk_listing_anchor_name(short):
            short = ""
        label = short or _slug_display_name(nu)
        prev = best.get(nu)
        if prev is None or len(label) > len(prev):
            best[nu] = label
    return best


def collect_urls_from_spa_globals(page, city: str) -> tuple[dict[str, str], int]:
    """
    Collect venue URL strings embedded in the page: common SPA globals, JSON ``<script>`` blobs,
    ``localStorage`` / ``sessionStorage``, and relative ``/{city}/…`` paths found in those trees.

    Zomato's listing stack changes over time; not every build exposes ``__PRELOADED_STATE__``.
    """
    city_l = city.lower()
    raw_seen = 0
    try:
        bundle = page.evaluate(
            """(city) => {
              const raw = new Set();
              const prefix = '/' + city + '/';

              function consider(s) {
                if (typeof s !== 'string') return;
                const t = s.trim();
                if (t.length < 22 || t.length > 900) return;

                if (t.startsWith(prefix) && !t.startsWith('//')) {
                  const path = t.split('?')[0].split('#')[0].replace(/\\/+$/, '');
                  const segs = path.split('/').filter(Boolean);
                  if (segs.length >= 2 && segs[0] === city) {
                    raw.add('https://www.zomato.com' + path);
                  }
                }

                if (t.includes('zomato.com') && t.includes(prefix) && /^https?:\\/\\//i.test(t)) {
                  raw.add(t.split('?')[0].split('#')[0].replace(/\\/+$/, ''));
                }
              }

              function walk(o, depth, seen) {
                if (depth > 55 || o == null) return;
                if (typeof o === 'string') {
                  consider(o);
                  return;
                }
                if (typeof o !== 'object') return;
                if (seen.has(o)) return;
                seen.add(o);
                if (Array.isArray(o)) {
                  for (const x of o) {
                    if (typeof x === 'string') consider(x);
                    else walk(x, depth + 1, seen);
                  }
                  return;
                }
                let keys;
                try {
                  keys = Object.keys(o);
                } catch (e) {
                  return;
                }
                for (const k of keys) {
                  try {
                    const v = o[k];
                    if (typeof v === 'string') consider(v);
                    else walk(v, depth + 1, seen);
                  } catch (e) {}
                }
              }

              const seen = new WeakSet();

              for (const k of ['__PRELOADED_STATE__', '__NEXT_DATA__', '__NUXT__']) {
                try {
                  if (window[k]) walk(window[k], 0, seen);
                } catch (e) {}
              }

              try {
                for (const k of Object.keys(window)) {
                  if (/^(webkit|chrome|inner|frame|length)/i.test(k)) continue;
                  if (!/PRELOAD|NEXT_DATA|NUXT|Zomato|zStore|apollo|relay|redux|entity|entities|swr|query|cache|state|store|props|payload|pageProps|data/i.test(k))
                    continue;
                  try {
                    const v = window[k];
                    if (v && typeof v === 'object') walk(v, 0, seen);
                  } catch (e) {}
                }
              } catch (e) {}

              const scriptSelector = [
                'script[type="application/json"]',
                'script[type="application/ld+json"]',
                'script#__NEXT_DATA__',
                'script[id="__NEXT_DATA__"]',
              ].join(', ');
              document.querySelectorAll(scriptSelector).forEach((sc) => {
                const txt = (sc.textContent || '').trim();
                if (!txt || txt.length > 6_000_000) return;
                try {
                  walk(JSON.parse(txt), 0, seen);
                } catch (e) {
                  consider(txt);
                }
              });

              function tryParseStorage(store) {
                try {
                  for (let i = 0; i < store.length; i++) {
                    const k = store.key(i);
                    const v = store.getItem(k);
                    if (!v || v.length > 4_000_000) continue;
                    try {
                      walk(JSON.parse(v), 0, seen);
                    } catch (e) {
                      consider(v);
                    }
                  }
                } catch (e) {}
              }
              tryParseStorage(localStorage);
              tryParseStorage(sessionStorage);

              return { urls: Array.from(raw), rawCount: raw.size };
            }""",
            city_l,
        )
    except Exception:
        return {}, 0

    if not isinstance(bundle, dict):
        return {}, 0
    raw_list = bundle.get("urls") or []
    raw_seen = int(bundle.get("rawCount", len(raw_list)))

    best: dict[str, str] = {}
    for raw in raw_list or []:
        raw_s = str(raw).strip()
        if raw_s.startswith("http://www.zomato.com"):
            raw_s = "https://www.zomato.com" + raw_s[len("http://www.zomato.com") :]
        nu = normalize_restaurant_url(raw_s, city_l)
        if nu:
            best.setdefault(nu, _slug_display_name(nu))
    return best, raw_seen


def collect_urls_from_html(html: str, city: str) -> dict[str, str]:
    """Opt-in fallback: venue-shaped ``https://`` strings in raw HTML (not the same as real ``href``s)."""
    best: dict[str, str] = {}
    c = re.escape(city.lower())
    # Slug must contain at least two hyphens (filters hub paths like ``/bangalore/delivery``).
    pat = re.compile(rf"https://www\.zomato\.com/{c}/[a-z0-9]+(?:-[a-z0-9]+){{2,}}", re.I)
    for m in pat.finditer(html or ""):
        raw = m.group(0).rstrip("/").split("?")[0]
        nu = normalize_restaurant_url(raw, city)
        if not nu:
            continue
        best.setdefault(nu, _slug_display_name(nu))
    return best


def _dismiss_common_overlays(page) -> None:
    for sel in (
        'button:has-text("Accept")',
        'button:has-text("I agree")',
        '[aria-label="Close"]',
        "div[role='dialog'] button.close",
    ):
        try:
            page.locator(sel).first.click(timeout=900)
            page.wait_for_timeout(400)
        except Exception:
            pass


def _pick_page_for_cdp(browser, city: str):
    """Prefer a tab whose URL looks like this city's Zomato listing or venue page."""
    city_l = city.lower()
    for ctx in browser.contexts:
        for pg in reversed(ctx.pages):
            u = (pg.url or "").lower()
            if "zomato.com" in u and f"/{city_l}/" in u:
                return ctx, pg
    for ctx in browser.contexts:
        if ctx.pages:
            return ctx, ctx.pages[-1]
    raise RuntimeError(
        "CDP connected but no open tabs. Open https://www.zomato.com/... in Chrome first."
    )


def _collect_round(
    page,
    city: str,
    *,
    use_html_regex: bool,
    use_spa_state: bool,
    metrics: dict | None = None,
) -> dict[str, str]:
    chunk = collect_links_from_page(page, city)
    if metrics is not None:
        metrics["dom_anchors"] = len(chunk)

    if use_spa_state:
        spa, spa_raw_seen = collect_urls_from_spa_globals(page, city)
        if metrics is not None:
            metrics["spa_raw_seen"] = spa_raw_seen
            metrics["spa_normalized_unique"] = len(spa)
        for u, n in spa.items():
            prev = chunk.get(u)
            if prev is None or len(n) > len(prev):
                chunk[u] = n

    if use_html_regex:
        try:
            html = page.content()
        except Exception:
            html = ""
        for u, n in collect_urls_from_html(html, city).items():
            prev = chunk.get(u)
            if prev is None or len(n) > len(prev):
                chunk[u] = n

    if metrics is not None:
        metrics["merged_unique"] = len(chunk)
    return chunk


def _log_empty_page_debug(page, city: str) -> None:
    try:
        title = page.title()
    except Exception:
        title = ""
    try:
        html = page.content()
    except Exception:
        html = ""
    c = re.escape(city.lower())
    n_href = len(re.findall(rf'href=["\'][^"\']*{c}[^"\']*["\']', html, re.I))
    print(
        f"[scroll] debug: title={title!r} html_len={len(html)} "
        f"city-ish href attrs≈{n_href} city={city!r}",
        flush=True,
    )


def run_scroll_collect(
    listing_url: str,
    city: str,
    *,
    headless: bool,
    scroll_wait_ms: int,
    max_rounds: int,
    stable_rounds_to_stop: int,
    goto_timeout_ms: int,
    cdp_endpoint: str | None,
    use_current_page: bool,
    single_pass: bool,
    use_html_regex: bool,
    use_spa_state: bool,
) -> tuple[dict[str, str], str]:
    """
    Returns (venues dict, listing_url_for_csv) — CSV ``listing_url`` column uses the active page
    URL when attaching via CDP without navigation.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise SystemExit(
            "Install Playwright: pip install playwright\n"
            "Then run **only**: python3 -m playwright install chromium"
        ) from exc

    listing_url = urljoin("https://www.zomato.com/", listing_url.strip())
    merged: dict[str, str] = {}
    listing_url_for_csv = listing_url

    kw = chromium_launch_kwargs()
    kw["headless"] = headless

    with sync_playwright() as p:
        use_cdp = bool(cdp_endpoint and cdp_endpoint.strip())
        browser = None
        context = None
        page = None
        try:
            if use_cdp:
                browser = p.chromium.connect_over_cdp(cdp_endpoint.strip())
                context, page = _pick_page_for_cdp(browser, city)
                listing_url_for_csv = page.url or listing_url
                print(
                    f"[scroll] CDP attached; using tab {listing_url_for_csv!r}",
                    flush=True,
                )
                did_fresh_goto = False
                if not use_current_page:
                    if _listing_urls_equivalent(page.url or "", listing_url):
                        print(
                            "[scroll] tab already matches --listing-url; skipping navigation "
                            "(avoids reload wiping in-memory listing).",
                            flush=True,
                        )
                    else:
                        page.goto(
                            listing_url,
                            wait_until="domcontentloaded",
                            timeout=goto_timeout_ms,
                        )
                        try:
                            page.wait_for_load_state("networkidle", timeout=25000)
                        except Exception:
                            pass
                        listing_url_for_csv = page.url or listing_url
                        did_fresh_goto = True
                if did_fresh_goto:
                    page.wait_for_timeout(2800)
                    _prime_listing_lazy_rows(page)
                else:
                    page.wait_for_timeout(800)
                _dismiss_common_overlays(page)
            else:
                browser = p.chromium.launch(**kw)
                context = browser.new_context(
                    viewport={"width": 1280, "height": 900},
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                    ),
                    locale="en-IN",
                    extra_http_headers={"Accept-Language": "en-IN,en;q=0.9"},
                )
                page = context.new_page()
                page.goto(listing_url, wait_until="domcontentloaded", timeout=goto_timeout_ms)
                try:
                    page.wait_for_load_state("networkidle", timeout=20000)
                except Exception:
                    pass
                page.wait_for_timeout(2000)
                _dismiss_common_overlays(page)
                listing_url_for_csv = page.url or listing_url

            if single_pass:
                mpass: dict[str, int | None] = {}
                merged = dict(
                    _collect_round(
                        page,
                        city,
                        use_html_regex=use_html_regex,
                        use_spa_state=use_spa_state,
                        metrics=mpass,
                    )
                )
                print(
                    f"[scroll] single-pass scrape: {len(merged)} unique venues "
                    f"(dom_anchors={mpass.get('dom_anchors', 0)}, "
                    f"spa_raw_seen={mpass.get('spa_raw_seen', 0)}, "
                    f"spa_urls={mpass.get('spa_normalized_unique', 0)})",
                    flush=True,
                )
            else:
                stable = 0
                prev_count = -1
                for round_i in range(1, max_rounds + 1):
                    for _ in range(6):
                        page.mouse.wheel(0, 900)
                        page.wait_for_timeout(120)
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(scroll_wait_ms)
                    chunk = _collect_round(
                        page,
                        city,
                        use_html_regex=use_html_regex,
                        use_spa_state=use_spa_state,
                    )
                    before = len(merged)
                    for u, n in chunk.items():
                        prev = merged.get(u)
                        if prev is None or len(n) > len(prev):
                            merged[u] = n
                    after = len(merged)
                    if after == prev_count:
                        stable += 1
                        if stable >= stable_rounds_to_stop:
                            print(
                                f"[scroll] stop: no new venue URLs for {stable_rounds_to_stop} rounds "
                                f"after {round_i} scrolls (venues={after})",
                                flush=True,
                            )
                            break
                    else:
                        stable = 0
                    prev_count = after
                    if round_i % 10 == 0 or round_i == 1:
                        print(f"[scroll] round={round_i} venues={after} (+{after - before})", flush=True)

            if not merged:
                _log_empty_page_debug(page, city)
        finally:
            if browser is not None:
                if use_cdp:
                    # Do not context.close() — that can tear down the user's default browser UI.
                    browser.close()
                else:
                    if context is not None:
                        context.close()
                    browser.close()

    return merged, listing_url_for_csv


def write_discovered_csv(path: Path, venues: dict[str, str], listing_url: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["url", "name", "listing_url"])
        w.writeheader()
        for url in sorted(venues):
            w.writerow({"url": url, "name": venues[url], "listing_url": listing_url})


def write_matched_csv(
    path: Path,
    masters: list[dict],
    venues: dict[str, str],
    *,
    min_score: float,
    require_area_slug_hit: bool,
) -> tuple[int, int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    matched = 0
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["raw_line", "name", "area", "url", "discovered_name", "match_score"],
        )
        w.writeheader()
        for m in masters:
            best_url = ""
            best_name = ""
            best_score = 0.0
            for url, dname in venues.items():
                sc = match_score(m["name"], m["area"], dname, url)
                if sc > best_score:
                    best_score = sc
                    best_url = url
                    best_name = _listing_card_title_for_match(dname, url)
            slug = _slug_blob(best_url) if best_url else ""
            toks = _area_tokens(m["area"])
            slug_hits = sum(1 for t in toks if t in slug) if slug and toks else 0
            ok = best_score >= min_score and best_url
            if ok and require_area_slug_hit and slug_hits < 1 and best_score < 0.92:
                ok = False
            if ok:
                matched += 1
            w.writerow(
                {
                    "raw_line": m["raw_line"],
                    "name": m["name"],
                    "area": m["area"],
                    "url": best_url if ok else "",
                    "discovered_name": best_name if ok else "",
                    "match_score": f"{best_score:.3f}",
                }
            )
    return matched, len(masters)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Playwright infinite scroll Zomato listing URL harvester.",
        epilog=(
            "Example: attach to Chrome you started with remote debugging (macOS), open Zomato, then scrape once:\n"
            '  /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222\n'
            "  python3 beast/discover_urls/scroll_listing_urls.py --cdp-endpoint http://127.0.0.1:9222 --single-pass\n"
            "Omit --single-pass to keep auto-scrolling that tab. Add --goto-listing to navigate the tab to --listing-url."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--listing-url",
        default="https://www.zomato.com/bangalore/restaurants",
        help="Full Zomato listing page URL to scroll.",
    )
    parser.add_argument("--city", default="bangalore", help="City slug for normalize_restaurant_url.")
    parser.add_argument(
        "--out-discovered",
        type=Path,
        default=REPO_ROOT / "beast/discover_urls/output/scroll_listing_urls.csv",
        help="CSV of discovered venue URLs.",
    )
    parser.add_argument(
        "--master",
        type=Path,
        default=REPO_ROOT / "beast/discover_urls/data/bangalore_master_list.txt",
        help="Text file: one ``Name - Area`` per line (after optional headers).",
    )
    parser.add_argument(
        "--skip-match",
        action="store_true",
        help="Do not read master list or write matched CSV (discovery only).",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=0.75,
        help="Minimum fuzzy+area match score to attach a URL (0–1).",
    )
    parser.add_argument(
        "--require-area-in-slug",
        action="store_true",
        help="Require at least one area token substring in the URL slug (unless score>=0.92).",
    )
    parser.add_argument(
        "--out-matched",
        type=Path,
        default=REPO_ROOT / "beast/discover_urls/output/master_matched_to_scroll.csv",
        help="Write when --master is set.",
    )
    parser.add_argument("--no-scroll", action="store_true", help="Skip browser; only match using existing discovered CSV.")
    parser.add_argument(
        "--discovered-in",
        type=Path,
        default=None,
        help="With --no-scroll: read url,name from this CSV instead of scrolling.",
    )
    parser.add_argument("--scroll-wait-ms", type=int, default=1600, help="Pause after each scroll-to-bottom.")
    parser.add_argument("--max-scroll-rounds", type=int, default=250, help="Safety cap on scroll iterations.")
    parser.add_argument(
        "--stable-rounds",
        type=int,
        default=8,
        help="Stop after this many consecutive rounds with no new venue URLs.",
    )
    parser.add_argument("--goto-timeout-ms", type=int, default=120000)
    parser.add_argument("--headed", action="store_true", help="Run browser headed (default headless).")
    parser.add_argument(
        "--cdp-endpoint",
        metavar="URL",
        default=None,
        help=(
            "Connect to an existing Chromium/Chrome over CDP (e.g. http://127.0.0.1:9222). "
            "Start the browser with --remote-debugging-port=9222, open Zomato, then run this."
        ),
    )
    parser.add_argument(
        "--goto-listing",
        action="store_true",
        help=(
            "With --cdp-endpoint: go to --listing-url if the tab is not already there "
            "(same path and query → skip ``goto`` so a fully loaded page is not reloaded). "
            "After a real navigation, waits longer and scrolls once to hydrate lazy rows."
        ),
    )
    parser.add_argument(
        "--single-pass",
        action="store_true",
        help="Do not auto-scroll; collect venue links from the current DOM once (then exit).",
    )
    parser.add_argument(
        "--html-regex",
        action="store_true",
        help=(
            "Also scan raw page HTML for URL-shaped strings (adds noise). "
            "By default we still use DOM anchors + SPA globals without this."
        ),
    )
    parser.add_argument(
        "--no-spa-state",
        action="store_true",
        help=(
            "Do not read venue URLs from window.__PRELOADED_STATE__ / __NEXT_DATA__ / __NUXT__. "
            "Use only visible DOM <a href> (often only ~16 on virtualized listing pages)."
        ),
    )
    args = parser.parse_args()

    venues: dict[str, str] = {}

    if args.no_scroll and args.cdp_endpoint:
        raise SystemExit("Use either --no-scroll (CSV only) or --cdp-endpoint (browser), not both.")

    if args.no_scroll:
        src = args.discovered_in or args.out_discovered
        if not src.exists():
            raise SystemExit(f"--no-scroll requires discovered CSV at {src}")
        with src.open(encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                u = (row.get("url") or "").strip()
                n = (row.get("name") or "").strip()
                if u:
                    venues[u] = n or venues.get(u, "")
        print(f"[scroll] loaded {len(venues)} venues from {src}", flush=True)
    else:
        t0 = time.perf_counter()
        use_cdp = bool(args.cdp_endpoint and str(args.cdp_endpoint).strip())
        use_current_page = use_cdp and not args.goto_listing
        venues, listing_ref = run_scroll_collect(
            args.listing_url,
            args.city.lower(),
            headless=not args.headed,
            scroll_wait_ms=args.scroll_wait_ms,
            max_rounds=args.max_scroll_rounds,
            stable_rounds_to_stop=args.stable_rounds,
            goto_timeout_ms=args.goto_timeout_ms,
            cdp_endpoint=args.cdp_endpoint.strip() if args.cdp_endpoint else None,
            use_current_page=use_current_page,
            single_pass=args.single_pass,
            use_html_regex=args.html_regex,
            use_spa_state=not args.no_spa_state,
        )
        print(f"[scroll] collected {len(venues)} unique venues in {time.perf_counter() - t0:.1f}s", flush=True)
        write_discovered_csv(args.out_discovered, venues, (listing_ref or args.listing_url).strip())
        print(f"[scroll] wrote {args.out_discovered}", flush=True)

    if not args.skip_match:
        if not args.master.exists():
            print(f"[scroll] skip match: missing master file {args.master}", flush=True)
        else:
            text = args.master.read_text(encoding="utf-8")
            masters = parse_master_text(text)
            if not masters:
                raise SystemExit("no master rows parsed (expected ``Name - Area`` lines)")
            matched, total = write_matched_csv(
                args.out_matched,
                masters,
                venues,
                min_score=args.min_score,
                require_area_slug_hit=args.require_area_in_slug,
            )
            req = "score>=%.2f" % args.min_score
            if args.require_area_in_slug:
                req += " + area-in-slug (or score>=0.92)"
            print(f"[scroll] matched {matched}/{total} master rows ({req}) -> {args.out_matched}", flush=True)


if __name__ == "__main__":
    main()
