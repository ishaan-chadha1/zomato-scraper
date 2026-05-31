#!/usr/bin/env python3
"""Scrape MyMyelomaTeam resource listings and articles using Playwright (real browser).

By default, discovers article links from **all** resource hubs × **all** sort orders,
dedupes by URL, then fetches each article **once**. On each article page it tries to
click **Read full article** (and similar labels) so collapsed body text is in the DOM
before HTML extraction.

- Hubs: ``types``, ``treatments``, ``topics``, ``roles``, ``multiple-diagnoses``
- Sorts: ``recent`` (no query), ``most_liked`` (``?sort=most_liked``),
  ``most_comments`` (``?sort=most_comments``)

Use ``--listing-url`` only for a single listing page (legacy / debugging).

Setup (pick one)::

    pip install playwright
    python3 -m playwright install chromium

Or use your existing **Google Chrome** and skip the bundled Chromium download::

    python3 scrapers/resources/mymyelomateam_resources_playwright.py --channel chrome --headed …

Examples::

    python3 scrapers/resources/mymyelomateam_resources_playwright.py --progress --headed

    python3 scrapers/resources/mymyelomateam_resources_playwright.py \\
      --hubs types --sorts most_liked --progress
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd

# Load sibling module without package imports (script may be run by path).
_HTTP_PATH = Path(__file__).resolve().parent / "mymyelomateam_resources_http.py"
_spec = importlib.util.spec_from_file_location("_mmm_http", _HTTP_PATH)
if _spec is None or _spec.loader is None:
    raise RuntimeError(f"Cannot load {_HTTP_PATH}")
_http = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_http)

DEFAULT_RESOURCE_HUBS = _http.DEFAULT_RESOURCE_HUBS
DEFAULT_LISTING_SORTS = _http.DEFAULT_LISTING_SORTS
resource_listing_url = _http.resource_listing_url
extract_urls_from_html = _http.extract_urls_from_html
extract_urls_from_next_data = _http.extract_urls_from_next_data
parse_declared_listing_total = _http.parse_declared_listing_total
article_row_from_html = _http.article_row_from_html
save_articles_csv = _http.save_articles_csv


def _dismiss_common_overlays(page) -> None:
    """Best-effort cookie / consent clicks; ignore failures."""
    candidates = [
        'button:has-text("Accept")',
        'button:has-text("Accept All")',
        'button:has-text("Agree")',
        'button:has-text("I Accept")',
        '[aria-label="Accept"]',
        'button:has-text("Continue")',
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            if loc.is_visible(timeout=600):
                loc.click(timeout=2000)
                page.wait_for_timeout(400)
                return
        except Exception:
            continue


def collect_listing_urls_playwright(
    page,
    listing_url: str,
    *,
    max_show_more_rounds: int,
    progress_log: bool,
    settle_ms: int,
    log_tag: str | None = None,
) -> list[str]:
    page.goto(listing_url, wait_until="domcontentloaded", timeout=90_000)
    page.wait_for_timeout(1500)
    _dismiss_common_overlays(page)

    all_urls: set[str] = set()
    declared: int | None = None
    stagnant = 0
    prev_n = -1

    def merge_from_dom() -> None:
        """Live DOM often has root-relative links before ``page.content()`` catches up."""
        try:
            href_list = page.eval_on_selector_all(
                'a[href^="/resources/"]',
                "els => els.map(e => e.getAttribute('href')).filter(Boolean)",
            )
        except Exception:
            href_list = []
        for h in href_list or []:
            u = _http.normalize_article_url(str(h))
            if u:
                all_urls.add(u)

    for rnd in range(max_show_more_rounds):
        html = page.content()
        all_urls |= extract_urls_from_html(html)
        all_urls |= extract_urls_from_next_data(html)
        merge_from_dom()
        if declared is None:
            declared = parse_declared_listing_total(html)

        n = len(all_urls)
        if progress_log:
            decl = f" declared={declared}" if declared is not None else ""
            prefix = f"[listing] {log_tag} " if log_tag else "[listing] "
            print(f"{prefix}round={rnd + 1} unique_urls={n}{decl}", flush=True)

        if declared is not None and n >= declared:
            break

        clicked = False
        show = page.get_by_role("button", name=re.compile(r"show\s*more", re.I))
        if show.count() == 0:
            show = page.get_by_role("link", name=re.compile(r"show\s*more", re.I))
        if show.count() == 0:
            show = page.locator("text=/show\\s+more/i")
        try:
            if show.count() > 0:
                btn = show.first
                if btn.is_visible(timeout=800):
                    btn.scroll_into_view_if_needed(timeout=5000)
                    btn.click(timeout=8000)
                    clicked = True
                    page.wait_for_timeout(settle_ms)
        except Exception:
            pass

        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1200)
        merge_from_dom()
        html2 = page.content()
        all_urls |= extract_urls_from_html(html2)
        all_urls |= extract_urls_from_next_data(html2)
        merge_from_dom()

        n2 = len(all_urls)
        if n2 == prev_n and not clicked:
            stagnant += 1
        else:
            stagnant = 0
        prev_n = n2

        if stagnant >= 4:
            break

    if declared is not None and len(all_urls) < declared:
        tag = log_tag or "listing"
        print(
            f"[listing] Warning ({tag}): heading claims {declared} items but collected "
            f"{len(all_urls)} article URLs. Try --headed, increase --settle-ms, "
            f"or raise --max-show-more-rounds.",
            flush=True,
        )

    return sorted(all_urls)


# Accessible names for controls that reveal the full article body (not generic "Read more" sitewide).
_EXPAND_NAME_RES = (
    re.compile(r"read\s+full\s+article", re.I),
    re.compile(r"read\s+the\s+full\s+article", re.I),
    re.compile(r"show\s+full\s+article", re.I),
    re.compile(r"see\s+full\s+article", re.I),
)


def _try_expand_full_article(page, *, settle_ms: int) -> None:
    """If the article body is behind an expand control, click it and wait for DOM to settle."""
    roots: list = [
        page.locator("article").first,
        page.locator("main").first,
        page.locator('[role="main"]').first,
    ]

    def try_on_root(root) -> bool:
        if root.count() == 0:
            return False
        try:
            if not root.is_visible(timeout=400):
                return False
        except Exception:
            return False
        for pat in _EXPAND_NAME_RES:
            for role in ("button", "link"):
                try:
                    loc = root.get_by_role(role, name=pat)
                    if loc.count() == 0:
                        continue
                    btn = loc.first
                    if not btn.is_visible(timeout=1_200):
                        continue
                    btn.scroll_into_view_if_needed(timeout=5_000)
                    btn.click(timeout=8_000)
                    page.wait_for_timeout(settle_ms)
                    return True
                except Exception:
                    continue
        return False

    for root in roots:
        if try_on_root(root):
            return

    for pat in _EXPAND_NAME_RES:
        for role in ("button", "link"):
            try:
                loc = page.get_by_role(role, name=pat)
                if loc.count() == 0:
                    continue
                btn = loc.first
                if not btn.is_visible(timeout=1_200):
                    continue
                btn.scroll_into_view_if_needed(timeout=5_000)
                btn.click(timeout=8_000)
                page.wait_for_timeout(settle_ms)
                return
            except Exception:
                continue
    # Fallback: styled control outside strict ARIA naming
    try:
        loc = page.locator("a, button, [role='button']").filter(has_text=re.compile(r"full\s+article", re.I))
        if loc.count() > 0:
            first = loc.first
            if first.is_visible(timeout=800):
                first.scroll_into_view_if_needed(timeout=5_000)
                first.click(timeout=8_000)
                page.wait_for_timeout(settle_ms)
    except Exception:
        pass


def scrape_article_playwright(
    page,
    url: str,
    *,
    goto_timeout_ms: int,
    expand_full_article: bool = True,
    expand_settle_ms: int = 2_500,
) -> dict[str, Any]:
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=goto_timeout_ms)
        page.wait_for_timeout(600)
        _dismiss_common_overlays(page)
        if expand_full_article:
            _try_expand_full_article(page, settle_ms=expand_settle_ms)
        return article_row_from_html(url, page.content())
    except Exception as e:
        return article_row_from_html(url, "", error=str(e))


def _parse_csv_choice(s: str, *, allowed: frozenset[str], label: str) -> list[str]:
    parts = [x.strip().lower() for x in s.split(",") if x.strip()]
    bad = [x for x in parts if x not in allowed]
    if bad:
        raise SystemExit(f"Unknown {label}: {bad!r}; allowed: {sorted(allowed)}")
    return parts


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("Examples::")[0].strip())
    p.add_argument(
        "--listing-url",
        default=None,
        help="If set, only this listing page is scraped (ignores --hubs / --sorts).",
    )
    p.add_argument(
        "--hubs",
        default=",".join(DEFAULT_RESOURCE_HUBS),
        help=f"Comma-separated hub slugs (default: {','.join(DEFAULT_RESOURCE_HUBS)})",
    )
    p.add_argument(
        "--sorts",
        default=",".join(DEFAULT_LISTING_SORTS),
        help="Comma-separated: recent, most_liked, most_comments (default: all three)",
    )
    p.add_argument(
        "--max-show-more-rounds",
        type=int,
        default=400,
        help="Upper bound on scroll / Show more iterations (default 400)",
    )
    p.add_argument(
        "--settle-ms",
        type=int,
        default=2000,
        help="Wait after each Show more click for new DOM (default 2000)",
    )
    p.add_argument("--max-articles", type=int, default=None, help="Cap articles scraped")
    p.add_argument("--delay", type=float, default=0.35, help="Seconds between article page loads")
    p.add_argument(
        "--output",
        default=os.path.join("data", "MyMyelomaTeam", "articles_playwright.csv"),
        help="Output CSV path",
    )
    p.add_argument(
        "--channel",
        choices=("chrome", "chrome-beta", "msedge"),
        default=None,
        help="Launch installed Chrome/Edge instead of Playwright's Chromium "
        "(use if you have not run: python3 -m playwright install chromium).",
    )
    p.add_argument("--slowmo", type=float, default=0.0, help="Playwright slow_mo ms (debug)")
    p.add_argument(
        "--headed",
        action="store_true",
        help="Show the browser window (default: headless Chromium)",
    )
    p.add_argument("--progress", action="store_true", help="Log progress")
    p.add_argument("--no-save", action="store_true", help="Do not write CSV")
    p.add_argument(
        "--article-timeout-ms",
        type=int,
        default=90_000,
        help="Navigation timeout per article (default 90000)",
    )
    p.add_argument(
        "--no-expand-full-article",
        action="store_true",
        help="Skip clicking Read full article (and similar) before extracting HTML",
    )
    p.add_argument(
        "--expand-settle-ms",
        type=int,
        default=2_500,
        help="Milliseconds to wait after expand click for body to render (default 2500)",
    )
    p.add_argument(
        "--no-wipe-output",
        action="store_true",
        help="Keep an existing output CSV; default is to delete it before writing.",
    )
    return p


def main() -> int:
    args = build_parser().parse_args()

    sort_allowed = frozenset(DEFAULT_LISTING_SORTS)
    hub_allowed = frozenset(DEFAULT_RESOURCE_HUBS)

    listing_jobs: list[tuple[str, str, str]]
    if args.listing_url:
        listing_jobs = [("custom", "custom", args.listing_url.strip())]
    else:
        hubs = _parse_csv_choice(args.hubs, allowed=hub_allowed, label="hub in --hubs")
        sorts = _parse_csv_choice(args.sorts, allowed=sort_allowed, label="sort in --sorts")
        listing_jobs = []
        for hub in hubs:
            for sort in sorts:
                listing_jobs.append((hub, sort, resource_listing_url(hub, sort)))

    out_path = Path(args.output).resolve()
    if not args.no_save and not args.no_wipe_output and out_path.is_file():
        out_path.unlink()
        if args.progress:
            print(f"[output] Removed existing {out_path}", flush=True)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print(
            "Install Playwright: pip install playwright\n"
            "Then either:\n"
            "  python3 -m playwright install chromium\n"
            "or re-run this script with --channel chrome (Google Chrome must be installed).",
            file=sys.stderr,
        )
        return 1

    all_article_urls: set[str] = set()
    rows: list[dict[str, Any]] = []

    with sync_playwright() as p:
        launch_kw: dict[str, Any] = {
            "headless": not args.headed,
            "slow_mo": int(args.slowmo) if args.slowmo else 0,
        }
        if args.channel:
            launch_kw["channel"] = args.channel
        try:
            browser = p.chromium.launch(**launch_kw)
        except Exception as e:
            err = str(e).lower()
            if "executable" in err or "doesn't exist" in err:
                print(
                    "Playwright could not find a browser executable.\n"
                    "Fix one of:\n"
                    "  1) Run: python3 -m playwright install chromium\n"
                    "  2) Or install Google Chrome and re-run with: --channel chrome\n",
                    file=sys.stderr,
                )
            raise
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        page = context.new_page()

        for hub, sort, listing_url in listing_jobs:
            if args.progress:
                print(f"[listing] === {hub} / {sort} === {listing_url}", flush=True)
            before = len(all_article_urls)
            found = collect_listing_urls_playwright(
                page,
                listing_url,
                max_show_more_rounds=args.max_show_more_rounds,
                progress_log=args.progress,
                settle_ms=args.settle_ms,
                log_tag=f"{hub}/{sort}",
            )
            all_article_urls.update(found)
            if args.progress:
                print(
                    f"[listing] {hub}/{sort} +{len(all_article_urls) - before} new "
                    f"| deduped_total={len(all_article_urls)}",
                    flush=True,
                )
            if args.delay and len(listing_jobs) > 1:
                time.sleep(args.delay)

        urls = sorted(all_article_urls)
        if not urls:
            print("[listing] No article URLs found.", file=sys.stderr)
            context.close()
            browser.close()
            return 2

        if args.max_articles is not None:
            urls = urls[: max(0, args.max_articles)]

        if args.progress:
            print(f"[article] Scraping {len(urls)} unique URLs once each", flush=True)

        for i, u in enumerate(urls, start=1):
            if args.progress:
                print(f"[article] {i}/{len(urls)} {u}", flush=True)
            rows.append(
                scrape_article_playwright(
                    page,
                    u,
                    goto_timeout_ms=args.article_timeout_ms,
                    expand_full_article=not args.no_expand_full_article,
                    expand_settle_ms=args.expand_settle_ms,
                )
            )
            if args.delay:
                time.sleep(args.delay)

        context.close()
        browser.close()

    if args.progress:
        errs = sum(1 for r in rows if r.get("error"))
        print(f"[done] articles={len(rows)} errors={errs}", flush=True)

    if not args.no_save:
        save_articles_csv(rows, args.output)
    else:
        print(pd.DataFrame(rows).head(3).to_string(), flush=True)
        print(f"\nRows: {len(rows)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
