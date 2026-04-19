#!/usr/bin/env python3
"""
Collect normalized Zomato restaurant URLs using many listing entry points:

  • Central + N/S/E/W dine-out corridors + HSR (explicit seeds)
  • Classic delivery / restaurants / dine-out
  • Curated hubs linked from /{city}/collections (optional Playwright if HTML is empty)

Output is replaced on each run (existing CSV removed first). Reuses
bulk_scraper.discover_restaurants_from_seeds + URL/name filters in bulk_scraper.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRAPER_DIR = REPO_ROOT / "zomato-scraper"
if str(SCRAPER_DIR) not in sys.path:
    sys.path.insert(0, str(SCRAPER_DIR))

from bulk_scraper import (  # noqa: E402
    LISTING_HTTP_HEADERS,
    collection_subpage_seeds_from_collections_index_html,
    discover_restaurants_from_seeds,
)


def _default_area_seeds(city: str) -> list[str]:
    c = city.lower().strip()
    return [
        f"https://www.zomato.com/{c}/central-bangalore-restaurants",
        f"https://www.zomato.com/{c}/dine-out-in-north-bangalore",
        f"https://www.zomato.com/{c}/dine-out-in-south-bangalore",
        f"https://www.zomato.com/{c}/dine-out-in-east-bangalore",
        f"https://www.zomato.com/{c}/dine-out-in-west-bangalore",
        f"https://www.zomato.com/{c}/dine-out-in-hsr",
    ]


def _classic_seeds(city: str) -> list[str]:
    c = city.lower().strip()
    return [
        f"https://www.zomato.com/{c}/delivery",
        f"https://www.zomato.com/{c}/restaurants",
        f"https://www.zomato.com/{c}/dine-out",
    ]


def _uniq_preserve(seq: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in seq:
        x = (x or "").strip()
        if not x or x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _fetch_collections_index_html(city: str, *, use_playwright: bool, playwright_wait_ms: int) -> str:
    url = f"https://www.zomato.com/{city.lower()}/collections"
    if not use_playwright:
        r = requests.get(url, headers=LISTING_HTTP_HEADERS, timeout=45)
        if r.status_code != 200:
            return ""
        return r.text

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("[discover_urls] Playwright not installed; skipping collections browser fetch.", flush=True)
        return ""

    from playwright_chromium import chromium_launch_kwargs  # noqa: E402

    with sync_playwright() as p:
        browser = p.chromium.launch(**chromium_launch_kwargs())
        page = browser.new_page(
            user_agent=LISTING_HTTP_HEADERS["User-Agent"],
            viewport={"width": 1280, "height": 1800},
        )
        page.goto(url, wait_until="commit", timeout=120000)
        try:
            page.wait_for_load_state("domcontentloaded", timeout=30000)
        except Exception:
            pass
        page.wait_for_timeout(playwright_wait_ms)
        html = page.content()
        browser.close()
    return html


def _build_seed_list(
    city: str,
    *,
    include_areas: bool,
    include_classic: bool,
    include_collections: bool,
    collections_playwright: bool,
    collections_playwright_wait_ms: int,
    extra_seeds: list[str],
) -> list[str]:
    seeds: list[str] = list(extra_seeds)
    if include_areas:
        seeds.extend(_default_area_seeds(city))
    if include_classic:
        seeds.extend(_classic_seeds(city))

    if include_collections:
        html = _fetch_collections_index_html(
            city,
            use_playwright=collections_playwright,
            playwright_wait_ms=collections_playwright_wait_ms,
        )
        if html:
            sub = collection_subpage_seeds_from_collections_index_html(html, city)
            print(f"[discover_urls] collections index -> {len(sub)} hub sub-seeds", flush=True)
            seeds.extend(sub)
        else:
            print("[discover_urls] collections index fetch returned empty HTML", flush=True)

    return _uniq_preserve(seeds)


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-seed Zomato restaurant URL discovery.")
    parser.add_argument("--city", default="bangalore", help="City slug in zomato.com URLs.")
    parser.add_argument("--max-restaurants", type=int, default=25000)
    parser.add_argument("--max-pages", type=int, default=120, help="Max listing pages per seed URL.")
    parser.add_argument("--stale-pages", type=int, default=6)
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output CSV (default: beast/discover_urls/output/{city}_restaurant_urls.csv).",
    )
    parser.add_argument("--no-areas", action="store_true", help="Skip central + NSEW + HSR seeds.")
    parser.add_argument("--no-classic", action="store_true", help="Skip delivery/restaurants/dine-out.")
    parser.add_argument("--no-collections", action="store_true", help="Skip /collections hub expansion.")
    parser.add_argument(
        "--collections-playwright",
        action="store_true",
        help="Load /collections with Chromium (use when static HTML has almost no links).",
    )
    parser.add_argument(
        "--collections-wait-ms",
        type=int,
        default=9000,
        help="Extra wait after load when using Playwright on /collections.",
    )
    parser.add_argument(
        "--extra-seed",
        action="append",
        default=[],
        help="Additional listing URL (repeatable). Merged with built-in seeds.",
    )
    args = parser.parse_args()

    out = args.out
    if out is None:
        out = REPO_ROOT / f"beast/discover_urls/output/{args.city.lower()}_restaurant_urls.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        out.unlink()

    seeds = _build_seed_list(
        args.city,
        include_areas=not args.no_areas,
        include_classic=not args.no_classic,
        include_collections=not args.no_collections,
        collections_playwright=args.collections_playwright,
        collections_playwright_wait_ms=args.collections_wait_ms,
        extra_seeds=args.extra_seed,
    )

    print(
        f"[discover_urls] city={args.city} unique_seeds={len(seeds)} "
        f"max_restaurants={args.max_restaurants} max_pages={args.max_pages} stale={args.stale_pages}",
        flush=True,
    )
    t0 = time.perf_counter()

    rows = discover_restaurants_from_seeds(
        args.city.lower(),
        seeds,
        max_restaurants=args.max_restaurants,
        max_scrolls=args.max_pages,
        listing_pages_without_growth_limit=args.stale_pages,
    )

    scraped_at = datetime.now(timezone.utc).isoformat()
    fieldnames = ("url", "name", "rating", "source_page", "rank", "discovered_at_utc")

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(
                {
                    "url": row["url"],
                    "name": row.get("name") or "",
                    "rating": row.get("rating") if row.get("rating") is not None else "",
                    "source_page": row.get("source_page") or "",
                    "rank": row.get("rank", ""),
                    "discovered_at_utc": scraped_at,
                }
            )

    elapsed = time.perf_counter() - t0
    print(f"[discover_urls] wrote {len(rows)} rows -> {out} ({elapsed:.1f}s)", flush=True)


if __name__ == "__main__":
    main()
