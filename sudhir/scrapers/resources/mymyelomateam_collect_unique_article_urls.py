#!/usr/bin/env python3
"""Discover all unique MyMyelomaTeam resource article URLs across hub listing pages.

This script **only** runs the listing phase (scroll + Show more + DOM merge). It does
**not** open each article or click **Read full article** — use that for a deduped URL
inventory before full article scraping.

Default coverage matches the main Playwright scraper:

- Hubs: ``types``, ``treatments``, ``topics``, ``roles``, ``multiple-diagnoses``
- Sorts: ``recent``, ``most_liked``, ``most_comments``

Outputs:

- A **text file** (default): one canonical ``https://www.mymyelomateam.com/resources/…``
  URL per line, sorted.
- Optional **JSON summary**: per listing job counts and declared heading totals when found.

Example::

    python3 scrapers/resources/mymyelomateam_collect_unique_article_urls.py --progress

    python3 scrapers/resources/mymyelomateam_collect_unique_article_urls.py \\
      --channel chrome --headed --progress \\
      --output data/MyMyelomaTeam/unique_article_urls.txt \\
      --summary-json data/MyMyelomaTeam/unique_article_urls_summary.json
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
from pathlib import Path
from typing import Any

# Reuse listing helpers from the Playwright scraper (same browser behavior).
_PW_PATH = Path(__file__).resolve().parent / "mymyelomateam_resources_playwright.py"
_spec = importlib.util.spec_from_file_location("_mmm_pw", _PW_PATH)
if _spec is None or _spec.loader is None:
    raise RuntimeError(f"Cannot load {_PW_PATH}")
_pw = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_pw)

collect_listing_urls_playwright = _pw.collect_listing_urls_playwright
resource_listing_url = _pw.resource_listing_url
DEFAULT_RESOURCE_HUBS = _pw.DEFAULT_RESOURCE_HUBS
DEFAULT_LISTING_SORTS = _pw.DEFAULT_LISTING_SORTS


def _parse_csv_choice(s: str, *, allowed: frozenset[str], label: str) -> list[str]:
    parts = [x.strip().lower() for x in s.split(",") if x.strip()]
    bad = [x for x in parts if x not in allowed]
    if bad:
        raise SystemExit(f"Unknown {label}: {bad!r}; allowed: {sorted(allowed)}")
    return parts


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("Example::")[0].strip())
    p.add_argument(
        "--output",
        default=str(Path("data") / "MyMyelomaTeam" / "unique_article_urls.txt"),
        help="Write one article URL per line (UTF-8)",
    )
    p.add_argument(
        "--summary-json",
        default=None,
        help="Optional path to write per-job stats and declared totals as JSON",
    )
    p.add_argument(
        "--listing-url",
        default=None,
        help="If set, only this listing page is used (ignores --hubs / --sorts)",
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
        help="Upper bound on scroll / Show more iterations per listing (default 400)",
    )
    p.add_argument(
        "--settle-ms",
        type=int,
        default=2000,
        help="Wait after each Show more click for new DOM (default 2000)",
    )
    p.add_argument(
        "--delay",
        type=float,
        default=0.35,
        help="Seconds between listing jobs when multiple (default 0.35)",
    )
    p.add_argument(
        "--channel",
        choices=("chrome", "chrome-beta", "msedge"),
        default=None,
        help="Launch installed Chrome/Edge instead of bundled Chromium",
    )
    p.add_argument("--slowmo", type=float, default=0.0, help="Playwright slow_mo ms (debug)")
    p.add_argument("--headed", action="store_true", help="Show the browser window")
    p.add_argument("--progress", action="store_true", help="Log progress")
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

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print(
            "Install Playwright: pip install playwright\n"
            "Then either:\n"
            "  python3 -m playwright install chromium\n"
            "or re-run with --channel chrome (Google Chrome must be installed).",
            file=sys.stderr,
        )
        return 1

    all_article_urls: set[str] = set()
    job_rows: list[dict[str, Any]] = []

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
            before_global = len(all_article_urls)
            found_list = collect_listing_urls_playwright(
                page,
                listing_url,
                max_show_more_rounds=args.max_show_more_rounds,
                progress_log=args.progress,
                settle_ms=args.settle_ms,
                log_tag=f"{hub}/{sort}",
            )
            found_set = set(found_list)
            new_here = len(found_set - all_article_urls)
            all_article_urls.update(found_set)
            job_rows.append(
                {
                    "hub": hub,
                    "sort": sort,
                    "listing_url": listing_url,
                    "urls_on_page": len(found_set),
                    "new_vs_prior_jobs": new_here,
                    "deduped_total_so_far": len(all_article_urls),
                }
            )
            if args.progress:
                print(
                    f"[listing] {hub}/{sort} job_unique={len(found_set)} "
                    f"+{len(all_article_urls) - before_global} new this job "
                    f"| deduped_total={len(all_article_urls)}",
                    flush=True,
                )
            if args.delay and len(listing_jobs) > 1:
                time.sleep(args.delay)

        context.close()
        browser.close()

    urls_sorted = sorted(all_article_urls)
    out_path = Path(args.output).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(urls_sorted) + ("\n" if urls_sorted else ""), encoding="utf-8")

    if args.progress:
        print(f"[done] unique_article_urls={len(urls_sorted)} → {out_path}", flush=True)

    if args.summary_json:
        summary_path = Path(args.summary_json).resolve()
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "unique_count": len(urls_sorted),
            "output_txt": str(out_path),
            "jobs": job_rows,
        }
        summary_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        if args.progress:
            print(f"[done] summary → {summary_path}", flush=True)

    return 0 if urls_sorted else 2


if __name__ == "__main__":
    raise SystemExit(main())
