#!/usr/bin/env python3
"""
Scrape dining-only reviews for one Zomato restaurant using the same URL shape as the site:
  {base}/reviews?page=N&sort=dd&filter=reviews-dining

Default POC: The Biere Club, Lavelle Road. Reuses zomato-scraper/review_scraper.get_reviews.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRAPER_DIR = REPO_ROOT / "zomato-scraper"
if str(SCRAPER_DIR) not in sys.path:
    sys.path.insert(0, str(SCRAPER_DIR))

from review_scraper import get_reviews  # noqa: E402

DEFAULT_RESTAURANT_URL = (
    "https://www.zomato.com/bangalore/the-biere-club-lavelle-road-bangalore"
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Dining-only Zomato review scrape (sort=dd).")
    parser.add_argument(
        "--url",
        default=DEFAULT_RESTAURANT_URL,
        help="Restaurant page URL (with or without /reviews).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=REPO_ROOT / "beast/output/the-biere-club_dining_reviews.csv",
        help="Output CSV path.",
    )
    parser.add_argument(
        "--delay-sec",
        type=float,
        default=0.25,
        help="Pause between page requests (be polite to Zomato).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=1007,
        help="Hard cap on listing pages per filter (safety). Stops earlier when Zomato has no more pages.",
    )
    parser.add_argument(
        "--max-reviews",
        type=int,
        default=None,
        help="Optional cap on unique reviews (for quick tests).",
    )
    parser.add_argument("--quiet", action="store_true", help="Disable per-page progress lines.")
    args = parser.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)

    print(
        f"[beast] dining-only, sort=new (dd), url={args.url}\n"
        f"[beast] writing -> {args.out}",
        flush=True,
    )

    df = get_reviews(
        args.url,
        max_reviews=args.max_reviews,
        sort="new",
        save=False,
        save_empty=False,
        filters=("reviews-dining",),
        safety_max_pages=args.max_pages,
        delay_sec=args.delay_sec,
        progress_log=not args.quiet,
    )

    scraped_at = datetime.now(timezone.utc).isoformat()
    df = df.copy()
    df.insert(0, "restaurant_url", args.url.split("?")[0].rstrip("/").removesuffix("/reviews"))
    df.insert(1, "review_filter", "reviews-dining")
    df.insert(2, "scraped_at_utc", scraped_at)

    df.to_csv(args.out, index=False)
    print(f"[beast] done: {len(df)} rows -> {args.out}", flush=True)


if __name__ == "__main__":
    main()
