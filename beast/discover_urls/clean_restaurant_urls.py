#!/usr/bin/env python3
"""
Turn a noisy discover_urls CSV into restaurant-only rows.

Default filters (fast, no per-URL GET):
  - drop locality hub URLs ending in -restaurants
  - drop junk listing anchor names (same heuristics as bulk_scraper.is_junk_listing_anchor_name)

Optional:
  --verify-jsonld  GET each venue page once; keep only JSON-LD @type=Restaurant (same gate as beast pipeline).
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRAPER_DIR = REPO_ROOT / "zomato-scraper"
if str(SCRAPER_DIR) not in sys.path:
    sys.path.insert(0, str(SCRAPER_DIR))

from bulk_scraper import BASE_URL, is_junk_listing_anchor_name  # noqa: E402
from urllib.parse import urljoin  # noqa: E402


def _is_locality_hub_url(url: str) -> bool:
    u = (url or "").split("?")[0].rstrip("/").lower()
    seg = u.rsplit("/", 1)[-1] if u else ""
    return seg.endswith("-restaurants")


def main() -> None:
    parser = argparse.ArgumentParser(description="Filter discovered rows to restaurant URLs only.")
    parser.add_argument(
        "--in",
        dest="in_path",
        type=Path,
        default=REPO_ROOT / "beast/discover_urls/output/bangalore_restaurant_urls.csv",
        help="Input CSV from fetch_restaurant_urls.py",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=REPO_ROOT / "beast/discover_urls/output/bangalore_restaurant_urls_clean.csv",
        help="Output CSV (restaurant rows only).",
    )
    parser.add_argument(
        "--verify-jsonld",
        action="store_true",
        help="HTTP GET each URL; keep only pages with JSON-Ld Restaurant (slow but strict).",
    )
    parser.add_argument("--verify-sleep-sec", type=float, default=0.2, help="Pause between GETs when verifying.")
    args = parser.parse_args()

    if args.verify_jsonld:
        print(
            f"[clean_urls] verify-jsonld on (sleep {args.verify_sleep_sec}s/URL) — this will take a while.",
            flush=True,
        )

    if not args.in_path.exists():
        raise SystemExit(f"missing input: {args.in_path}")

    from info_scraper import get_info  # noqa: E402

    rows_in = list(csv.DictReader(args.in_path.open(encoding="utf-8")))
    kept: list[dict] = []
    stats = {"in": len(rows_in), "hub_drop": 0, "junk_name_drop": 0, "jsonld_fail": 0, "out": 0}

    seen: set[str] = set()
    verify_n = 0
    for row in rows_in:
        url = (row.get("url") or "").strip()
        name = row.get("name") or ""
        if not url:
            continue
        if _is_locality_hub_url(url):
            stats["hub_drop"] += 1
            continue
        if is_junk_listing_anchor_name(name):
            stats["junk_name_drop"] += 1
            continue
        if url in seen:
            continue
        seen.add(url)

        if args.verify_jsonld:
            verify_n += 1
            if verify_n % 25 == 1:
                print(f"[clean_urls] verify-jsonld progress {verify_n}…", flush=True)
            try:
                info = get_info(url)
            except Exception:
                info = None
            if not info or info[0] != "Restaurant" or not info[2]:
                stats["jsonld_fail"] += 1
                continue
            row = dict(row)
            canon = info[2].strip()
            row["url"] = urljoin(BASE_URL, canon) if canon.startswith("/") else canon
            row["name"] = (info[1] or row.get("name") or "").strip()
            if info[16] is not None and str(info[16]).strip():
                row["rating"] = str(info[16]).strip()
            if args.verify_sleep_sec:
                time.sleep(args.verify_sleep_sec)

        kept.append(row)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows_in[0].keys()) if rows_in else ["url", "name", "rating", "source_page", "rank"]
    with args.out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in kept:
            w.writerow({k: row.get(k, "") for k in fieldnames})

    stats["out"] = len(kept)
    print(
        f"[clean_urls] in={stats['in']} hub_drop={stats['hub_drop']} junk_name_drop={stats['junk_name_drop']} "
        f"jsonld_fail={stats['jsonld_fail']} out={stats['out']} -> {args.out}",
        flush=True,
    )


if __name__ == "__main__":
    main()
