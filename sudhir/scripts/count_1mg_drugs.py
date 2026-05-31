#!/usr/bin/env python3
"""Count individual drug URLs in a 1mg URL list.

Reads scripts/output/one_mg_urls.txt by default and reports how many lines are
actual drug product pages vs listings, OTC, labs, etc.

Run from repository root::

    python3 scripts/count_1mg_drugs.py
    python3 scripts/count_1mg_drugs.py --url-file scripts/output/one_mg_urls.txt
"""

from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse


def path_parts(url: str) -> list[str]:
    path = urlparse(url.strip()).path
    return [p for p in path.strip("/").split("/") if p]


def classify(url: str) -> str:
    parts = path_parts(url)
    if not parts:
        return "other"

    head = parts[0]

    if head == "drugs" and len(parts) == 2:
        return "drug_page"
    if head == "generics" and len(parts) == 2:
        return "generic_page"
    if head == "otc" and len(parts) == 2:
        return "otc_page"
    if head == "drug-store" and len(parts) == 2:
        return "drug_store_page"

    if head.startswith("drugs-"):
        return "drugs_listing"
    if head == "drugs":
        return "drugs_other"

    return head.replace("-", "_") or "other"


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url-file",
        type=Path,
        default=root / "scripts" / "output" / "one_mg_urls.txt",
        help="URL list (one per line)",
    )
    args = parser.parse_args()

    if not args.url_file.is_file():
        raise SystemExit(f"URL file not found: {args.url_file}")

    counts: Counter[str] = Counter()
    seen: set[str] = set()
    duplicate_lines = 0

    with args.url_file.open(encoding="utf-8") as f:
        for line in f:
            url = line.strip()
            if not url:
                continue
            counts["total_lines"] += 1
            if url in seen:
                duplicate_lines += 1
            else:
                seen.add(url)
            counts[classify(url)] += 1

    drug_pages = counts["drug_page"]
    generic_pages = counts["generic_page"]
    prescription_like = drug_pages + generic_pages

    print(f"URL file: {args.url_file}")
    print(f"Total lines:           {counts['total_lines']:,}")
    print(f"Unique URLs:           {len(seen):,}")
    if duplicate_lines:
        print(f"Duplicate lines:       {duplicate_lines:,}")
    print()
    print("Individual drug pages (product detail URLs):")
    print(f"  /drugs/*   (branded): {drug_pages:,}")
    print(f"  /generics/*:          {generic_pages:,}")
    print(f"  Combined:             {prescription_like:,}")
    print()
    print("Other product pages:")
    print(f"  /otc/*:               {counts['otc_page']:,}")
    print(f"  /drug-store/*:        {counts['drug_store_page']:,}")
    print()
    print("Drug-related listings (not individual drugs):")
    print(f"  drugs-* paths:        {counts['drugs_listing']:,}")
    print()

    skip = {
        "total_lines",
        "drug_page",
        "generic_page",
        "otc_page",
        "drug_store_page",
        "drugs_listing",
        "drugs_other",
    }
    other = [(k, v) for k, v in counts.items() if k not in skip and v]
    if other:
        print("Other URL types (top categories):")
        for name, n in sorted(other, key=lambda x: -x[1])[:15]:
            print(f"  {name}: {n:,}")
        remaining = sum(n for k, n in other[15:])
        if remaining:
            print(f"  ... ({len(other) - 15} more categories): {remaining:,}")


if __name__ == "__main__":
    main()
