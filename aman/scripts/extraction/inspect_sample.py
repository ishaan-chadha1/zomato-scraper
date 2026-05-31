#!/usr/bin/env python3
"""Summarize the sample extraction outputs for human review.

Prints, for each review:
  - the original review text (truncated)
  - the extracted JSON (non-null fields only)
  - any pydantic validation warnings

And at the end:
  - aggregate stats: coverage per section, common warning patterns
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def strip_nulls(obj):
    """Recursively drop null / empty fields so we can read what was actually extracted."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            v2 = strip_nulls(v)
            if v2 in (None, [], {}, ""):
                continue
            out[k] = v2
        return out
    if isinstance(obj, list):
        return [strip_nulls(x) for x in obj if strip_nulls(x) not in (None, [], {}, "")]
    return obj


def truncate(s: str, n: int) -> str:
    s = s.replace("\n", " ").strip()
    return s if len(s) <= n else s[:n] + "…"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--combined",
        type=Path,
        default=ROOT / "data/llm_cache/sample/sample_combined.json",
    )
    p.add_argument(
        "--per-review-chars",
        type=int,
        default=350,
        help="how many chars of review text to show per record",
    )
    args = p.parse_args()

    if not args.combined.exists():
        print(f"ERROR: {args.combined} not found", file=sys.stderr)
        return 1

    with open(args.combined) as f:
        records = json.load(f)

    section_coverage: Counter[str] = Counter()
    warning_counter: Counter[str] = Counter()
    total_with_extraction = 0

    for i, rec in enumerate(records):
        extracted = rec.get("extracted") or {}
        if not extracted or "_error" in extracted or "_parse_error" in extracted:
            continue
        total_with_extraction += 1
        for section_name in extracted:
            section_coverage[section_name] += 1
        for w in rec.get("validation_warnings", []):
            short = w.split("\n")[0][:120]
            warning_counter[short] += 1

    print("=" * 78)
    print("PER-REVIEW DETAIL")
    print("=" * 78)
    for i, rec in enumerate(records):
        print()
        print(f"--- [{i:02d}] {rec['restaurant']}  r={rec['rating']}  src={rec['source']}")
        print(f"    REVIEW ({rec['description_len']} chars):")
        text = rec["description"] or ""
        print(f"      {truncate(text, args.per_review_chars)}")
        extracted = rec.get("extracted") or {}
        clean = strip_nulls(extracted)
        print(f"    EXTRACTED ({rec['input_tokens']}in / {rec['output_tokens']}out tok):")
        if not clean:
            print("      {}  (empty extraction)")
        else:
            ext_json = json.dumps(clean, indent=2, ensure_ascii=False)
            for line in ext_json.split("\n"):
                print(f"      {line}")
        warnings = rec.get("validation_warnings", [])
        if warnings:
            print("    WARNINGS:")
            for w in warnings:
                print(f"      ~ {truncate(w, 400)}")

    print()
    print("=" * 78)
    print("AGGREGATE")
    print("=" * 78)
    print(f"Total records:               {len(records)}")
    print(f"With non-empty extraction:   {total_with_extraction}")
    print()
    print("Section coverage (out of records with extraction):")
    for section, count in section_coverage.most_common():
        pct = 100 * count / max(total_with_extraction, 1)
        print(f"  {section:18s} {count:>3d}  ({pct:5.1f}%)")
    print()
    print("Validation warnings (top):")
    if not warning_counter:
        print("  (none)")
    for w, c in warning_counter.most_common(20):
        print(f"  {c:>3d}x  {w}")
    print()
    print("=" * 78)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
