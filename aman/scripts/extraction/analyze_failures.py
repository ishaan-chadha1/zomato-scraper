#!/usr/bin/env python3
"""Catalog every prompt issue from a sample run.

Reads the `sample_combined.json` from a run and produces:
  - per-field invalid-value frequency
  - missing-required-field counts
  - schema-shape errors (list vs dict, etc.)
  - made-up field names (not in schema)
  - API-error vs validation-warning split
  - examples for each failure mode

Output is plain text, designed to drive prompt iteration.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from extraction.schema import ReviewExtraction


def collect_valid_field_names() -> set[str]:
    """Every legal field name appearing anywhere in the schema."""
    seen: set[str] = set()

    def walk(model_cls):
        if not hasattr(model_cls, "model_fields"):
            return
        for fname, fi in model_cls.model_fields.items():
            seen.add(fname)
            ann = fi.annotation
            # Recurse into nested models (Optional[X] / list[X])
            for sub in _candidates(ann):
                walk(sub)

    walk(ReviewExtraction)
    return seen


def _candidates(ann):
    """Yield model classes referenced by an annotation."""
    args = getattr(ann, "__args__", ())
    if args:
        for a in args:
            yield from _candidates(a)
    if hasattr(ann, "model_fields"):
        yield ann


def collect_legal_keys_per_path() -> dict[str, set[str]]:
    """For each top-level section, the set of legal sub-field names.

    Example: 'context' -> {'companions', 'group_size_exact', ...}
    Used to detect made-up field names.
    """
    by_section: dict[str, set[str]] = {}
    for sname, fi in ReviewExtraction.model_fields.items():
        ann = fi.annotation
        # Unwrap Optional / list
        for sub in _candidates(ann):
            by_section[sname] = set(sub.model_fields.keys())
            break
    return by_section


def parse_pydantic_warning(warn: str) -> list[dict]:
    """Pull individual (path, kind, value, allowed) tuples out of a pydantic warning blob."""
    out: list[dict] = []
    # Each error block starts with the path on its own line (indented or not), followed by
    # an "Input should be ..." or other diagnostic. We just split on '\n  ' as a heuristic.
    blocks = re.split(r"\n(?=[a-zA-Z_][\w\.]*\n)", warn)
    for block in blocks:
        lines = block.splitlines()
        if not lines:
            continue
        path = lines[0].strip()
        if not path or path.startswith("pydantic_validation"):
            continue
        diag = " ".join(l.strip() for l in lines[1:])
        kind = None
        bad_value = None
        allowed = []

        m = re.search(
            r"Input should be\s+(.*?)\s+\[type=([a-z_]+),\s+input_value=(.*?),\s+input_type=",
            diag,
        )
        if m:
            allowed_str = m.group(1)
            kind = m.group(2)
            bad_value = m.group(3)
            allowed = re.findall(r"'([^']+)'", allowed_str)
        else:
            m2 = re.search(r"\[type=([a-z_]+),\s+input_value=(.*?),\s+input_type=", diag)
            if m2:
                kind = m2.group(1)
                bad_value = m2.group(2)

        out.append(
            {
                "path": path,
                "kind": kind or "unknown",
                "input_value": bad_value,
                "allowed": allowed,
            }
        )
    return out


def find_unknown_fields(extracted: dict, legal: dict[str, set[str]]) -> list[str]:
    """Top-level section field names that aren't in the schema."""
    unknowns: list[str] = []
    legal_sections = set(legal.keys())
    for sec_name, sec_val in extracted.items():
        if sec_name not in legal_sections:
            unknowns.append(sec_name)
            continue
        if isinstance(sec_val, dict):
            for k in sec_val.keys():
                if k not in legal[sec_name] and k != "span":
                    unknowns.append(f"{sec_name}.{k}")
        elif isinstance(sec_val, list):
            for item in sec_val:
                if isinstance(item, dict):
                    for k in item.keys():
                        if k not in legal[sec_name] and k != "span":
                            unknowns.append(f"{sec_name}[].{k}")
    return unknowns


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--combined",
        type=Path,
        default=ROOT / "data/llm_cache/sample/v1/sample_combined.json",
    )
    args = p.parse_args()

    with open(args.combined) as f:
        records = json.load(f)

    legal_by_section = collect_legal_keys_per_path()

    n_total = len(records)
    n_api_error = 0
    n_parse_error = 0
    n_empty = 0
    n_extracted = 0
    n_with_warnings = 0

    invalid_value_by_field: dict[str, Counter[str]] = defaultdict(Counter)
    shape_errors: Counter[str] = Counter()  # path -> count for model_type errors
    unknown_field_counter: Counter[str] = Counter()
    example_by_failure: dict[str, list[tuple[int, str]]] = defaultdict(list)

    for i, rec in enumerate(records):
        ext = rec.get("extracted") or {}
        if isinstance(ext, dict) and "_error" in ext:
            n_api_error += 1
            continue
        if isinstance(ext, dict) and "_parse_error" in ext:
            n_parse_error += 1
            continue
        if not ext:
            n_empty += 1
            continue
        n_extracted += 1

        unknowns = find_unknown_fields(ext, legal_by_section)
        for u in unknowns:
            unknown_field_counter[u] += 1
            if len(example_by_failure[f"UNKNOWN_FIELD::{u}"]) < 3:
                example_by_failure[f"UNKNOWN_FIELD::{u}"].append((i, json.dumps(ext)[:200]))

        warnings = rec.get("validation_warnings", [])
        if warnings:
            n_with_warnings += 1
        for w in warnings:
            for err in parse_pydantic_warning(w):
                path = err["path"]
                kind = err["kind"]
                if kind == "enum":
                    key = f"{path}"
                    val = err["input_value"]
                    invalid_value_by_field[key][val] += 1
                    if len(example_by_failure[f"ENUM::{key}::{val}"]) < 3:
                        example_by_failure[f"ENUM::{key}::{val}"].append(
                            (i, json.dumps(ext)[:200])
                        )
                elif kind == "model_type":
                    shape_errors[path] += 1
                    if len(example_by_failure[f"SHAPE::{path}"]) < 3:
                        example_by_failure[f"SHAPE::{path}"].append(
                            (i, json.dumps(ext)[:200])
                        )
                else:
                    shape_errors[f"{path} (kind={kind})"] += 1

    print("=" * 78)
    print("SAMPLE RUN — FAILURE CATALOG")
    print("=" * 78)
    print(f"  total records:                {n_total}")
    print(f"  API errors (503 etc):         {n_api_error}")
    print(f"  JSON parse errors:            {n_parse_error}")
    print(f"  empty {{}} (correct on empty review): {n_empty}")
    print(f"  with extraction:              {n_extracted}")
    print(f"  with validation warnings:     {n_with_warnings}  "
          f"({100*n_with_warnings/max(n_extracted,1):.1f}% of extractions)")
    print()

    print("-" * 78)
    print("ENUM VIOLATIONS (model emitted value not in schema)")
    print("-" * 78)
    if not invalid_value_by_field:
        print("  (none)")
    for field, vals in sorted(invalid_value_by_field.items(), key=lambda kv: -sum(kv[1].values())):
        total = sum(vals.values())
        print(f"\n  {field}   ({total} occurrences)")
        for v, c in vals.most_common(8):
            print(f"      {c:>3d}x  -> {v}")
    print()

    print("-" * 78)
    print("SHAPE ERRORS (object vs list, missing required field, etc.)")
    print("-" * 78)
    if not shape_errors:
        print("  (none)")
    for path, c in shape_errors.most_common():
        print(f"  {c:>3d}x  {path}")
    print()

    print("-" * 78)
    print("UNKNOWN / INVENTED FIELD NAMES (not in schema)")
    print("-" * 78)
    if not unknown_field_counter:
        print("  (none)")
    for path, c in unknown_field_counter.most_common(40):
        print(f"  {c:>3d}x  {path}")
    print()

    print("-" * 78)
    print("EXAMPLES (first 1-2 per failure mode)")
    print("-" * 78)
    keys_in_order = (
        list(unknown_field_counter.keys())[:8]
        + sorted(invalid_value_by_field.keys(), key=lambda k: -sum(invalid_value_by_field[k].values()))[:8]
        + list(shape_errors.keys())[:5]
    )
    for k in keys_in_order:
        prefix_keys = [k2 for k2 in example_by_failure if k in k2]
        for pk in prefix_keys[:1]:
            for idx, snippet in example_by_failure[pk][:1]:
                print(f"\n  [{pk}]")
                print(f"    record idx {idx}, restaurant={records[idx]['restaurant']}")
                review = (records[idx]['description'] or '').replace('\n', ' ')[:200]
                print(f"    review: {review}")
                print(f"    extracted snippet: {snippet}")
    print()
    print("=" * 78)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
