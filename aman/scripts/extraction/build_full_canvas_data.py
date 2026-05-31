#!/usr/bin/env python3
"""Generate compact per-review records for the all-reviews canvas.

Strips null fields and `description == None` placeholders so the
embedded JSON is as small as possible while still showing the user
exactly what each review produced.
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def strip_nulls(obj):
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


def main():
    src = ROOT / "data/llm_cache/sample/v2/sample_combined.json"
    with open(src) as f:
        records = json.load(f)

    compact = []
    for i, r in enumerate(records):
        ext = r.get("extracted") or {}
        if isinstance(ext, dict) and ("_error" in ext or "_parse_error" in ext):
            ext = {"_error": ext.get("_error") or ext.get("_parse_error")}
        else:
            ext = strip_nulls(ext)
        compact.append(
            {
                "idx": i,
                "restaurant": r["restaurant"],
                "rating": r.get("rating"),
                "source": r.get("source"),
                "desc_len": r.get("description_len", 0),
                "desc": r.get("description") or "",
                "ext": ext,
                "warns": r.get("validation_warnings", []),
                "in_tok": r.get("input_tokens", 0),
                "out_tok": r.get("output_tokens", 0),
            }
        )

    out_path = ROOT / "data/llm_cache/sample/canvas_full_data.json"
    with open(out_path, "w") as f:
        json.dump(compact, f, ensure_ascii=False, separators=(",", ":"))
    size_kb = out_path.stat().st_size / 1024
    print(f"Wrote {out_path} ({size_kb:.1f} KB, {len(compact)} records)")


if __name__ == "__main__":
    main()
