#!/usr/bin/env python3
"""Compute v1 vs v2 stats and pick showcase reviews for the canvas.

Writes a self-contained JSON to /tmp/canvas_data.json that the canvas
can embed inline.
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from extraction.schema import ReviewExtraction  # noqa: E402


def legal_sections() -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for sname, fi in ReviewExtraction.model_fields.items():
        ann = fi.annotation
        args = getattr(ann, "__args__", ())
        for a in args:
            if hasattr(a, "model_fields"):
                out[sname] = set(a.model_fields.keys())
                break
            inner_args = getattr(a, "__args__", ())
            for b in inner_args:
                if hasattr(b, "model_fields"):
                    out[sname] = set(b.model_fields.keys())
                    break
    return out


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


def load_run(path: Path):
    with open(path) as f:
        return json.load(f)


def parse_warning(w: str):
    """Yield {section, kind} entries pulled from a pydantic warning blob."""
    blocks = re.split(r"\n(?=[a-zA-Z_][\w\.]*\n)", w)
    out = []
    for b in blocks:
        lines = b.splitlines()
        if not lines:
            continue
        path = lines[0].strip()
        if not path or path.startswith("pydantic_validation"):
            continue
        diag = " ".join(l.strip() for l in lines[1:])
        m = re.search(r"\[type=([a-z_]+),", diag)
        kind = m.group(1) if m else "unknown"
        section = path.split(".")[0].split("[")[0]
        out.append({"path": path, "section": section, "kind": kind})
    return out


def find_unknown(extracted, legal):
    """Return list of invented field paths."""
    invented = []
    for sec, sec_val in extracted.items():
        if sec not in legal:
            invented.append(sec)
            continue
        items = []
        if isinstance(sec_val, dict):
            items = [sec_val]
        elif isinstance(sec_val, list):
            items = [it for it in sec_val if isinstance(it, dict)]
        for item in items:
            for k in item:
                if k not in legal[sec] and k != "span":
                    invented.append(f"{sec}.{k}")
    return invented


def summarize_run(records, legal):
    n = len(records)
    n_api_err = sum(1 for r in records if isinstance(r.get("extracted"), dict) and "_error" in r["extracted"])
    n_parse_err = sum(1 for r in records if isinstance(r.get("extracted"), dict) and "_parse_error" in r["extracted"])
    n_empty = sum(1 for r in records if r.get("extracted") == {})
    n_with_extraction = sum(
        1 for r in records
        if r.get("extracted")
        and "_error" not in r["extracted"]
        and "_parse_error" not in r["extracted"]
    )
    n_with_warn = sum(1 for r in records if r.get("validation_warnings"))

    total_in = sum(r.get("input_tokens", 0) for r in records)
    total_out = sum(r.get("output_tokens", 0) for r in records)
    cost = total_in / 1_000_000 * 0.10 + total_out / 1_000_000 * 0.40

    # Failure-mode breakdown
    mode_counts: Counter[str] = Counter()
    section_warns: Counter[str] = Counter()
    invented_field_total = 0
    for r in records:
        ext = r.get("extracted") or {}
        if isinstance(ext, dict) and ("_error" in ext or "_parse_error" in ext):
            continue
        invented = find_unknown(ext, legal)
        invented_field_total += len(invented)
        if invented:
            mode_counts["invented_field"] += len(invented)
        for w in r.get("validation_warnings", []):
            for err in parse_warning(w):
                section_warns[err["section"]] += 1
                if err["kind"] == "enum":
                    mode_counts["enum_violation"] += 1
                elif err["kind"] == "model_type":
                    mode_counts["shape_error"] += 1
                elif err["kind"] == "missing":
                    mode_counts["missing_required"] += 1
                else:
                    mode_counts["other_validation"] += 1

    # Section coverage
    cov: Counter[str] = Counter()
    for r in records:
        ext = r.get("extracted") or {}
        if not ext or "_error" in ext or "_parse_error" in ext:
            continue
        for sec in ext:
            if sec in legal:
                cov[sec] += 1

    return {
        "total": n,
        "api_errors": n_api_err,
        "parse_errors": n_parse_err,
        "empty_extractions": n_empty,
        "with_extraction": n_with_extraction,
        "with_warnings": n_with_warn,
        "warning_rate_pct": round(100 * n_with_warn / max(n_with_extraction, 1), 1),
        "input_tokens": total_in,
        "output_tokens": total_out,
        "total_cost_usd": round(cost, 4),
        "failure_modes": dict(mode_counts),
        "section_warns": dict(section_warns),
        "section_coverage": dict(cov),
    }


def pick_showcase(v2_records):
    """Pick 8 diverse showcase reviews — interesting for the user to see."""
    candidates = [
        r for r in v2_records
        if r.get("extracted")
        and "_error" not in (r["extracted"] or {})
        and "_parse_error" not in (r["extracted"] or {})
    ]

    chosen = []
    used_urls = set()

    def add_by(predicate, label):
        for r in candidates:
            if r["review_url"] in used_urls:
                continue
            if predicate(r):
                r2 = dict(r)
                r2["showcase_label"] = label
                chosen.append(r2)
                used_urls.add(r["review_url"])
                return True
        return False

    # 1. A scathing review with many immune flags
    add_by(
        lambda r: len(strip_nulls(r["extracted"].get("immune_flags", {}) or {})) >= 2
        and r["rating"] <= 2,
        "scathing review (multiple immune flags)",
    )

    # 2. A glowing long review
    add_by(
        lambda r: r["rating"] >= 4.5 and r["description_len"] >= 400,
        "glowing review (long, 4.5+)",
    )

    # 3. A review with rich dishes extraction
    add_by(
        lambda r: len(r["extracted"].get("dishes", []) or []) >= 3,
        "multi-dish extraction",
    )

    # 4. A medium-quality 3★ with mixed signals
    add_by(
        lambda r: r["rating"] == 3.0 and r["description_len"] >= 200,
        "3-star mixed signals",
    )

    # 5. A short review where model still extracted useful signal
    add_by(
        lambda r: r["description_len"] < 100
        and len(strip_nulls(r["extracted"])) >= 2,
        "short review, useful extraction",
    )

    # 6. Empty review (model correctly returns {})
    add_by(
        lambda r: r["description_len"] == 0 and r["extracted"] == {},
        "empty review (model correctly emits {})",
    )

    # 7. Bar / drinks review
    add_by(
        lambda r: r["extracted"].get("bar")
        or (r["extracted"].get("dietary") or {}).get("alcohol_served"),
        "bar/drinks extraction",
    )

    # 8. A 5-star with strong resonance signal
    add_by(
        lambda r: r["rating"] == 5.0
        and (r["extracted"].get("resonance") or {}).get("resonance_markers"),
        "5-star with resonance signal",
    )

    showcase = []
    for r in chosen:
        showcase.append(
            {
                "label": r["showcase_label"],
                "restaurant": r["restaurant"],
                "rating": r["rating"],
                "source": r["source"],
                "description_len": r["description_len"],
                "description": r["description"],
                "extracted": strip_nulls(r["extracted"]),
                "input_tokens": r["input_tokens"],
                "output_tokens": r["output_tokens"],
                "warnings": r.get("validation_warnings", []),
            }
        )
    return showcase


def main():
    legal = legal_sections()
    v1 = load_run(ROOT / "data/llm_cache/sample/v1/sample_combined.json")
    v2 = load_run(ROOT / "data/llm_cache/sample/v2/sample_combined.json")

    v1_summary = summarize_run(v1, legal)
    v2_summary = summarize_run(v2, legal)
    showcase = pick_showcase(v2)

    # Project full-corpus cost (using v2 per-review average + batch + caching estimate)
    n_reviews_total = 2_955_778
    v2_per_review = v2_summary["total_cost_usd"] / max(v2_summary["total"], 1)
    full_sync = round(v2_per_review * n_reviews_total, 0)
    full_batch = round(full_sync * 0.5, 0)
    full_batch_cached = round(full_sync * 0.25, 0)  # rough heuristic with context cache

    out = {
        "v1": v1_summary,
        "v2": v2_summary,
        "showcase": showcase,
        "n_reviews_total_corpus": n_reviews_total,
        "projection_sync_usd": full_sync,
        "projection_batch_usd": full_batch,
        "projection_batch_cached_usd": full_batch_cached,
    }

    out_path = ROOT / "data/llm_cache/sample/canvas_data.json"
    with open(out_path, "w") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"Wrote {out_path}")
    print(json.dumps({"v1": v1_summary, "v2": v2_summary,
                      "n_showcase": len(showcase),
                      "projection_sync_usd": full_sync,
                      "projection_batch_usd": full_batch,
                      "projection_batch_cached_usd": full_batch_cached}, indent=2))


if __name__ == "__main__":
    main()
