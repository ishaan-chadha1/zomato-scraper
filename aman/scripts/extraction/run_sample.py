#!/usr/bin/env python3
"""Run the extraction prompt on a stratified sample of reviews via Gemini sync API.

Picks ~35 reviews stratified by rating + length + restaurant, calls Gemini
2.5 Flash Lite synchronously with structured output, writes each raw
response to `data/llm_cache/sample/`, and prints a summary table.

Usage:
    python3 scripts/extraction/run_sample.py
    python3 scripts/extraction/run_sample.py --n 50
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google import genai
from google.genai import types

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from extraction.effort_signal import classify_review_effort
from extraction.normalize import normalize_extraction
from extraction.prompt import SYSTEM_INSTRUCTION, build_user_prompt
from extraction.schema import ReviewExtraction
from extraction.tiers import SCHEMA_VERSION
from extraction.triggers import cross_dish_consistency_trigger, scan_triggers


def select_samples(df: pd.DataFrame, n: int, seed: int = 42) -> pd.DataFrame:
    """Stratified sample across rating, length, and source.

    Splits the budget roughly as:
      - 50% rated, with description >= 30 chars (across all rating buckets)
      - 15% very long (95th pct+)
      - 15% short-but-text (15-50 chars)
      - 10% empty / near-empty
      - 10% rescrape-source extras
    """

    df = df.copy()
    df["desc_len"] = df["Description"].fillna("").str.len()

    rated_budget = max(int(n * 0.50), 9)
    long_budget = max(int(n * 0.15), 3)
    short_budget = max(int(n * 0.15), 3)
    empty_budget = max(int(n * 0.10), 2)
    rescrape_budget = max(int(n * 0.10), 2)

    buckets: list[pd.DataFrame] = []

    rated_per = max(1, rated_budget // 9)  # 5 integer + 4 half-star buckets
    rating_values = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
    for rating in rating_values:
        sub = df[(df["Rating"] == rating) & (df["desc_len"] >= 30)]
        if not sub.empty:
            take = min(rated_per, len(sub))
            buckets.append(sub.sample(n=take, random_state=seed))

    long_threshold = df["desc_len"].quantile(0.95)
    long_sub = df[df["desc_len"] >= long_threshold]
    if not long_sub.empty:
        buckets.append(long_sub.sample(n=min(long_budget, len(long_sub)), random_state=seed))

    short_sub = df[(df["desc_len"] >= 15) & (df["desc_len"] < 50)]
    if not short_sub.empty:
        buckets.append(short_sub.sample(n=min(short_budget, len(short_sub)), random_state=seed))

    empty_sub = df[df["desc_len"] < 15]
    if not empty_sub.empty:
        buckets.append(empty_sub.sample(n=min(empty_budget, len(empty_sub)), random_state=seed))

    rescrape_sub = df[df["source"] == "rescrape"]
    if not rescrape_sub.empty:
        buckets.append(
            rescrape_sub.sample(n=min(rescrape_budget, len(rescrape_sub)), random_state=seed)
        )

    sample = pd.concat(buckets).drop_duplicates(subset="Review URL")
    if len(sample) > n:
        sample = sample.sample(n=n, random_state=seed)
    return sample.reset_index(drop=True)


def extract_one(
    client: genai.Client,
    *,
    review_text: str,
    rating: float | None,
    restaurant: str,
    t2_triggers: list[str],
    review_effort_signal: str,
    max_retries: int = 3,
) -> tuple[dict, int, int, list[str]]:
    """Single sync extraction call with retry on 503 / transient errors.

    Returns (parsed_dict, input_tokens, output_tokens, validation_warnings).
    """

    user_msg = build_user_prompt(
        review_text,
        rating,
        restaurant,
        t2_triggers=t2_triggers,
        review_effort_signal=review_effort_signal,
    )

    last_err: Exception | None = None
    response = None
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash-lite",
                contents=user_msg,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_INSTRUCTION,
                    response_mime_type="application/json",
                    temperature=0.0,
                ),
            )
            last_err = None
            break
        except Exception as e:  # noqa: BLE001 — genai raises ClientError, ServerError, etc
            last_err = e
            msg = str(e)
            transient = ("503" in msg) or ("UNAVAILABLE" in msg) or ("429" in msg)
            if not transient or attempt == max_retries - 1:
                break
            sleep_s = 2 ** attempt  # 1, 2, 4
            time.sleep(sleep_s)

    if response is None:
        raise last_err if last_err else RuntimeError("unknown error")

    parsed: dict = {}
    if response.text:
        try:
            parsed = json.loads(response.text)
        except json.JSONDecodeError:
            parsed = {"_parse_error": response.text[:1000]}

    warnings: list[str] = []
    if parsed and "_parse_error" not in parsed:
        parsed = normalize_extraction(parsed, t2_triggers)
        # Merge Python pre-processing (authoritative for effort + triggers)
        parsed.setdefault("schema_version", SCHEMA_VERSION)
        parsed["t2_triggers_fired"] = t2_triggers
        rs = parsed.setdefault("reviewer_signal", {})
        if isinstance(rs, dict):
            rs["review_effort_signal"] = review_effort_signal
        try:
            ReviewExtraction.model_validate(parsed)
        except Exception as e:
            warnings.append(f"pydantic_validation: {str(e)[:400]}")

    usage = response.usage_metadata
    in_tok = getattr(usage, "prompt_token_count", 0) or 0
    out_tok = getattr(usage, "candidates_token_count", 0) or 0
    return parsed, in_tok, out_tok, warnings


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--n", type=int, default=35, help="Sample size (default 35)")
    p.add_argument(
        "--reviews",
        type=Path,
        default=ROOT / "data/reviews.parquet",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=ROOT / "data/llm_cache/sample",
    )
    p.add_argument(
        "--label",
        type=str,
        default=None,
        help="Subdirectory under --out-dir (e.g. 'v1', 'v2'). Default: no subdir.",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Sampling seed; keep stable across prompt versions for clean A/B.",
    )
    args = p.parse_args()

    if args.label:
        args.out_dir = args.out_dir / args.label

    load_dotenv(ROOT / ".env")
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("ERROR: GEMINI_API_KEY not found in environment (.env)", file=sys.stderr)
        return 1

    args.out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading reviews from {args.reviews} ...")
    df = pd.read_parquet(args.reviews)
    print(f"  loaded {len(df):,} reviews")

    print(f"\nSelecting stratified sample of ~{args.n} reviews (seed={args.seed}) ...")
    sample = select_samples(df, args.n, seed=args.seed)
    print(f"  selected {len(sample)} reviews\n")

    client = genai.Client(api_key=api_key)

    total_in = 0
    total_out = 0
    results: list[dict] = []
    started = time.time()

    for i, row in sample.iterrows():
        desc = row["Description"] if isinstance(row["Description"], str) else ""
        rating = row["Rating"] if pd.notna(row["Rating"]) else None
        restaurant = row["restaurant"]
        review_url = row["Review URL"]

        print(
            f"[{i+1:>2}/{len(sample)}] "
            f"r={rating} len={len(desc):>4}  {restaurant[:55]:<55}",
            flush=True,
        )

        effort = classify_review_effort(desc)
        extra_t2: list[str] = []
        if cross_dish_consistency_trigger(desc):
            extra_t2.append("cross_dish_consistency_signal")
        t2 = scan_triggers(desc, extra_fields=extra_t2)

        try:
            extracted, in_tok, out_tok, warnings = extract_one(
                client,
                review_text=desc,
                rating=float(rating) if rating is not None else None,
                restaurant=restaurant,
                t2_triggers=t2,
                review_effort_signal=effort,
            )
        except Exception as e:
            print(f"      !! error: {e}")
            extracted = {"_error": str(e)}
            in_tok = out_tok = 0
            warnings = []

        total_in += in_tok
        total_out += out_tok

        if warnings:
            for w in warnings:
                print(f"      ~ validation: {w[:160]}")

        record = {
            "review_url": review_url,
            "restaurant": restaurant,
            "rating": float(rating) if rating is not None else None,
            "source": row["source"],
            "description_len": len(desc),
            "description": desc,
            "review_effort_signal": effort,
            "t2_triggers_fired": t2,
            "input_tokens": in_tok,
            "output_tokens": out_tok,
            "validation_warnings": warnings,
            "extracted": extracted,
        }
        results.append(record)

        # Write individual file for inspection
        safe_url = review_url.replace("/", "_").replace(":", "_")[-60:]
        with open(args.out_dir / f"{i:03d}__{safe_url}.json", "w") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)

    elapsed = time.time() - started

    # Combined output
    with open(args.out_dir / "sample_combined.json", "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # Cost estimate
    cost_input = total_in / 1_000_000 * 0.10
    cost_output = total_out / 1_000_000 * 0.40
    total_cost = cost_input + cost_output

    print()
    print("=" * 70)
    print(f"Done. {len(results)} reviews extracted in {elapsed:.1f}s")
    print(f"Total input tokens:   {total_in:>10,}  (${cost_input:.4f})")
    print(f"Total output tokens:  {total_out:>10,}  (${cost_output:.4f})")
    print(f"Total cost (sync):    ${total_cost:.4f}")
    print(f"Per review average:   ${total_cost/max(len(results),1):.5f}")
    print()
    print(f"Outputs written to: {args.out_dir}")
    print(f"Combined JSON:      {args.out_dir / 'sample_combined.json'}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
