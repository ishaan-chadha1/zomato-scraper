#!/usr/bin/env python3
"""Schema normalization for the Myeloma Beacon scrape.

Reads ``posts.csv`` + ``topics.csv`` and writes analysis-ready datasets:

* ``posts_clean.csv``    — deduplicated + enriched (one row per unique post).
* ``posts_clean.parquet``— same rows, faster + smaller for analysis.
* ``topics_clean.csv``   — enriched with ISO dates + actual post counts.
* ``topics_clean.parquet``— same.

Raw inputs are never modified.

Usage::

    python3 scripts/clean_myeloma_beacon.py
    python3 scripts/clean_myeloma_beacon.py --data-dir data/MyelomaBeacon/multiple-myeloma
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd


PHPBB_DATE_FMT = "%a %b %d, %Y %I:%M %p"
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
AGE_RE = re.compile(r"\b(\d{1,3})\b")


def parse_phpbb_date(s: Any) -> pd.Timestamp | pd.NaT:
    """Parse strings like ``Thu Apr 19, 2012 2:19 pm`` to a Timestamp."""
    if not isinstance(s, str) or not s.strip():
        return pd.NaT
    try:
        return pd.to_datetime(s.strip(), format=PHPBB_DATE_FMT, errors="raise")
    except (ValueError, TypeError):
        return pd.NaT


def extract_year(s: str | None) -> int | None:
    if not isinstance(s, str):
        return None
    m = YEAR_RE.search(s)
    if not m:
        return None
    y = int(m.group(1))
    return y if 1950 <= y <= 2100 else None


def extract_age(s: str | None) -> int | None:
    if not isinstance(s, str):
        return None
    m = AGE_RE.search(s.strip())
    if not m:
        return None
    n = int(m.group(1))
    return n if 0 < n <= 110 else None


def parse_author_meta(blob: Any) -> dict[str, Any]:
    out = {
        "author_name_meta": None,
        "author_who": None,
        "author_dx_when_raw": None,
        "author_dx_year": None,
        "author_age_at_dx_raw": None,
        "author_age_at_dx": None,
    }
    if not isinstance(blob, str) or not blob.strip():
        return out
    try:
        d = json.loads(blob)
    except json.JSONDecodeError:
        return out
    if not isinstance(d, dict):
        return out

    out["author_name_meta"] = d.get("Name") or None
    out["author_who"] = d.get("Who do you know with myeloma?") or None

    dx_when = d.get("When were you/they diagnosed?") or None
    out["author_dx_when_raw"] = dx_when
    out["author_dx_year"] = extract_year(dx_when)

    age = d.get("Age at diagnosis") or None
    out["author_age_at_dx_raw"] = age
    out["author_age_at_dx"] = extract_age(age)
    return out


def word_count(s: Any) -> int:
    if not isinstance(s, str) or not s:
        return 0
    return len(s.split())


def clean_posts(
    posts_path: Path, topics_path: Path, *, progress: bool = False
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Load + clean posts.csv. Returns (cleaned_df, stats)."""
    if progress:
        print(f"  reading {posts_path} ...", flush=True)
    posts = pd.read_csv(posts_path, low_memory=False)
    raw_rows = len(posts)

    # 1. Dedupe by (topic_id, post_id) keeping first occurrence.
    #    This drops the ~6 site-duplicated rows where phpBB re-served the
    #    last 2 posts of page 1 on page 2.
    posts = posts.sort_values(
        ["topic_id", "post_offset", "position_in_topic"], kind="stable"
    ).drop_duplicates(subset=["topic_id", "post_id"], keep="first")
    after_dedupe = len(posts)
    if progress:
        print(
            f"  deduped: {raw_rows} -> {after_dedupe} (dropped {raw_rows - after_dedupe})",
            flush=True,
        )

    # 2. Date parsing.
    if progress:
        print("  parsing posted_at ...", flush=True)
    posts["posted_at_iso"] = posts["posted_at"].map(parse_phpbb_date)
    posts["posted_date"] = posts["posted_at_iso"].dt.date.astype("string")
    posts["posted_year"] = posts["posted_at_iso"].dt.year.astype("Int64")
    posts["posted_month"] = posts["posted_at_iso"].dt.to_period("M").astype("string")

    # 3. Author meta flattening.
    if progress:
        print("  flattening author_meta_json ...", flush=True)
    meta_records = posts["author_meta_json"].map(parse_author_meta).tolist()
    meta_df = pd.DataFrame(meta_records, index=posts.index)
    posts = pd.concat([posts, meta_df], axis=1)

    # 4. Body size.
    posts["body_word_count"] = posts["body_text"].fillna("").map(word_count)
    posts["body_char_count"] = posts["body_text"].fillna("").map(len)

    # 5. First-post flag.
    posts["is_first_post"] = (posts["position_in_topic"].astype("Int64") == 0)

    # 6. Tidy column order.
    column_order = [
        "topic_id",
        "post_id",
        "post_offset",
        "page_index",
        "position_in_topic",
        "is_first_post",
        "title",
        "author",
        "author_profile_url",
        "author_name_meta",
        "author_who",
        "author_dx_when_raw",
        "author_dx_year",
        "author_age_at_dx_raw",
        "author_age_at_dx",
        "posted_at",
        "posted_at_iso",
        "posted_date",
        "posted_year",
        "posted_month",
        "body_text",
        "body_word_count",
        "body_char_count",
        "body_html",
        "attachment_urls_json",
        "page_url",
        "author_meta_json",
    ]
    column_order = [c for c in column_order if c in posts.columns]
    extras = [c for c in posts.columns if c not in column_order]
    posts = posts[column_order + extras].reset_index(drop=True)

    stats = {
        "raw_rows": raw_rows,
        "deduped_rows": after_dedupe,
        "dropped_duplicates": raw_rows - after_dedupe,
        "posted_at_parsed": int(posts["posted_at_iso"].notna().sum()),
        "dx_year_extracted": int(posts["author_dx_year"].notna().sum()),
        "age_at_dx_extracted": int(posts["author_age_at_dx"].notna().sum()),
    }
    return posts, stats


def clean_topics(
    topics_path: Path, cleaned_posts: pd.DataFrame, *, progress: bool = False
) -> tuple[pd.DataFrame, dict[str, int]]:
    if progress:
        print(f"  reading {topics_path} ...", flush=True)
    topics = pd.read_csv(topics_path)
    raw_rows = len(topics)

    # Parse dates.
    topics["start_date_iso"] = topics["start_date"].map(parse_phpbb_date)
    topics["last_post_date_iso"] = topics["last_post_date"].map(parse_phpbb_date)
    topics["start_year"] = topics["start_date_iso"].dt.year.astype("Int64")

    # Actual scraped post count per topic (post-dedupe).
    actual_counts = (
        cleaned_posts.groupby("topic_id").size().to_dict()
    )
    topics["actual_post_count"] = (
        topics["topic_id"].map(actual_counts).fillna(0).astype("Int64")
    )

    # Was the listing's reply count stale relative to what we actually scraped?
    topics["expected_post_count"] = (
        topics["replies"].fillna(0).astype("Int64") + 1
    )
    topics["count_delta"] = (
        topics["actual_post_count"] - topics["expected_post_count"]
    )

    # Tidy column order.
    column_order = [
        "topic_id",
        "slug",
        "url",
        "title",
        "starter_author",
        "starter_profile_url",
        "start_date",
        "start_date_iso",
        "start_year",
        "replies",
        "expected_post_count",
        "actual_post_count",
        "count_delta",
        "views",
        "last_post_author",
        "last_post_profile_url",
        "last_post_date",
        "last_post_date_iso",
        "last_post_link",
        "has_attachment",
    ]
    column_order = [c for c in column_order if c in topics.columns]
    extras = [c for c in topics.columns if c not in column_order]
    topics = topics[column_order + extras].reset_index(drop=True)

    stats = {
        "topics_rows": raw_rows,
        "start_date_parsed": int(topics["start_date_iso"].notna().sum()),
        "last_post_date_parsed": int(topics["last_post_date_iso"].notna().sum()),
        "topics_with_stale_replies": int((topics["count_delta"] < 0).sum()),
        "topics_with_extra_posts": int((topics["count_delta"] > 0).sum()),
    }
    return topics, stats


def write_outputs(
    data_dir: Path,
    posts: pd.DataFrame,
    topics: pd.DataFrame,
    *,
    progress: bool = False,
) -> dict[str, str]:
    posts_csv = data_dir / "posts_clean.csv"
    posts_parquet = data_dir / "posts_clean.parquet"
    topics_csv = data_dir / "topics_clean.csv"
    topics_parquet = data_dir / "topics_clean.parquet"

    if progress:
        print(f"  writing {posts_csv} ...", flush=True)
    posts.to_csv(posts_csv, index=False)

    if progress:
        print(f"  writing {posts_parquet} ...", flush=True)
    posts.to_parquet(posts_parquet, index=False, compression="zstd")

    if progress:
        print(f"  writing {topics_csv} ...", flush=True)
    topics.to_csv(topics_csv, index=False)

    if progress:
        print(f"  writing {topics_parquet} ...", flush=True)
    topics.to_parquet(topics_parquet, index=False, compression="zstd")

    return {
        "posts_csv": str(posts_csv),
        "posts_parquet": str(posts_parquet),
        "topics_csv": str(topics_csv),
        "topics_parquet": str(topics_parquet),
    }


def print_summary(
    posts: pd.DataFrame,
    topics: pd.DataFrame,
    post_stats: dict[str, int],
    topic_stats: dict[str, int],
    paths: dict[str, str],
    elapsed: float,
) -> None:
    print(f"\n{'=' * 70}")
    print("  Myeloma Beacon — cleaned datasets")
    print(f"{'=' * 70}")
    print(f"  posts.csv:           {post_stats['raw_rows']:>7} raw rows")
    print(f"  posts (deduped):     {post_stats['deduped_rows']:>7} unique posts "
          f"(dropped {post_stats['dropped_duplicates']} site-duplicated)")
    print(f"  posted_at parsed:    {post_stats['posted_at_parsed']:>7} "
          f"({100 * post_stats['posted_at_parsed'] / max(post_stats['deduped_rows'], 1):.1f}%)")
    print(f"  dx_year extracted:   {post_stats['dx_year_extracted']:>7} "
          f"({100 * post_stats['dx_year_extracted'] / max(post_stats['deduped_rows'], 1):.1f}%)")
    print(f"  age_at_dx extracted: {post_stats['age_at_dx_extracted']:>7} "
          f"({100 * post_stats['age_at_dx_extracted'] / max(post_stats['deduped_rows'], 1):.1f}%)")
    print(f"{'-' * 70}")
    print(f"  topics.csv:          {topic_stats['topics_rows']:>7} rows")
    print(f"  start dates parsed:  {topic_stats['start_date_parsed']:>7}")
    print(f"  last dates parsed:   {topic_stats['last_post_date_parsed']:>7}")
    print(f"  stale reply counts:  {topic_stats['topics_with_stale_replies']:>7} "
          f"topic(s) where listing > actual")
    print(f"  extra posts found:   {topic_stats['topics_with_extra_posts']:>7} "
          f"topic(s) where actual > listing")
    print(f"{'-' * 70}")
    for k, v in paths.items():
        size_mb = Path(v).stat().st_size / 1024 / 1024
        print(f"  {k:<18}  {v}   ({size_mb:.2f} MB)")
    print(f"{'-' * 70}")
    print(f"  elapsed: {elapsed:.1f}s")
    print(f"{'=' * 70}\n")


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("Usage::")[0].strip())
    p.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data") / "MyelomaBeacon" / "multiple-myeloma",
        help="Scrape output directory (default: data/MyelomaBeacon/multiple-myeloma)",
    )
    p.add_argument(
        "--quiet", action="store_true", help="Suppress per-step progress"
    )
    return p


def main() -> int:
    args = build_argparser().parse_args()
    progress = not args.quiet

    posts_path = args.data_dir / "posts.csv"
    topics_path = args.data_dir / "topics.csv"
    if not posts_path.exists() or not topics_path.exists():
        print(f"Missing inputs in {args.data_dir}", file=sys.stderr)
        return 2

    started = time.time()

    if progress:
        print("[clean] posts ...")
    posts_clean, post_stats = clean_posts(posts_path, topics_path, progress=progress)

    if progress:
        print("[clean] topics ...")
    topics_clean, topic_stats = clean_topics(topics_path, posts_clean, progress=progress)

    if progress:
        print("[clean] writing outputs ...")
    paths = write_outputs(args.data_dir, posts_clean, topics_clean, progress=progress)

    print_summary(posts_clean, topics_clean, post_stats, topic_stats, paths, time.time() - started)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
