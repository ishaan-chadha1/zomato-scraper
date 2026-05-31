"""Combine cleaned Myeloma Beacon parquet files into a single denormalised file.

Reads each forum's ``posts_clean.parquet`` + ``topics_clean.parquet``, stacks
both forums together, then left-joins each post with its topic metadata so
that every row is one post + the topic it belongs to + a ``forum_slug`` tag.

Output (default):
    data/MyelomaBeacon/posts_with_topics_all.parquet

Usage::

    python3 scripts/combine_myeloma_beacon.py
    python3 scripts/combine_myeloma_beacon.py --root data/MyelomaBeacon
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd

DEFAULT_FORUMS = ("multiple-myeloma", "treatments-side-effects")
DEFAULT_ROOT = Path("data") / "MyelomaBeacon"
DEFAULT_OUT_NAME = "posts_with_topics_all.parquet"

POST_COLS_ORDER = [
    "topic_id",
    "post_id",
    "post_offset",
    "page_index",
    "position_in_topic",
    "is_first_post",
    "post_title",
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

TOPIC_COLS_ORDER = [
    "topic_title",
    "topic_slug",
    "topic_url",
    "topic_starter_author",
    "topic_starter_profile_url",
    "topic_start_date",
    "topic_start_date_iso",
    "topic_start_year",
    "topic_replies",
    "topic_expected_post_count",
    "topic_actual_post_count",
    "topic_count_delta",
    "topic_views",
    "topic_last_post_author",
    "topic_last_post_profile_url",
    "topic_last_post_date",
    "topic_last_post_date_iso",
    "topic_last_post_link",
    "topic_has_attachment",
]

FINAL_COL_ORDER = ["forum_slug", *POST_COLS_ORDER, *TOPIC_COLS_ORDER]


def load_forum(root: Path, slug: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    p_path = root / slug / "posts_clean.parquet"
    t_path = root / slug / "topics_clean.parquet"
    if not p_path.exists():
        raise FileNotFoundError(f"missing posts parquet: {p_path}")
    if not t_path.exists():
        raise FileNotFoundError(f"missing topics parquet: {t_path}")
    posts = pd.read_parquet(p_path)
    topics = pd.read_parquet(t_path)
    posts.insert(0, "forum_slug", slug)
    topics.insert(0, "forum_slug", slug)
    return posts, topics


def combine(root: Path, forums: list[str], out_path: Path) -> dict[str, object]:
    started = time.time()
    posts_frames: list[pd.DataFrame] = []
    topics_frames: list[pd.DataFrame] = []
    per_forum_post_counts: dict[str, int] = {}
    per_forum_topic_counts: dict[str, int] = {}

    for slug in forums:
        posts, topics = load_forum(root, slug)
        per_forum_post_counts[slug] = len(posts)
        per_forum_topic_counts[slug] = len(topics)
        posts_frames.append(posts)
        topics_frames.append(topics)

    posts = pd.concat(posts_frames, ignore_index=True).rename(
        columns={"title": "post_title"}
    )
    topics = pd.concat(topics_frames, ignore_index=True)
    # Prefix every non-key topic column with "topic_" to dodge collisions.
    # ``title`` becomes ``topic_title`` via this same pass (no double prefix).
    topics = topics.rename(
        columns={
            c: f"topic_{c}"
            for c in topics.columns
            if c not in {"forum_slug", "topic_id"}
        }
    )

    merged = posts.merge(
        topics,
        on=["forum_slug", "topic_id"],
        how="left",
        validate="m:1",
        indicator=True,
    )
    unmatched = int((merged["_merge"] != "both").sum())
    merged = merged.drop(columns=["_merge"])

    # Strict column ordering; will raise KeyError if anything unexpected exists.
    missing = [c for c in FINAL_COL_ORDER if c not in merged.columns]
    extra = [c for c in merged.columns if c not in FINAL_COL_ORDER]
    if missing:
        raise RuntimeError(f"missing expected columns after join: {missing}")
    if extra:
        raise RuntimeError(f"unexpected extra columns after join: {extra}")
    merged = merged[FINAL_COL_ORDER]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(out_path, compression="snappy", index=False)
    size_bytes = out_path.stat().st_size

    return {
        "rows": len(merged),
        "cols": len(merged.columns),
        "unmatched_posts": unmatched,
        "per_forum_posts": per_forum_post_counts,
        "per_forum_topics": per_forum_topic_counts,
        "out_path": out_path,
        "size_bytes": size_bytes,
        "elapsed_s": time.time() - started,
    }


def fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} TB"


def print_summary(stats: dict[str, object]) -> None:
    bar = "=" * 70
    sub = "-" * 70
    print()
    print(bar)
    print("  Myeloma Beacon — combined posts + topics parquet")
    print(bar)
    print(f"  rows                {stats['rows']:>10,}")
    print(f"  cols                {stats['cols']:>10}")
    print(f"  unmatched posts     {stats['unmatched_posts']:>10}  (expect 0)")
    print(sub)
    print("  per-forum post counts:")
    for slug, n in stats["per_forum_posts"].items():
        print(f"    {slug:<32} {n:>8,} posts")
    print("  per-forum topic counts:")
    for slug, n in stats["per_forum_topics"].items():
        print(f"    {slug:<32} {n:>8,} topics")
    print(sub)
    print(f"  output     {stats['out_path']}")
    print(f"  size       {fmt_bytes(stats['size_bytes'])}")
    print(f"  elapsed    {stats['elapsed_s']:.2f}s")
    print(bar)
    print()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("Usage::")[0].strip())
    p.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help=f"Root directory holding per-forum sub-dirs (default: {DEFAULT_ROOT})",
    )
    p.add_argument(
        "--forums",
        nargs="+",
        default=list(DEFAULT_FORUMS),
        help=f"Forum slugs to combine (default: {' '.join(DEFAULT_FORUMS)})",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help=(
            f"Output parquet path "
            f"(default: <root>/{DEFAULT_OUT_NAME})"
        ),
    )
    return p


def main() -> int:
    args = build_parser().parse_args()
    out_path = args.out if args.out is not None else args.root / DEFAULT_OUT_NAME
    stats = combine(args.root, list(args.forums), out_path)
    print_summary(stats)
    if stats["unmatched_posts"] != 0:
        print(f"  WARN: {stats['unmatched_posts']} posts had no matching topic row")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
