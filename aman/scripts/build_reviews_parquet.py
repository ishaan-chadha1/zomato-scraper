#!/usr/bin/env python3
"""Build a single canonical Parquet file from the raw review CSVs.

Reads every CSV in ``data/Reviews/`` and ``data/Reviews_rescrape/``,
adds a ``restaurant`` column (from the filename stem) and a ``source``
column (``"reviews"`` or ``"rescrape"``), deduplicates on ``Review URL``
with ``rescrape`` winning on conflict, and writes
``data/reviews.parquet``.

Source CSVs are never modified or deleted.

Usage:
    python3 scripts/build_reviews_parquet.py
    python3 scripts/build_reviews_parquet.py --output data/reviews.parquet
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd

EXPECTED_COLUMNS = ["Author", "Review URL", "Description", "Rating"]
SKIP_FILENAMES = {
    ".batch_failed.csv",
    "review_coverage_report.csv",
    "rescrape_summary.csv",
}
BATCH_SIZE = 500


def list_csvs(directory: Path) -> list[Path]:
    return sorted(
        p
        for p in directory.glob("*.csv")
        if p.is_file()
        and p.name not in SKIP_FILENAMES
        and not p.name.startswith(".")
    )


def read_csv_safe(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="utf-8", encoding_errors="replace")
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=EXPECTED_COLUMNS)


def load_directory(
    directory: Path,
    source: str,
    *,
    log_prefix: str,
) -> pd.DataFrame:
    paths = list_csvs(directory)
    if not paths:
        print(f"{log_prefix} no CSVs found in {directory}")
        return pd.DataFrame(columns=[*EXPECTED_COLUMNS, "restaurant", "source"])

    print(f"{log_prefix} {len(paths):,} CSVs found in {directory}")
    frames: list[pd.DataFrame] = []
    started = time.time()

    for batch_start in range(0, len(paths), BATCH_SIZE):
        batch = paths[batch_start : batch_start + BATCH_SIZE]
        batch_frames: list[pd.DataFrame] = []
        for path in batch:
            df = read_csv_safe(path)
            if df.empty and not df.columns.size:
                continue
            for col in EXPECTED_COLUMNS:
                if col not in df.columns:
                    df[col] = pd.NA
            df = df[EXPECTED_COLUMNS].copy()
            df["restaurant"] = path.stem
            df["source"] = source
            batch_frames.append(df)
        if batch_frames:
            frames.append(pd.concat(batch_frames, ignore_index=True))
        done = batch_start + len(batch)
        elapsed = time.time() - started
        rate = done / elapsed if elapsed > 0 else 0
        print(
            f"{log_prefix}   read {done:,}/{len(paths):,} "
            f"({rate:.0f} files/s, {elapsed:.0f}s elapsed)",
            flush=True,
        )

    if not frames:
        return pd.DataFrame(columns=[*EXPECTED_COLUMNS, "restaurant", "source"])

    df = pd.concat(frames, ignore_index=True)
    for col in ("Author", "Review URL", "Description", "restaurant", "source"):
        df[col] = df[col].astype("string")
    df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")
    return df


def merge_and_dedupe(
    df_reviews: pd.DataFrame,
    df_rescrape: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, int]]:
    rows_reviews = len(df_reviews)
    rows_rescrape = len(df_rescrape)

    rescrape_urls = (
        df_rescrape["Review URL"]
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda s: s != ""]
    )
    rescrape_url_set = set(rescrape_urls)

    if rescrape_url_set and not df_reviews.empty:
        review_urls = df_reviews["Review URL"].fillna("").astype(str).str.strip()
        keep_mask = ~review_urls.isin(rescrape_url_set)
        df_reviews_kept = df_reviews.loc[keep_mask].copy()
    else:
        df_reviews_kept = df_reviews

    dropped_from_reviews = rows_reviews - len(df_reviews_kept)

    if df_rescrape.empty and df_reviews_kept.empty:
        merged = pd.DataFrame(columns=df_reviews.columns)
    elif df_rescrape.empty:
        merged = df_reviews_kept
    elif df_reviews_kept.empty:
        merged = df_rescrape
    else:
        merged = pd.concat([df_reviews_kept, df_rescrape], ignore_index=True)

    stats = {
        "rows_from_reviews": rows_reviews,
        "rows_from_rescrape": rows_rescrape,
        "rows_dropped_reviews_overlap": dropped_from_reviews,
        "rows_merged": len(merged),
    }
    return merged, stats


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--reviews-dir", type=Path, default=root / "data/Reviews")
    p.add_argument("--rescrape-dir", type=Path, default=root / "data/Reviews_rescrape")
    p.add_argument("--output", type=Path, default=root / "data/reviews.parquet")
    args = p.parse_args()

    reviews_dir = args.reviews_dir.resolve()
    rescrape_dir = args.rescrape_dir.resolve()
    output = args.output.resolve()

    if not reviews_dir.is_dir():
        print(f"ERROR: reviews dir not found: {reviews_dir}", file=sys.stderr)
        return 2
    if not rescrape_dir.is_dir():
        print(f"ERROR: rescrape dir not found: {rescrape_dir}", file=sys.stderr)
        return 2

    print(f"Output → {output}")
    df_rescrape = load_directory(rescrape_dir, source="rescrape", log_prefix="[rescrape]")
    df_reviews = load_directory(reviews_dir, source="reviews", log_prefix="[reviews] ")

    merged, stats = merge_and_dedupe(df_reviews, df_rescrape)

    output.parent.mkdir(parents=True, exist_ok=True)
    print(f"Writing {output} ...")
    merged.to_parquet(output, engine="pyarrow", compression="zstd", index=False)

    size_mb = output.stat().st_size / (1024 * 1024)
    print()
    print("=" * 60)
    print(f"Wrote {output} ({size_mb:.1f} MB)")
    print("Summary:")
    for k, v in stats.items():
        print(f"  {k:30s} {v:>12,}")
    if not merged.empty:
        print()
        print("Per-source row counts in output:")
        for src, count in merged["source"].value_counts().items():
            print(f"  {src:30s} {count:>12,}")
        print()
        print(f"Unique restaurants: {merged['restaurant'].nunique():,}")
        print(f"Unique Review URLs: {merged['Review URL'].dropna().nunique():,}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
