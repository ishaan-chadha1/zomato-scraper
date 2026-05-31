#!/usr/bin/env python3
"""Fetch HTTP reviews for every restaurant in ``zomato_master_unique.csv``.

For each row: read ``Link`` → call ``get_reviews`` from ``scrapers/reviews/zomato_reviews_http_scraper.py``
→ write one CSV under ``--output-dir`` → append normalized URL to the completed log → continue.

Re-scrape restaurants with no reviews (from ``review_coverage_report.csv``)::

    python3 zomato_reviews_batch.py \\
      --output-dir data/Reviews \\
      --coverage-report data/Reviews/review_coverage_report.csv \\
      --retry-status no_dining_reviews processed_no_csv_match \\
      --force-retry --rewrite-order-to-info --progress

Run from the repository root so paths resolve correctly.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import sys
import time
from pathlib import Path


def _load_scraper():
    root = Path(__file__).resolve().parent
    path = root / "scrapers" / "reviews" / "zomato_reviews_http_scraper.py"
    spec = importlib.util.spec_from_file_location("zomato_reviews_http_scraper", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


DEFAULT_RETRY_STATUSES = ("no_dining_reviews", "processed_no_csv_match")


def _prefer_info_url(link: str) -> str:
    """Use /info listing when master has /order (dining reviews live on base/info path)."""
    u = link.strip().split("#")[0].split("?")[0].rstrip("/")
    if u.endswith("/order"):
        return u[: -len("/order")] + "/info"
    return link.strip()


def _work_from_coverage_report(
    path: Path,
    normalize,
    statuses: tuple[str, ...],
) -> tuple[list[tuple[str, str]], dict[str, dict]]:
    """[(normalized_key, raw_link), ...] and metadata keyed by normalized URL."""
    wanted = set(statuses)
    out: list[tuple[str, str]] = []
    meta: dict[str, dict] = {}
    seen: set[str] = set()
    with path.open(encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        if "link" not in (reader.fieldnames or []):
            raise SystemExit(f"{path} missing 'link' column")
        for row in reader:
            status = (row.get("coverage_status") or "").strip()
            if status not in wanted:
                continue
            raw = (row.get("link") or "").strip()
            if not raw or "zomato.com" not in raw.lower():
                continue
            key = (row.get("normalized_url") or "").strip() or normalize(raw)
            if not key or key in seen:
                continue
            seen.add(key)
            try:
                before = int((row.get("review_count") or "0").strip() or 0)
            except ValueError:
                before = 0
            meta[key] = {
                "restaurant_name": (row.get("restaurant_name") or "").strip(),
                "area_found": (row.get("area_found") or "").strip(),
                "coverage_status_before": status,
                "before_count": before,
                "old_review_csv": (row.get("review_csv_file") or "").strip(),
            }
            out.append((key, raw))
    return out, meta


def _ordered_unique_links(df, normalize) -> list[tuple[str, str]]:
    """Return [(normalized_key, raw_link_from_csv), ...] preserving order, deduped."""
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    col = "Link"
    if col not in df.columns:
        raise SystemExit(f"CSV missing {col!r} column; got {list(df.columns)}")

    for raw in df[col].astype(str):
        link = raw.strip()
        if not link or link.lower() == "nan" or "zomato.com" not in link.lower():
            continue
        key = normalize(link)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append((key, link))
    return out


def main() -> int:
    zrs = _load_scraper()
    normalize = zrs._normalize_restaurant_base_url

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--csv",
        type=Path,
        default=Path("data/restaurants/csv/zomato_master_unique.csv"),
        help="Path to zomato_master_unique.csv",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/reviews"),
        help="Directory for per-restaurant CSV files (default: data/reviews)",
    )
    p.add_argument(
        "--completed-log",
        type=Path,
        default=None,
        help="One normalized URL per line after each success "
        "(default: <output-dir>/.batch_completed_urls.txt)",
    )
    p.add_argument(
        "--failed-log",
        type=Path,
        default=None,
        help="Append failures as CSV (default: <output-dir>/.batch_failed.csv)",
    )
    p.add_argument("--start", type=int, default=0, help="Skip the first N unique rows (after dedupe)")
    p.add_argument("--limit", type=int, default=None, help="Process at most N rows (for testing)")
    p.add_argument(
        "--between-restaurants",
        type=float,
        default=1.0,
        metavar="SEC",
        help="Sleep between restaurants (default: 1.0)",
    )
    p.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore completed log for all rows (still appends to it on success)",
    )
    p.add_argument(
        "--coverage-report",
        type=Path,
        default=None,
        help="Only scrape rows from this report (see --retry-status). "
        "Build with scripts/build_review_coverage_report.py",
    )
    p.add_argument(
        "--retry-status",
        nargs="*",
        default=list(DEFAULT_RETRY_STATUSES),
        metavar="STATUS",
        help="coverage_status values to re-scrape (default: %(default)s)",
    )
    p.add_argument(
        "--force-retry",
        action="store_true",
        help="Required for --coverage-report retries (re-scrape empty restaurants from "
        "the report, not the full master batch). Resume still skips URLs in "
        "--completed-log / <output-dir>/.batch_completed_urls.txt.",
    )
    p.add_argument(
        "--rewrite-order-to-info",
        action="store_true",
        help="Replace trailing /order with /info on each link before fetching",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print how many URLs would be scraped and exit",
    )
    p.add_argument(
        "--retry-summary",
        type=Path,
        default=None,
        help="Append before/after review counts per URL (retry mode only)",
    )
    p.add_argument("--sort", choices=("popular", "new"), default="new")
    p.add_argument(
        "--filters",
        nargs="*",
        default=list(zrs.DEFAULT_FILTERS),
        help="Zomato filter query values",
    )
    p.add_argument("--max-pages", type=int, default=5000, dest="safety_max_pages")
    p.add_argument("--delay", type=float, default=0.25, help="Seconds between page requests")
    p.add_argument("--no-detect-pages", action="store_true")
    p.add_argument("--progress", action="store_true")
    args = p.parse_args()

    import pandas as pd
    import requests

    csv_path = args.csv.resolve()
    if not csv_path.is_file():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        return 1

    out_dir = args.output_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    completed_path = (
        args.completed_log.resolve()
        if args.completed_log
        else (out_dir / ".batch_completed_urls.txt")
    )
    failed_path = (
        args.failed_log.resolve() if args.failed_log else (out_dir / ".batch_failed.csv")
    )

    completed: set[str] = set()
    if completed_path.is_file() and not args.no_resume:
        completed = {
            line.strip()
            for line in completed_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }

    coverage_meta: dict[str, dict] = {}
    if args.coverage_report:
        cov_path = args.coverage_report.resolve()
        if not cov_path.is_file():
            print(f"Coverage report not found: {cov_path}", file=sys.stderr)
            return 1
        statuses = tuple(args.retry_status) if args.retry_status else DEFAULT_RETRY_STATUSES
        work, coverage_meta = _work_from_coverage_report(cov_path, normalize, statuses)
        if not args.force_retry:
            print(
                "Retry mode requires --force-retry (otherwise completed log skips empty restaurants).",
                file=sys.stderr,
            )
            return 1
    else:
        df = pd.read_csv(csv_path)
        work = _ordered_unique_links(df, normalize)

    if args.start:
        work = work[args.start :]
    if args.limit is not None:
        work = work[: args.limit]

    if args.rewrite_order_to_info:
        work = [(key, _prefer_info_url(link)) for key, link in work]

    if args.dry_run:
        print(
            f"Dry run: would scrape {len(work)} restaurants "
            f"(output_dir={out_dir}, filters={args.filters})",
            flush=True,
        )
        for key, link in work[:5]:
            print(f"  sample: {link}  [{key}]", flush=True)
        if len(work) > 5:
            print(f"  ... and {len(work) - 5} more", flush=True)
        return 0

    flt = tuple(args.filters) if args.filters else zrs.DEFAULT_FILTERS
    session = requests.Session()

    failed_exists = failed_path.is_file() and failed_path.stat().st_size > 0
    failed_fp = open(failed_path, "a", newline="", encoding="utf-8")
    failed_writer = csv.writer(failed_fp)
    if not failed_exists:
        failed_writer.writerow(["normalized_url", "raw_link", "error"])

    summary_path = args.retry_summary.resolve() if args.retry_summary else None
    summary_fp = None
    summary_writer = None
    if summary_path and args.coverage_report:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_new = not summary_path.is_file() or summary_path.stat().st_size == 0
        summary_fp = open(summary_path, "a", newline="", encoding="utf-8")
        summary_writer = csv.writer(summary_fp)
        if summary_new:
            summary_writer.writerow(
                [
                    "normalized_url",
                    "link",
                    "restaurant_name",
                    "area_found",
                    "coverage_status_before",
                    "before_count",
                    "after_count",
                    "delta",
                    "outcome",
                    "old_review_csv",
                ]
            )

    total = len(work)
    pending = sum(1 for key, _ in work if key not in completed)
    already_done = total - pending
    if already_done:
        print(
            f"Resuming: {already_done} already in {completed_path.name}, "
            f"{pending} left to scrape.",
            flush=True,
        )
    done = 0
    skipped = 0
    errors = 0
    attempt = 0

    try:
        for i, (key, link) in enumerate(work, start=1):
            if key in completed:
                skipped += 1
                continue
            attempt += 1
            print(f"[{attempt}/{pending}] ({i}/{total}) {link}", flush=True)
            before_count = coverage_meta.get(key, {}).get("before_count", 0)
            try:
                review_df = zrs.get_reviews(
                    link,
                    sort=args.sort,
                    save=True,
                    save_empty=True,
                    save_dir=str(out_dir),
                    filters=flt,
                    safety_max_pages=args.safety_max_pages,
                    delay_sec=args.delay,
                    session=session,
                    progress_log=args.progress,
                    detect_page_count=not args.no_detect_pages,
                )
            except Exception as e:
                errors += 1
                failed_writer.writerow([key, link, str(e)])
                failed_fp.flush()
                print(f"  ERROR: {e}", file=sys.stderr, flush=True)
                if summary_writer:
                    m = coverage_meta.get(key, {})
                    summary_writer.writerow(
                        [
                            key,
                            link,
                            m.get("restaurant_name", ""),
                            m.get("area_found", ""),
                            m.get("coverage_status_before", ""),
                            before_count,
                            "",
                            "",
                            "error",
                            m.get("old_review_csv", ""),
                        ]
                    )
                    summary_fp.flush()
            else:
                done += 1
                after_count = len(review_df)
                delta = after_count - before_count
                if after_count > 0 and before_count == 0:
                    outcome = "gained_reviews"
                elif after_count > before_count:
                    outcome = "more_reviews"
                elif after_count == 0:
                    outcome = "still_empty"
                else:
                    outcome = "unchanged"
                if summary_writer:
                    m = coverage_meta.get(key, {})
                    summary_writer.writerow(
                        [
                            key,
                            link,
                            m.get("restaurant_name", ""),
                            m.get("area_found", ""),
                            m.get("coverage_status_before", ""),
                            before_count,
                            after_count,
                            delta,
                            outcome,
                            m.get("old_review_csv", ""),
                        ]
                    )
                    summary_fp.flush()
                with open(completed_path, "a", encoding="utf-8") as cf:
                    cf.write(key + "\n")
                completed.add(key)

            if args.between_restaurants > 0:
                time.sleep(args.between_restaurants)
    finally:
        failed_fp.close()
        if summary_fp:
            summary_fp.close()

    print(
        f"Finished: scraped_ok={done} skipped_already_done={skipped} errors={errors} "
        f"output_dir={out_dir}",
        flush=True,
    )
    return 0 if errors == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
