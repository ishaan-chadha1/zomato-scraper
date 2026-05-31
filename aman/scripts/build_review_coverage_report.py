#!/usr/bin/env python3
"""Build a coverage report: master restaurant list vs per-restaurant review CSVs."""

from __future__ import annotations

import argparse
import csv
import importlib.util
import re
from collections import defaultdict
from pathlib import Path


def _load_scraper(root: Path):
    path = root / "scrapers" / "reviews" / "zomato_reviews_http_scraper.py"
    spec = importlib.util.spec_from_file_location("zomato_reviews_http_scraper", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _slugify(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (text or "").lower())
    return re.sub(r"-+", "-", s).strip("-")


def _url_slug(normalized_url: str) -> str:
    return normalized_url.rstrip("/").rsplit("/", maxsplit=1)[-1]


def _classify_url(raw_link: str) -> str:
    low = raw_link.lower()
    if "/delivery/dish-" in low or "/delivery/dish/" in low:
        return "delivery_dish"
    if low.endswith("/order") or "/order?" in low:
        return "order"
    if low.endswith("/info") or "/info?" in low:
        return "info"
    if "/delivery/" in low:
        return "delivery_other"
    return "other"


def _candidate_names(row: dict, zrs) -> list[str]:
    area = (row.get("Area Found") or "").strip()
    name = (row.get("Restaurant Name") or "").strip()
    link = (row.get("Link") or "").strip()
    out: list[str] = []
    if name and area:
        out.append(f"{name}, {area}, Bangalore")
        out.append(f"{name}, {area}")
    if name:
        out.append(name)
    if link:
        out.append(zrs.restaurant_name_from_url(link))
    return [zrs.sanitize_file_name(n) for n in out if n]


def _token_set(slug: str) -> set[str]:
    return {t for t in slug.split("-") if len(t) > 2}


def _score_slug_match(url_slug: str, file_slug: str) -> float:
    if not url_slug or not file_slug:
        return 0.0
    if url_slug == file_slug:
        return 1.0
    if url_slug in file_slug or file_slug in url_slug:
        return 0.92
    a, b = _token_set(url_slug), _token_set(file_slug)
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _count_reviews(csv_path: Path) -> int:
    try:
        with csv_path.open(encoding="utf-8", errors="replace") as f:
            return max(0, sum(1 for _ in f) - 1)
    except OSError:
        return -1


def _coverage_status(
    *,
    review_count: int,
    csv_matched: bool,
    in_completed: bool,
    url_type: str,
) -> str:
    if url_type == "delivery_dish":
        return "bad_url_delivery_dish"
    if not in_completed:
        return "not_processed"
    if not csv_matched:
        return "processed_no_csv_match"
    if review_count < 0:
        return "csv_read_error"
    if review_count == 0:
        return "no_dining_reviews"
    return "has_reviews"


def build_report(
    *,
    root: Path,
    master_csv: Path,
    reviews_dir: Path,
    completed_log: Path,
    output_csv: Path,
) -> dict:
    zrs = _load_scraper(root)

    completed: set[str] = set()
    if completed_log.is_file():
        completed = {
            line.strip()
            for line in completed_log.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }

    # Index review CSVs by slugified stem and by exact stem.
    files_by_slug: dict[str, list[str]] = defaultdict(list)
    all_files: list[str] = []
    for p in reviews_dir.glob("*.csv"):
        if p.name.startswith("."):
            continue
        all_files.append(p.name)
        stem_slug = _slugify(p.stem)
        files_by_slug[stem_slug].append(p.name)

    rows_out: list[dict] = []
    stats = defaultdict(int)

    with master_csv.open(encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_link = (row.get("Link") or "").strip()
            if not raw_link or "zomato.com" not in raw_link.lower():
                continue

            normalized = zrs._normalize_restaurant_base_url(raw_link)
            url_slug = _url_slug(normalized)
            url_type = _classify_url(raw_link)
            in_completed = normalized in completed

            matched_file = ""
            match_score = 0.0

            # 1) Exact candidate names from master row.
            for cand in _candidate_names(row, zrs):
                cand_slug = _slugify(cand)
                if cand_slug in files_by_slug:
                    matched_file = sorted(files_by_slug[cand_slug])[0]
                    match_score = 1.0
                    break
                exact_path = reviews_dir / f"{cand}.csv"
                if exact_path.is_file():
                    matched_file = exact_path.name
                    match_score = 1.0
                    break

            # 2) Slug similarity against all files.
            if not matched_file and url_slug:
                best_name = ""
                best_score = 0.0
                for fname in all_files:
                    file_slug = _slugify(Path(fname).stem)
                    score = _score_slug_match(url_slug, file_slug)
                    if score > best_score:
                        best_score = score
                        best_name = fname
                if best_score >= 0.55:
                    matched_file = best_name
                    match_score = best_score

            review_count = _count_reviews(reviews_dir / matched_file) if matched_file else 0
            csv_matched = bool(matched_file)
            status = _coverage_status(
                review_count=review_count,
                csv_matched=csv_matched,
                in_completed=in_completed,
                url_type=url_type,
            )
            stats[status] += 1

            rows_out.append(
                {
                    "area_found": (row.get("Area Found") or "").strip(),
                    "restaurant_name": (row.get("Restaurant Name") or "").strip(),
                    "master_rating": (row.get("Rating") or "").strip(),
                    "cuisine": (row.get("Cuisine") or "").strip(),
                    "price": (row.get("Price") or "").strip(),
                    "link": raw_link,
                    "normalized_url": normalized,
                    "url_slug": url_slug,
                    "url_type": url_type,
                    "in_completed_log": "yes" if in_completed else "no",
                    "review_csv_file": matched_file,
                    "match_score": f"{match_score:.2f}" if matched_file else "",
                    "review_count": review_count,
                    "coverage_status": status,
                }
            )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows_out[0].keys()) if rows_out else []
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)

    stats["total_master_rows"] = len(rows_out)
    return dict(stats)


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--master-csv",
        type=Path,
        default=root / "data/restaurants/csv/zomato_master_unique.csv",
    )
    p.add_argument(
        "--reviews-dir",
        type=Path,
        default=root / "data/Reviews",
    )
    p.add_argument(
        "--completed-log",
        type=Path,
        default=None,
        help="Default: <reviews-dir>/.batch_completed_urls.txt",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=root / "data/Reviews/review_coverage_report.csv",
    )
    args = p.parse_args()

    reviews_dir = args.reviews_dir.resolve()
    completed_log = (
        args.completed_log.resolve()
        if args.completed_log
        else reviews_dir / ".batch_completed_urls.txt"
    )
    output_csv = args.output.resolve()

    stats = build_report(
        root=root,
        master_csv=args.master_csv.resolve(),
        reviews_dir=reviews_dir,
        completed_log=completed_log,
        output_csv=output_csv,
    )

    print(f"Wrote {output_csv}")
    print("Summary:")
    for key in sorted(stats):
        if key != "total_master_rows":
            print(f"  {key}: {stats[key]}")
    print(f"  total_master_rows: {stats.get('total_master_rows', 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
