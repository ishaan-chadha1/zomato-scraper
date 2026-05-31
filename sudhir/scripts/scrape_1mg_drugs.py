#!/usr/bin/env python3
"""Scrape Tata 1mg drug pages (/drugs/*) into a CSV with JSON columns for nested data.

Reads URLs from scripts/output/one_mg_urls.txt (or --url-file), fetches HTML, parses
window.PRELOADED_STATE, and appends one row per URL to drugs.csv. Resumes via
.batch_completed_urls.txt.

Run from repository root::

    python3 scripts/scrape_1mg_drugs.py --limit 100 --progress
"""

from __future__ import annotations

import argparse
import csv
import json
import queue
import re
import threading
import time
from html import unescape
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

DRUGS_PREFIX = "https://www.1mg.com/drugs/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

CSV_COLUMNS = [
    "url",
    "fetch_status",
    "fetch_http_status",
    "fetch_error",
    "sku_id",
    "name",
    "slug",
    "prescription_required",
    "salt_composition",
    "storage",
    "marketer_name",
    "marketer_address",
    "marketer_url",
    "manufacturer_name",
    "price",
    "discounted_price",
    "discount_percent",
    "per_unit_price",
    "in_stock",
    "available",
    "therapeutic_class",
    "introduction_text",
    "how_to_use_text",
    "how_it_works_text",
    "missed_dose_text",
    "written_by",
    "written_by_qualifications",
    "reviewed_by",
    "reviewed_by_qualifications",
    "last_updated",
    "pack_info",
    "uses_json",
    "benefits_json",
    "side_effects_json",
    "safety_advice_json",
    "quick_tips_json",
    "fact_box_json",
    "substitutes_json",
    "drug_interactions_json",
    "faqs_json",
    "references_json",
    "related_doctors_json",
    "related_lab_tests_json",
    "breadcrumbs_json",
    "meta_title",
    "meta_description",
]

_PRELOADED_RE = re.compile(
    r"window\.PRELOADED_STATE\s*=\s*(\[.*?\]);",
    re.DOTALL,
)
_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str | None) -> str:
    if not text:
        return ""
    return unescape(_TAG_RE.sub(" ", text)).replace("\xa0", " ").strip()


def _json_col(value: Any) -> str:
    if value is None:
        return ""
    return json.dumps(value, ensure_ascii=False)


def _scalar_text(value: Any) -> str:
    """Coerce API fields that may be str or {displayText, text, ...} into a CSV cell."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, dict):
        for key in ("displayText", "display_text", "text", "label", "value", "date"):
            if value.get(key):
                return _scalar_text(value[key])
        return json.dumps(value, ensure_ascii=False)
    return str(value).strip()


def _first_display_text(blocks: Any) -> str:
    if not isinstance(blocks, list):
        return ""
    for block in blocks:
        if isinstance(block, dict):
            text = block.get("display_text") or block.get("displayText") or ""
            if text:
                return _strip_html(str(text))
    return ""


def _author(authored_by: dict | None, role: str) -> tuple[str, str]:
    if not isinstance(authored_by, dict):
        return "", ""
    for author in authored_by.get("authors") or []:
        if not isinstance(author, dict):
            continue
        if (author.get("role") or "").strip() == role:
            return (
                (author.get("name") or "").strip(),
                (author.get("qualifications") or "").strip(),
            )
    return "", ""


def parse_preloaded_state(html: str) -> dict[str, Any] | None:
    match = _PRELOADED_RE.search(html)
    if not match:
        return None
    try:
        outer = json.loads(match.group(1))
    except json.JSONDecodeError:
        return None
    if not outer:
        return None
    try:
        return json.loads(outer[0]) if isinstance(outer[0], str) else outer[0]
    except (json.JSONDecodeError, IndexError, TypeError):
        return None


def parse_drug_page(url: str, html: str, http_status: int) -> dict[str, str]:
    row = {col: "" for col in CSV_COLUMNS}
    row["url"] = url
    row["fetch_http_status"] = str(http_status)

    state = parse_preloaded_state(html)
    if state is None:
        row["fetch_status"] = "no_preloaded_state"
        row["fetch_error"] = "PRELOADED_STATE not found or invalid"
        return row

    drug_page = state.get("drugPage") or {}
    if drug_page.get("drugNotFound"):
        row["fetch_status"] = "drug_not_found"
        return row

    drug_info = drug_page.get("drugInfo")
    if not isinstance(drug_info, dict):
        row["fetch_status"] = "no_drug_info"
        return row

    sku = drug_info.get("sku")
    if not isinstance(sku, dict):
        row["fetch_status"] = "no_sku"
        return row

    row["fetch_status"] = "ok"
    row["sku_id"] = str(sku.get("id") or "")
    row["name"] = _scalar_text(sku.get("name"))
    row["slug"] = _scalar_text(sku.get("slug"))
    row["prescription_required"] = str(bool(sku.get("prescriptionRequired")))
    row["price"] = "" if sku.get("price") is None else str(sku.get("price"))
    row["discounted_price"] = (
        "" if sku.get("discountedPrice") is None else str(sku.get("discountedPrice"))
    )
    row["discount_percent"] = (
        "" if sku.get("discountPercent") is None else str(sku.get("discountPercent"))
    )
    row["per_unit_price"] = _scalar_text(sku.get("perUnitPrice"))
    row["in_stock"] = str(bool(sku.get("inStock")))
    row["available"] = str(bool(sku.get("available")))
    row["therapeutic_class"] = _scalar_text(sku.get("therapeuticClass"))

    sc = sku.get("shortComposition")
    if isinstance(sc, dict):
        row["salt_composition"] = _strip_html(sc.get("displayText") or sc.get("display_text") or "")
    else:
        row["salt_composition"] = _strip_html(str(sc or ""))

    storage = sku.get("storageCondition")
    if isinstance(storage, dict):
        row["storage"] = _strip_html(storage.get("display_text") or storage.get("displayText") or "")
    else:
        row["storage"] = _strip_html(str(storage or ""))

    marketer = sku.get("marketer")
    if isinstance(marketer, dict):
        row["marketer_name"] = (marketer.get("name") or "").strip()
        row["marketer_address"] = (marketer.get("address") or "").strip()
        row["marketer_url"] = (marketer.get("url") or "").strip()

    manufacturer = sku.get("manufacturer")
    if isinstance(manufacturer, dict):
        row["manufacturer_name"] = (manufacturer.get("name") or "").strip()

    row["introduction_text"] = _first_display_text(sku.get("introduction"))
    row["how_to_use_text"] = _first_display_text(sku.get("howToTake"))
    row["how_it_works_text"] = _first_display_text(sku.get("mechanismOfAction"))

    missed = sku.get("missedDose")
    if isinstance(missed, dict):
        row["missed_dose_text"] = _strip_html(missed.get("displayText") or missed.get("display_text") or "")

    authored = drug_info.get("authoredBy")
    written_name, written_qual = _author(authored, "Written By")
    reviewed_name, reviewed_qual = _author(authored, "Reviewed By")
    row["written_by"] = written_name
    row["written_by_qualifications"] = written_qual
    row["reviewed_by"] = reviewed_name
    row["reviewed_by_qualifications"] = reviewed_qual
    if isinstance(authored, dict):
        row["last_updated"] = _scalar_text(
            authored.get("lastUpdated") or authored.get("last_updated")
        )

    quantities = sku.get("quantities")
    if isinstance(quantities, list) and quantities:
        row["pack_info"] = _json_col(quantities)
    elif isinstance(sku.get("quantityProduct"), dict):
        row["pack_info"] = _json_col(sku.get("quantityProduct"))

    fact_box = drug_info.get("skuFactBox") or sku.get("factBox")
    row["uses_json"] = _json_col(sku.get("uses"))
    row["benefits_json"] = _json_col(sku.get("benefits"))
    row["side_effects_json"] = _json_col(sku.get("sideEffects"))
    row["safety_advice_json"] = _json_col(sku.get("warning"))
    row["quick_tips_json"] = _json_col(sku.get("expertAdvice"))
    row["fact_box_json"] = _json_col(fact_box)
    row["substitutes_json"] = _json_col(drug_info.get("substitute"))
    row["drug_interactions_json"] = _json_col(sku.get("drugInteraction"))
    row["faqs_json"] = _json_col(sku.get("faqs"))
    row["references_json"] = _json_col(sku.get("references"))
    row["related_doctors_json"] = _json_col(drug_info.get("relatedDoctor"))
    row["related_lab_tests_json"] = _json_col(drug_info.get("relatedLabTest"))
    row["breadcrumbs_json"] = _json_col(drug_info.get("breadcrumbs"))

    meta = drug_info.get("metaData")
    if isinstance(meta, dict):
        row["meta_title"] = (meta.get("title") or "").strip()
        row["meta_description"] = (meta.get("description") or meta.get("meta_desc") or "").strip()

    return row


def load_drug_urls(path: Path, limit: int | None) -> list[str]:
    urls: list[str] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            url = line.strip()
            if not url.startswith(DRUGS_PREFIX):
                continue
            urls.append(url)
            if limit is not None and len(urls) >= limit:
                break
    return urls


def load_completed(path: Path) -> set[str]:
    if not path.is_file():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


class DrugScraper:
    def __init__(
        self,
        urls: list[str],
        *,
        completed_path: Path,
        failed_path: Path,
        csv_path: Path,
        concurrency: int,
        rate_limit: float,
        timeout: float,
        max_retries: int,
        dry_run: bool,
        progress: bool,
    ):
        self.urls = urls
        self.completed_path = completed_path
        self.failed_path = failed_path
        self.csv_path = csv_path
        self.concurrency = concurrency
        self.rate_limit = rate_limit
        self.timeout = timeout
        self.max_retries = max_retries
        self.dry_run = dry_run
        self.progress = progress

        self.work_q: queue.Queue[str] = queue.Queue()
        self.lock = threading.Lock()
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.stats = {"ok": 0, "failed": 0, "skipped": 0}

    def fetch_html(self, url: str) -> tuple[int, str, str | None]:
        last_err: str | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                if resp.status_code >= 500:
                    last_err = f"HTTP {resp.status_code}"
                    time.sleep(self.rate_limit * attempt)
                    continue
                return resp.status_code, resp.text, None
            except requests.RequestException as exc:
                last_err = str(exc)
                time.sleep(self.rate_limit * attempt)
        return 0, "", last_err

    def _append_csv_row(self, row: dict[str, str], write_header: bool) -> None:
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.csv_path, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            writer.writerow(row)

    def _mark_completed(self, url: str) -> None:
        self.completed_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.completed_path, "a", encoding="utf-8") as f:
            f.write(url + "\n")

    def _mark_failed(self, url: str, status: str, error: str) -> None:
        self.failed_path.parent.mkdir(parents=True, exist_ok=True)
        new_file = not self.failed_path.exists()
        with open(self.failed_path, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            if new_file:
                writer.writerow(["url", "fetch_status", "fetch_error"])
            writer.writerow([url, status, error])

    def worker(self, completed: set[str]) -> None:
        while True:
            try:
                url = self.work_q.get(timeout=3)
            except queue.Empty:
                return

            if url in completed:
                with self.lock:
                    self.stats["skipped"] += 1
                self.work_q.task_done()
                continue

            if self.dry_run:
                if self.progress:
                    print(f"[dry-run] would fetch {url}", flush=True)
                with self.lock:
                    self.stats["skipped"] += 1
                self.work_q.task_done()
                continue

            http_status, html, fetch_err = self.fetch_html(url)
            if fetch_err:
                row = {col: "" for col in CSV_COLUMNS}
                row.update(
                    {
                        "url": url,
                        "fetch_status": "fetch_error",
                        "fetch_http_status": str(http_status),
                        "fetch_error": fetch_err,
                    }
                )
                with self.lock:
                    self._append_csv_row(row, write_header=not self.csv_path.exists())
                    self._mark_failed(url, "fetch_error", fetch_err)
                    self.stats["failed"] += 1
                if self.progress:
                    print(f"[fail] {url} -> {fetch_err}", flush=True)
            else:
                try:
                    row = parse_drug_page(url, html, http_status)
                except Exception as exc:
                    row = {col: "" for col in CSV_COLUMNS}
                    row.update(
                        {
                            "url": url,
                            "fetch_status": "parse_error",
                            "fetch_http_status": str(http_status),
                            "fetch_error": str(exc),
                        }
                    )
                with self.lock:
                    write_header = not self.csv_path.exists()
                    self._append_csv_row(row, write_header=write_header)
                    self._mark_completed(url)
                    completed.add(url)
                    if row["fetch_status"] == "ok":
                        self.stats["ok"] += 1
                    else:
                        self._mark_failed(url, row["fetch_status"], row.get("fetch_error", ""))
                        self.stats["failed"] += 1
                if self.progress:
                    print(f"[{row['fetch_status']}] {url} ({row.get('name') or '-'})", flush=True)

            time.sleep(self.rate_limit)
            self.work_q.task_done()

    def run(self, completed: set[str]) -> None:
        for url in self.urls:
            self.work_q.put(url)

        threads = [
            threading.Thread(target=self.worker, args=(completed,), daemon=True)
            for _ in range(self.concurrency)
        ]
        for t in threads:
            t.start()
        self.work_q.join()
        for t in threads:
            t.join(timeout=0)


def build_parser() -> argparse.ArgumentParser:
    root = Path(__file__).resolve().parent.parent
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--url-file",
        type=Path,
        default=root / "scripts" / "output" / "one_mg_urls.txt",
        help="URL list (one per line)",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=root / "data" / "1mg_drugs",
        help="Directory for drugs.csv and checkpoint files",
    )
    p.add_argument("--limit", type=int, default=None, help="Max drug URLs to process")
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--rate-limit", type=float, default=1.0, help="Seconds between requests per worker")
    p.add_argument("--timeout", type=float, default=15.0)
    p.add_argument("--max-retries", type=int, default=3)
    p.add_argument("--dry-run", action="store_true", help="List URLs only, no HTTP or writes")
    p.add_argument("--no-resume", action="store_true", help="Ignore completed log")
    p.add_argument("--progress", action="store_true")
    return p


def main() -> int:
    args = build_parser().parse_args()
    out_dir = args.output_dir
    csv_path = out_dir / "drugs.csv"
    completed_path = out_dir / ".batch_completed_urls.txt"
    failed_path = out_dir / ".batch_failed.csv"

    if not args.url_file.is_file():
        raise SystemExit(f"URL file not found: {args.url_file}")

    urls = load_drug_urls(args.url_file, args.limit)
    if not urls:
        raise SystemExit(f"No {DRUGS_PREFIX} URLs found in {args.url_file}")

    completed = set() if args.no_resume else load_completed(completed_path)
    pending = [u for u in urls if u not in completed]

    print(
        f"[1mg] drug URLs in batch: {len(urls)} | already done: {len(urls) - len(pending)} "
        f"| pending: {len(pending)}",
        flush=True,
    )
    if args.dry_run:
        for u in pending[:20]:
            print(f"  {u}")
        if len(pending) > 20:
            print(f"  ... and {len(pending) - 20} more")
        return 0

    scraper = DrugScraper(
        pending,
        completed_path=completed_path,
        failed_path=failed_path,
        csv_path=csv_path,
        concurrency=args.concurrency,
        rate_limit=args.rate_limit,
        timeout=args.timeout,
        max_retries=args.max_retries,
        dry_run=False,
        progress=args.progress,
    )
    scraper.run(completed)
    print(
        f"[done] ok={scraper.stats['ok']} failed={scraper.stats['failed']} "
        f"skipped={scraper.stats['skipped']} -> {csv_path}",
        flush=True,
    )
    return 0 if scraper.stats["failed"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
