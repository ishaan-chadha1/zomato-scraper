"""Phase 1 — walk the Multiple Myeloma sub-forum index and write topic metadata.

Outputs (default under ``data/MyelomaBeacon/``):

- ``topic_urls.txt``  : one topic URL per line (deduped, sorted).
- ``topics.csv``      : one row per topic with starter / replies / views / last-post metadata.
- ``raw_html/_index/<offset>.html`` : raw HTML cache for every index page.

Usage::

    python3 -m scrapers.myeloma_beacon.discover_topics --progress

Re-running is safe: cached index HTML is reused, and the CSV is rewritten
from scratch (deterministic from the cache).
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path
from typing import Any

from .http import (
    BASE_URL,
    build_session,
    cache_path_for_index_page,
    ensure_dir,
    fetch_with_cache,
)
from .parser import parse_index_page

DEFAULT_FORUM_SLUG = "multiple-myeloma"
TOPICS_PER_PAGE = 25

TOPIC_CSV_COLUMNS: tuple[str, ...] = (
    "topic_id",
    "slug",
    "url",
    "title",
    "starter_author",
    "starter_profile_url",
    "start_date",
    "replies",
    "views",
    "last_post_author",
    "last_post_profile_url",
    "last_post_date",
    "last_post_link",
    "has_attachment",
)


def index_page_url(forum_slug: str, page_offset: int) -> str:
    """Return the index URL for ``page_offset`` (0, 25, 50, ...)."""
    if page_offset == 0:
        return f"{BASE_URL}{forum_slug}.html"
    return f"{BASE_URL}{forum_slug}-{page_offset}.html"


def discover_all_topics(
    output_dir: Path,
    *,
    forum_slug: str = DEFAULT_FORUM_SLUG,
    progress: bool = False,
    base_delay: float = 0.5,
    jitter: float = 0.4,
    max_pages: int | None = None,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    """Walk every index page; return the merged list of topic dicts."""
    cache_root = ensure_dir(output_dir / "raw_html")
    session = build_session()

    # Fetch page 1 to learn total pages.
    first_url = index_page_url(forum_slug, 0)
    cache_file = cache_path_for_index_page(cache_root, 0)
    if progress:
        print(f"[index] page 1 of ? : {first_url}", flush=True)
    html = fetch_with_cache(
        session, first_url, cache_file, base_delay=base_delay, jitter=jitter,
        force_refresh=force_refresh,
    )
    parsed = parse_index_page(html)
    total_pages = parsed["total_pages"] or 1
    total_topics_declared = parsed["total_topics"]
    if progress:
        print(
            f"[index] declared total_topics={total_topics_declared} "
            f"total_pages={total_pages}",
            flush=True,
        )

    by_id: dict[int, dict[str, Any]] = {t["topic_id"]: t for t in parsed["topics"]}

    pages_to_fetch = total_pages if max_pages is None else min(total_pages, max_pages)
    for page_no in range(2, pages_to_fetch + 1):
        offset = (page_no - 1) * TOPICS_PER_PAGE
        url = index_page_url(forum_slug, offset)
        cache_file = cache_path_for_index_page(cache_root, offset)
        if progress:
            print(
                f"[index] page {page_no} of {total_pages} : {url}",
                flush=True,
            )
        try:
            html = fetch_with_cache(
                session, url, cache_file,
                base_delay=base_delay, jitter=jitter,
                force_refresh=force_refresh,
            )
        except Exception as exc:
            print(f"[index] page {page_no} FAILED: {exc}", file=sys.stderr, flush=True)
            continue
        for t in parse_index_page(html)["topics"]:
            by_id[t["topic_id"]] = t

    topics = sorted(by_id.values(), key=lambda t: -(t["topic_id"] or 0))

    if progress:
        print(
            f"[index] done: collected {len(topics)} unique topics "
            f"(declared {total_topics_declared})",
            flush=True,
        )

    return topics


def write_topics(output_dir: Path, topics: list[dict[str, Any]]) -> None:
    """Write ``topic_urls.txt`` and ``topics.csv`` atomically."""
    ensure_dir(output_dir)

    urls_path = output_dir / "topic_urls.txt"
    csv_path = output_dir / "topics.csv"

    seen: set[str] = set()
    with urls_path.open("w", encoding="utf-8") as fh:
        for t in topics:
            url = t.get("url")
            if url and url not in seen:
                fh.write(url + "\n")
                seen.add(url)

    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=TOPIC_CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for t in topics:
            row = {k: t.get(k, "") for k in TOPIC_CSV_COLUMNS}
            writer.writerow(row)

    print(f"[index] wrote {urls_path} ({len(seen)} URLs)", flush=True)
    print(f"[index] wrote {csv_path} ({len(topics)} rows)", flush=True)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("Usage::")[0].strip())
    p.add_argument(
        "--forum-slug",
        default=DEFAULT_FORUM_SLUG,
        help=(
            "Sub-forum slug to scrape "
            f"(default: {DEFAULT_FORUM_SLUG}). Examples: treatments-side-effects, "
            "mgus, smoldering-myeloma."
        ),
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=(
            "Output directory (default: data/MyelomaBeacon/<forum-slug>). "
            "Pass this to override."
        ),
    )
    p.add_argument("--progress", action="store_true", help="Print per-page progress")
    p.add_argument("--delay", type=float, default=0.5, help="Base seconds between requests")
    p.add_argument("--jitter", type=float, default=0.4, help="Random jitter added to delay")
    p.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Cap on index pages fetched (default: all pages)",
    )
    p.add_argument(
        "--force-refresh",
        action="store_true",
        help="Ignore cached HTML and re-fetch every index page",
    )
    return p


def resolve_output_dir(forum_slug: str, override: Path | None) -> Path:
    if override is not None:
        return override
    return Path("data") / "MyelomaBeacon" / forum_slug


def main() -> int:
    args = build_parser().parse_args()
    output_dir = resolve_output_dir(args.forum_slug, args.output_dir)
    started = time.time()
    topics = discover_all_topics(
        output_dir,
        forum_slug=args.forum_slug,
        progress=args.progress,
        base_delay=args.delay,
        jitter=args.jitter,
        max_pages=args.max_pages,
        force_refresh=args.force_refresh,
    )
    write_topics(output_dir, topics)
    if args.progress:
        print(f"[index] elapsed {time.time() - started:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
