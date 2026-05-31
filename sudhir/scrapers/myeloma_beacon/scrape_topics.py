"""Phase 2 — open every topic URL and append every post to ``posts.csv``.

Design
------
Workers process one *topic* at a time (not one page at a time), so the
``.completed_topics.txt`` checkpoint is granular and re-runs are cheap:
any topic ID already in that file is skipped.

For each topic we:

1. Fetch page 1 (offset 0) to learn the total page count + per-page offsets.
2. Fetch every other page (offsets 10, 20, ...).
3. Parse all posts and append them to ``posts.csv`` under a lock.
4. Mark the topic ID in ``.completed_topics.txt`` so we never redo it.

Outputs (default under ``data/MyelomaBeacon/``):

- ``posts.csv``                : one row per post (header written on first run).
- ``raw_html/<topic_id>/<offset>.html`` : cached page HTML.
- ``.completed_topics.txt``    : newline-delimited topic IDs already done.
- ``.failed_topics.txt``       : topic IDs that errored, with reason.
- ``scrape.log``               : per-topic progress log.

Usage::

    python3 -m scrapers.myeloma_beacon.scrape_topics --workers 6 --progress
"""

from __future__ import annotations

import argparse
import csv
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from .http import (
    BASE_URL,
    append_line,
    build_session,
    cache_path_for_topic_page,
    ensure_dir,
    fetch_with_cache,
    parse_topic_url,
    read_lines,
)
from .parser import parse_topic_page

DEFAULT_FORUM_SLUG = "multiple-myeloma"

POST_CSV_COLUMNS: tuple[str, ...] = (
    "topic_id",
    "post_id",
    "post_offset",
    "page_index",
    "position_in_topic",
    "title",
    "author",
    "author_profile_url",
    "author_meta_json",
    "posted_at",
    "body_html",
    "body_text",
    "attachment_urls_json",
    "page_url",
)


def topic_page_url(topic_url: str, post_offset: int) -> str:
    """Given the canonical topic URL, return the URL for ``post_offset``.

    >>> topic_page_url("https://myelomabeacon.org/forum/biking-with-multiple-myeloma-t1002.html", 30)
    'https://myelomabeacon.org/forum/biking-with-multiple-myeloma-t1002-30.html'
    """
    if post_offset == 0:
        if "-t" in topic_url and topic_url.endswith(".html"):
            return topic_url
        return topic_url
    # Strip any trailing ``-N`` segment between ``-t<id>`` and ``.html``.
    base = topic_url
    if base.endswith(".html"):
        base = base[: -len(".html")]
    # If base already has ``-<offset>`` after the topic id, strip it.
    import re

    base = re.sub(r"(-t\d+)(?:-\d+)?$", r"\1", base)
    return f"{base}-{post_offset}.html"


def scrape_one_topic(
    topic_url: str,
    *,
    output_dir: Path,
    session,
    base_delay: float,
    jitter: float,
    progress: bool,
    force_refresh: bool,
) -> tuple[int | None, list[dict[str, Any]], str | None]:
    """Scrape every post in one topic. Returns ``(topic_id, post_rows, error_or_None)``."""
    topic_id, _ = parse_topic_url(topic_url)
    if topic_id is None:
        return None, [], f"unparseable topic URL: {topic_url}"

    cache_root = output_dir / "raw_html"
    page_url_0 = topic_page_url(topic_url, 0)
    cache_file = cache_path_for_topic_page(cache_root, topic_id, 0)
    try:
        html = fetch_with_cache(
            session, page_url_0, cache_file,
            base_delay=base_delay, jitter=jitter,
            force_refresh=force_refresh,
        )
    except Exception as exc:
        return topic_id, [], f"fetch page 0 failed: {exc}"

    parsed = parse_topic_page(html, topic_id=topic_id, post_offset=0)
    offsets = sorted(parsed["post_offsets"]) or [0]
    total_pages = parsed["total_pages"] or len(offsets)

    # Fill in any missing offsets between min and max — phpBB always shows
    # boundary links but on long threads the middle pages live behind the
    # "Page X of Y" jumper. Total posts / 10 is a safe upper bound.
    if total_pages and total_pages > 1:
        offsets = sorted(set(offsets) | {i * 10 for i in range(total_pages)})

    # Collect rows from page 1 first.
    rows: list[dict[str, Any]] = []
    page_index_by_offset = {off: idx for idx, off in enumerate(offsets, start=1)}

    def absorb(parsed_page: dict[str, Any], offset: int, page_url: str) -> None:
        page_idx = page_index_by_offset.get(offset, 1)
        base_pos = offset
        for i, p in enumerate(parsed_page["posts"]):
            row = dict(p)
            row["page_index"] = page_idx
            row["position_in_topic"] = base_pos + i
            row["page_url"] = page_url
            row["topic_id"] = topic_id
            rows.append(row)

    absorb(parsed, 0, page_url_0)

    for offset in offsets:
        if offset == 0:
            continue
        page_url = topic_page_url(topic_url, offset)
        cache_file = cache_path_for_topic_page(cache_root, topic_id, offset)
        try:
            html = fetch_with_cache(
                session, page_url, cache_file,
                base_delay=base_delay, jitter=jitter,
                force_refresh=force_refresh,
            )
        except Exception as exc:
            return topic_id, rows, f"fetch offset {offset} failed: {exc}"
        page_parsed = parse_topic_page(html, topic_id=topic_id, post_offset=offset)
        absorb(page_parsed, offset, page_url)

    if progress:
        print(
            f"[topic] t{topic_id} pages={len(offsets)} posts={len(rows)}",
            flush=True,
        )
    return topic_id, rows, None


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------


class PostCSVWriter:
    """Thread-safe append-only CSV writer with deterministic header."""

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        self._fh = None
        self._writer = None
        self._initialize()

    def _initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        is_new = not self.path.exists() or self.path.stat().st_size == 0
        self._fh = self.path.open("a", encoding="utf-8", newline="")
        self._writer = csv.DictWriter(
            self._fh, fieldnames=POST_CSV_COLUMNS, extrasaction="ignore"
        )
        if is_new:
            self._writer.writeheader()
            self._fh.flush()

    def write_rows(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with self._lock:
            for r in rows:
                self._writer.writerow({k: r.get(k, "") for k in POST_CSV_COLUMNS})
            self._fh.flush()

    def close(self) -> None:
        with self._lock:
            if self._fh and not self._fh.closed:
                self._fh.close()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def scrape_all_topics(
    output_dir: Path,
    *,
    workers: int = 6,
    base_delay: float = 0.5,
    jitter: float = 0.4,
    limit: int | None = None,
    only_topic_ids: set[int] | None = None,
    retry_failed: bool = False,
    progress: bool = False,
    force_refresh: bool = False,
) -> dict[str, int]:
    """Walk ``topic_urls.txt`` and scrape every topic not yet completed.

    Returns a dict with counts: ``{"scraped": N, "skipped": M, "failed": K}``.
    """
    urls_path = output_dir / "topic_urls.txt"
    if not urls_path.exists():
        raise FileNotFoundError(
            f"{urls_path} not found — run `python -m scrapers.myeloma_beacon.discover_topics` first"
        )

    completed_path = output_dir / ".completed_topics.txt"
    failed_path = output_dir / ".failed_topics.txt"
    log_path = output_dir / "scrape.log"
    posts_csv = output_dir / "posts.csv"

    completed = {int(x) for x in read_lines(completed_path) if x.isdigit()}
    if retry_failed:
        # Drop failures from completed set so we re-try them.
        failed_ids = {
            int(line.split("\t", 1)[0])
            for line in read_lines(failed_path)
            if line.split("\t", 1)[0].isdigit()
        }
        completed -= failed_ids

    todo: list[str] = []
    seen: set[str] = set()
    for line in urls_path.read_text(encoding="utf-8").splitlines():
        url = line.strip()
        if not url or url in seen:
            continue
        seen.add(url)
        tid, _ = parse_topic_url(url)
        if tid is None:
            continue
        if only_topic_ids is not None and tid not in only_topic_ids:
            continue
        if tid in completed:
            continue
        todo.append(url)

    if limit is not None:
        todo = todo[: max(0, limit)]

    if progress:
        print(
            f"[scrape] {len(todo)} topics to scrape "
            f"({len(completed)} already completed)",
            flush=True,
        )

    writer = PostCSVWriter(posts_csv)
    counts = {"scraped": 0, "skipped": len(completed), "failed": 0}
    sessions: dict[int, Any] = {}

    def session_for_thread() -> Any:
        tid = threading.get_ident()
        s = sessions.get(tid)
        if s is None:
            s = build_session()
            sessions[tid] = s
        return s

    try:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            future_to_url = {
                pool.submit(
                    scrape_one_topic,
                    url,
                    output_dir=output_dir,
                    session=session_for_thread(),
                    base_delay=base_delay,
                    jitter=jitter,
                    progress=progress,
                    force_refresh=force_refresh,
                ): url
                for url in todo
            }
            for fut in as_completed(future_to_url):
                url = future_to_url[fut]
                try:
                    topic_id, rows, err = fut.result()
                except Exception as exc:  # pragma: no cover - safety net
                    topic_id, rows, err = None, [], f"worker exception: {exc}"

                if rows:
                    writer.write_rows(rows)

                if err is None and topic_id is not None:
                    append_line(completed_path, str(topic_id))
                    append_line(
                        log_path,
                        f"{time.strftime('%Y-%m-%dT%H:%M:%S')}\tOK\tt{topic_id}\tposts={len(rows)}\t{url}",
                    )
                    counts["scraped"] += 1
                else:
                    counts["failed"] += 1
                    append_line(
                        failed_path,
                        f"{topic_id if topic_id is not None else '?'}\t{err}\t{url}",
                    )
                    append_line(
                        log_path,
                        f"{time.strftime('%Y-%m-%dT%H:%M:%S')}\tFAIL\tt{topic_id}\t{err}\t{url}",
                    )
                    if progress:
                        print(
                            f"[scrape] FAIL t{topic_id}: {err}",
                            file=sys.stderr,
                            flush=True,
                        )
    finally:
        writer.close()

    if progress:
        print(
            f"[scrape] done: scraped={counts['scraped']} "
            f"skipped={counts['skipped']} failed={counts['failed']}",
            flush=True,
        )
    return counts


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_id_list(text: str) -> set[int]:
    out: set[int] = set()
    for chunk in text.replace(";", ",").split(","):
        chunk = chunk.strip().lstrip("t")
        if chunk.isdigit():
            out.add(int(chunk))
    return out


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("Usage::")[0].strip())
    p.add_argument(
        "--forum-slug",
        default=DEFAULT_FORUM_SLUG,
        help=(
            "Sub-forum slug; selects default output dir "
            f"(default: {DEFAULT_FORUM_SLUG})."
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
    p.add_argument("--workers", type=int, default=6, help="Concurrent worker threads")
    p.add_argument("--delay", type=float, default=0.5, help="Base seconds between requests per worker")
    p.add_argument("--jitter", type=float, default=0.4, help="Random jitter added to delay")
    p.add_argument("--limit", type=int, default=None, help="Cap topics scraped this run")
    p.add_argument(
        "--only",
        type=str,
        default=None,
        help="Comma-separated topic IDs to scrape (overrides default todo)",
    )
    p.add_argument(
        "--retry-failed",
        action="store_true",
        help="Re-attempt topics listed in .failed_topics.txt",
    )
    p.add_argument(
        "--force-refresh",
        action="store_true",
        help="Ignore cached HTML for fetched pages",
    )
    p.add_argument("--progress", action="store_true", help="Print progress")
    return p


def resolve_output_dir(forum_slug: str, override: Path | None) -> Path:
    if override is not None:
        return override
    return Path("data") / "MyelomaBeacon" / forum_slug


def main() -> int:
    args = build_parser().parse_args()
    only_ids = _parse_id_list(args.only) if args.only else None
    output_dir = resolve_output_dir(args.forum_slug, args.output_dir)
    started = time.time()
    counts = scrape_all_topics(
        output_dir,
        workers=args.workers,
        base_delay=args.delay,
        jitter=args.jitter,
        limit=args.limit,
        only_topic_ids=only_ids,
        retry_failed=args.retry_failed,
        progress=args.progress,
        force_refresh=args.force_refresh,
    )
    elapsed = time.time() - started
    print(
        f"[scrape] scraped={counts['scraped']} skipped={counts['skipped']} "
        f"failed={counts['failed']} elapsed={elapsed:.1f}s",
        flush=True,
    )
    return 0 if counts["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
