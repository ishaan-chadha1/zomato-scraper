"""HTTP session + raw-HTML cache for the Myeloma Beacon scraper.

The site (phpBB3 behind Apache) is plain static HTML — a ``requests.Session``
with a browser UA, retries, and polite jitter is enough.

This module also persists the raw HTML of every page we touch under
``raw_html/<topic_id>/<offset>.html`` so we can re-parse offline later
without re-hitting the live site.
"""

from __future__ import annotations

import os
import random
import re
import time
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter

try:
    from urllib3.util.retry import Retry
except ImportError:  # pragma: no cover
    from requests.packages.urllib3.util.retry import Retry  # type: ignore[no-redef]


BASE_HOST = "myelomabeacon.org"
BASE_URL = f"https://{BASE_HOST}/forum/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}


def build_session(*, trust_env: bool = False) -> requests.Session:
    """Return a configured ``requests.Session`` with retries + browser UA."""
    sess = requests.Session()
    sess.headers.update(HEADERS)
    sess.trust_env = trust_env

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)

    # Prime phpBB session cookies by hitting the forum root once.
    try:
        sess.get(BASE_URL, timeout=20)
    except requests.RequestException:
        pass
    return sess


def polite_sleep(base: float = 0.5, jitter: float = 0.4) -> None:
    """Sleep ``base`` plus up to ``jitter`` seconds of uniform noise."""
    if base <= 0 and jitter <= 0:
        return
    time.sleep(base + random.random() * jitter)


_TOPIC_ID_RE = re.compile(r"-t(\d+)(?:-(\d+))?\.html$")


def parse_topic_url(url: str) -> tuple[Optional[int], Optional[int]]:
    """Return ``(topic_id, post_offset)`` from a topic URL, or ``(None, None)``.

    Examples
    --------
    >>> parse_topic_url("https://myelomabeacon.org/forum/biking-with-multiple-myeloma-t1002.html")
    (1002, 0)
    >>> parse_topic_url("https://myelomabeacon.org/forum/biking-with-multiple-myeloma-t1002-30.html")
    (1002, 30)
    """
    m = _TOPIC_ID_RE.search(url)
    if not m:
        return None, None
    topic_id = int(m.group(1))
    offset = int(m.group(2)) if m.group(2) is not None else 0
    return topic_id, offset


def cache_path_for_topic_page(
    cache_root: Path, topic_id: int, post_offset: int
) -> Path:
    """Return the raw-HTML cache path for one topic-post page."""
    return cache_root / str(topic_id) / f"{post_offset}.html"


def cache_path_for_index_page(cache_root: Path, page_offset: int) -> Path:
    """Return the raw-HTML cache path for one index page."""
    return cache_root / "_index" / f"{page_offset}.html"


def fetch_with_cache(
    session: requests.Session,
    url: str,
    cache_file: Optional[Path],
    *,
    timeout: int = 30,
    base_delay: float = 0.5,
    jitter: float = 0.4,
    force_refresh: bool = False,
) -> str:
    """Fetch ``url`` and persist the response body to ``cache_file``.

    If ``cache_file`` exists and ``force_refresh`` is False, reads from disk
    and skips the network call.
    """
    if cache_file is not None and not force_refresh and cache_file.exists():
        try:
            return cache_file.read_text(encoding="utf-8", errors="replace")
        except OSError:
            pass

    resp = session.get(url, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} for {url}")
    # phpBB pages declare UTF-8 in headers; lock it in to avoid mojibake.
    if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
        resp.encoding = "utf-8"
    html = resp.text

    if cache_file is not None:
        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(html, encoding="utf-8")
        except OSError:
            pass

    polite_sleep(base_delay, jitter)
    return html


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_lines(path: Path) -> set[str]:
    """Read a newline-separated file into a set; missing file → empty set."""
    if not path.exists():
        return set()
    out: set[str] = set()
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            s = line.strip()
            if s:
                out.add(s)
    return out


def append_line(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(value.rstrip("\n") + "\n")


__all__ = [
    "BASE_HOST",
    "BASE_URL",
    "HEADERS",
    "append_line",
    "build_session",
    "cache_path_for_index_page",
    "cache_path_for_topic_page",
    "ensure_dir",
    "fetch_with_cache",
    "parse_topic_url",
    "polite_sleep",
    "read_lines",
]
