#!/usr/bin/env python3
"""Scrape MyMyelomaTeam resource listings and articles via HTTP (requests + BeautifulSoup).

This mirrors the style of ``zomato_reviews_http_scraper.py``: a ``requests.Session``,
browser-like headers, HTML parsing, and optional embedded JSON (``__NEXT_DATA__``).

**Listing phase**

1. GET the listing URL (default: Types, sort=most_liked).
2. Collect article URLs from:

   - ``<a href="…/resources/{slug}">`` (single-segment slugs only)
   - Any string in ``__NEXT_DATA__`` JSON (if present) that looks like an article URL
   - Raw HTML regex fallback

3. **Pagination** (replaces scrolling and repeatedly clicking **Show more** in a browser):

   - Appends ``page=2``, ``page=3``, … while new article URLs appear.
   - Also follows ``rel="next"`` in the HTTP ``Link`` header when present.

If the site uses a different query key (for example ``offset`` / ``cursor``), open
DevTools → Network, click **Show more** once, and inspect the request that returns more
rows; then adjust ``--listing-page-param`` or extend ``merge_query`` / add that URL
pattern locally.

**Article phase**

For each discovered ``/resources/{slug}`` URL, collects:

- Title (``<title>``, ``og:title``, ``<h1>``)
- Description meta
- Visible article text (``<article>``, ``main``, or largest text block heuristic)
- All ``application/ld+json`` payloads (often Article / MedicalWebPage)

Output: one CSV of articles plus optional per-article JSON sidecars.

After listing discovery, if the HTML contains a heading like **Types (144)** but fewer
article URLs were collected, the script prints a short hint about **Show more** and
DevTools.

Example::

    python3 scrapers/resources/mymyelomateam_resources_http.py \\
      --listing-url "https://www.mymyelomateam.com/resources/types?sort=most_liked" \\
      --progress --delay 0.35
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import pandas as pd
import requests
from bs4 import BeautifulSoup, FeatureNotFound

BASE_HOST = "www.mymyelomateam.com"
LISTING_DEFAULT = f"https://{BASE_HOST}/resources/types?sort=most_liked"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Listing rows that are not single-slug articles (adjust if you see false negatives).
RESERVED_SLUGS = frozenset(
    {
        "types",
        "treatments",
        "topics",
        "roles",
        "multiple-diagnoses",
        "search",
        "users",
        "about",
        "help",
        "sign_in",
        "sign_up",
        "privacy-choices",
    }
)

_ARTICLE_HREF_RE = re.compile(
    r'(https?://(?:www\.)?mymyelomateam\.com/resources/[a-z0-9][a-z0-9-]*)/?(?:["\s#>]|$)',
    re.IGNORECASE,
)
# Next/React often emit root-relative links; these do not match _ARTICLE_HREF_RE.
_ARTICLE_REL_PATH_RE = re.compile(
    r'(?:^|["\'=\s])(/resources/[a-z0-9][a-z0-9-]*)(?:["\'\s#>]|$)',
    re.IGNORECASE,
)


def parse_html(text: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(text, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(text, "html.parser")


def merge_query(url: str, updates: Mapping[str, str]) -> str:
    p = urlparse(url)
    pairs = list(parse_qsl(p.query, keep_blank_values=True))
    d = dict(pairs)
    for k, v in updates.items():
        d[k] = v
    new_q = urlencode(sorted(d.items()))
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, p.fragment))


DEFAULT_RESOURCE_HUBS: tuple[str, ...] = (
    "types",
    "treatments",
    "topics",
    "roles",
    "multiple-diagnoses",
)
# ``recent`` = no ``sort`` query (Recently Published).
DEFAULT_LISTING_SORTS: tuple[str, ...] = ("recent", "most_liked", "most_comments")


def resource_listing_url(hub_slug: str, sort_key: str) -> str:
    """Return the listing page URL for a hub (e.g. ``types``) and sort mode."""
    slug = hub_slug.strip().lower()
    sk = (sort_key or "recent").strip().lower()
    base = f"https://{BASE_HOST}/resources/{slug}"
    if sk in ("recent", "recently-published", "recently_published", ""):
        return base
    if sk == "most_liked":
        return merge_query(base, {"sort": "most_liked"})
    if sk == "most_comments":
        return merge_query(base, {"sort": "most_comments"})
    raise ValueError(
        f"Unknown sort key {sort_key!r}; expected one of: recent, most_liked, most_comments"
    )


def _host_ok(netloc: str) -> bool:
    n = (netloc or "").lower()
    return n == BASE_HOST or n == "mymyelomateam.com"


def normalize_article_url(href: str) -> str | None:
    if not href:
        return None
    href = href.strip()
    if href.startswith("//"):
        href = "https:" + href
    if href.lower().startswith("javascript:") or href.startswith("#"):
        return None
    p = urlparse(href)
    if p.scheme in ("http", "https") and _host_ok(p.netloc):
        abs_href = href
    elif not p.scheme and not p.netloc and p.path.startswith("/"):
        # Root-relative and other path-only hrefs from the live DOM (lazy-loaded cards).
        abs_href = urljoin(f"https://{BASE_HOST}", href)
    else:
        return None
    p = urlparse(abs_href)
    if p.scheme not in ("http", "https") or not _host_ok(p.netloc):
        return None
    parts = [x for x in p.path.split("/") if x]
    if len(parts) != 2 or parts[0].lower() != "resources":
        return None
    slug = parts[1]
    if not slug or slug.lower() in RESERVED_SLUGS:
        return None
    return f"https://{BASE_HOST}/resources/{slug}"


def extract_urls_from_html(page_text: str) -> set[str]:
    found: set[str] = set()
    soup = parse_html(page_text)
    for a in soup.find_all("a", href=True):
        u = normalize_article_url(a["href"])
        if u:
            found.add(u)
    for m in _ARTICLE_HREF_RE.finditer(page_text):
        u = normalize_article_url(m.group(1))
        if u:
            found.add(u)
    for m in _ARTICLE_REL_PATH_RE.finditer(page_text):
        u = normalize_article_url(m.group(1))
        if u:
            found.add(u)
    return found


def _balanced_json_object(page_text: str, open_brace_idx: int) -> str | None:
    if open_brace_idx < 0 or open_brace_idx >= len(page_text):
        return None
    depth = 0
    in_str = False
    esc = False
    quote: str | None = None
    for i in range(open_brace_idx, len(page_text)):
        c = page_text[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif quote and c == quote:
                in_str = False
                quote = None
            continue
        if c in ("'", '"'):
            in_str = True
            quote = c
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return page_text[open_brace_idx : i + 1]
    return None


def parse_next_data(page_text: str) -> dict[str, Any] | None:
    """Parse Next.js ``__NEXT_DATA__`` object if embedded."""
    marker = 'id="__NEXT_DATA__"'
    i = page_text.find(marker)
    if i == -1:
        i = page_text.find("__NEXT_DATA__")
    if i == -1:
        return None
    j = page_text.find("{", i)
    if j == -1:
        return None
    raw = _balanced_json_object(page_text, j)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _walk_collect_resource_urls(obj: Any, out: set[str]) -> None:
    if isinstance(obj, str):
        s = obj.strip()
        if "/resources/" not in s:
            return
        # Standalone absolute URLs, or root-relative paths in JSON (lazy lists).
        if (
            BASE_HOST in s.replace("mymyelomateam.com", BASE_HOST)
            or s.startswith("/resources/")
        ) and len(s) < 2048:
            u = normalize_article_url(s)
            if u:
                out.add(u)
        return
    if isinstance(obj, dict):
        for v in obj.values():
            _walk_collect_resource_urls(v, out)
        return
    if isinstance(obj, list):
        for v in obj:
            _walk_collect_resource_urls(v, out)


def extract_urls_from_next_data(page_text: str) -> set[str]:
    data = parse_next_data(page_text)
    if not data:
        return set()
    out: set[str] = set()
    _walk_collect_resource_urls(data, out)
    return out


def parse_declared_listing_total(page_text: str) -> int | None:
    """e.g. ``Types (144)`` or ``Treatments (71)`` — count from the listing section heading."""
    m = re.search(
        r"(?:Types|Treatments|Topics|Roles|Multiple\s+Diagnoses)\s*\(\s*(\d+)\s*\)",
        page_text,
        re.IGNORECASE,
    )
    if not m:
        return None
    try:
        n = int(m.group(1))
    except ValueError:
        return None
    return n if n >= 1 else None


def parse_link_next_url(response: requests.Response) -> str | None:
    link = response.headers.get("Link")
    if not link:
        return None
    for part in link.split(","):
        section = part.strip()
        if 'rel="next"' not in section and "rel='next'" not in section and "rel=next" not in section:
            continue
        m = re.search(r"<([^>]+)>", section)
        if m:
            target = m.group(1).strip()
            return target if target.lower().startswith("http") else urljoin(response.url, target)
    return None


def discover_article_urls(
    listing_url: str,
    session: requests.Session,
    *,
    listing_page_param: str,
    safety_max_listing_pages: int,
    delay_sec: float,
    progress_log: bool,
) -> list[str]:
    """Walk listing pages until no growth or cap."""
    all_urls: set[str] = set()
    page = 1
    next_from_header: str | None = None
    declared_total: int | None = None

    while page <= safety_max_listing_pages:
        if next_from_header:
            url = next_from_header
            next_from_header = None
        elif page == 1:
            url = listing_url
        else:
            url = merge_query(listing_url, {listing_page_param: str(page)})

        try:
            resp = session.get(url, timeout=30)
        except requests.RequestException:
            break
        if resp.status_code != 200:
            break

        if declared_total is None:
            declared_total = parse_declared_listing_total(resp.text)

        batch = set()
        batch |= extract_urls_from_html(resp.text)
        batch |= extract_urls_from_next_data(resp.text)

        new = batch - all_urls
        all_urls |= batch

        if progress_log:
            decl = f" | declared={declared_total}" if declared_total is not None else ""
            print(
                f"[listing] page={page} url={url} "
                f"+{len(new)} new | total_unique={len(all_urls)}{decl}",
                flush=True,
            )

        if not new and page > 1:
            break

        nxt = parse_link_next_url(resp)
        if nxt:
            next_from_header = nxt
            page += 1
            if delay_sec:
                time.sleep(delay_sec)
            continue

        if not new and page == 1 and len(batch) == 0:
            break

        page += 1
        if delay_sec:
            time.sleep(delay_sec)

    if declared_total is not None and len(all_urls) < declared_total:
        print(
            f"[listing] Page heading claims {declared_total} items but only {len(all_urls)} "
            f"article URLs were collected over HTTP. The rest may load after Show more; "
            f"use the Playwright scraper or inspect Network for the load-more request.",
            flush=True,
        )

    return sorted(all_urls)


def _meta_content(soup: BeautifulSoup, prop_or_name: str) -> str | None:
    for sel in (
        {"property": prop_or_name},
        {"name": prop_or_name},
    ):
        tag = soup.find("meta", attrs=sel)
        if tag and tag.get("content"):
            return str(tag["content"]).strip()
    return None


def _collect_json_ld(soup: BeautifulSoup) -> list[Any]:
    blobs: list[Any] = []
    for script in soup.find_all("script", type="application/ld+json"):
        if not script.string or not script.string.strip():
            continue
        try:
            blobs.append(json.loads(script.string))
        except json.JSONDecodeError:
            continue
    return blobs


def _visible_article_text(soup: BeautifulSoup) -> str:
    for sel in ("article", "main", '[role="main"]'):
        node = soup.select_one(sel)
        if node:
            txt = node.get_text("\n", strip=True)
            if len(txt) > 200:
                return txt
    body = soup.body
    if not body:
        return soup.get_text("\n", strip=True)
    best = ""
    for div in body.find_all(["article", "main", "div"], limit=80):
        t = div.get_text("\n", strip=True)
        if len(t) > len(best):
            best = t
    return best


def article_row_from_html(url: str, html: str, *, error: str | None = None) -> dict[str, Any]:
    """Build one CSV row from article HTML (shared with the Playwright scraper)."""
    out: dict[str, Any] = {
        "url": url,
        "title": "",
        "meta_description": "",
        "h1": "",
        "article_text": "",
        "json_ld": "",
    }
    if error:
        out["error"] = error
        return out
    soup = parse_html(html)
    title_tag = soup.title.get_text(strip=True) if soup.title else ""
    out["title"] = title_tag
    out["meta_description"] = _meta_content(soup, "description") or ""
    h1 = soup.find("h1")
    out["h1"] = h1.get_text(strip=True) if h1 else ""
    og = _meta_content(soup, "og:title")
    if og and (not out["title"] or len(og) > len(out["title"])):
        out["title"] = og
    out["article_text"] = _visible_article_text(soup)
    blobs = _collect_json_ld(soup)
    out["json_ld"] = json.dumps(blobs, ensure_ascii=False) if blobs else ""
    return out


def scrape_article(url: str, session: requests.Session) -> dict[str, Any]:
    try:
        resp = session.get(url, timeout=30)
    except requests.RequestException as e:
        return article_row_from_html(url, "", error=str(e))
    if resp.status_code != 200:
        return article_row_from_html(url, "", error=f"HTTP {resp.status_code}")
    return article_row_from_html(url, resp.text)


def save_articles_csv(rows: Iterable[Mapping[str, Any]], path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df = pd.DataFrame(list(rows))
    df.to_csv(path, index=False)
    print(f"Wrote {path}", flush=True)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("Example::")[0].strip())
    p.add_argument("--listing-url", default=LISTING_DEFAULT, help="Resource listing URL")
    p.add_argument(
        "--listing-page-param",
        default="page",
        help="Query key appended for pagination (default: page)",
    )
    p.add_argument(
        "--max-listing-pages",
        type=int,
        default=200,
        help="Safety cap on listing fetches (default 200)",
    )
    p.add_argument("--max-articles", type=int, default=None, help="Cap articles scraped")
    p.add_argument("--delay", type=float, default=0.35, help="Seconds between HTTP calls")
    p.add_argument(
        "--output",
        default=os.path.join("data", "MyMyelomaTeam", "articles.csv"),
        help="Output CSV path",
    )
    p.add_argument(
        "--trust-env",
        action="store_true",
        help="Use system proxy env (HTTP_PROXY / HTTPS_PROXY). Default: ignore env.",
    )
    p.add_argument("--progress", action="store_true", help="Log listing progress")
    p.add_argument("--no-save", action="store_true", help="Print sample only; do not write CSV")
    return p


def main() -> int:
    args = build_parser().parse_args()
    sess = requests.Session()
    sess.headers.update(HEADERS)
    if not args.trust_env:
        sess.trust_env = False

    urls = discover_article_urls(
        args.listing_url,
        sess,
        listing_page_param=args.listing_page_param,
        safety_max_listing_pages=args.max_listing_pages,
        delay_sec=args.delay,
        progress_log=args.progress,
    )
    if not urls:
        print(
            "[listing] No article URLs found. Open the listing in a browser → DevTools → "
            "Network, click Show more, find the XHR/fetch that returns more rows, and "
            "mirror that URL pattern in this script (or adjust --listing-page-param).",
            file=sys.stderr,
        )
        return 2

    if args.max_articles is not None:
        urls = urls[: max(0, args.max_articles)]

    rows: list[dict[str, Any]] = []
    for i, u in enumerate(urls, start=1):
        if args.progress:
            print(f"[article] {i}/{len(urls)} {u}", flush=True)
        rows.append(scrape_article(u, sess))
        if args.delay:
            time.sleep(args.delay)

    if args.progress:
        errs = sum(1 for r in rows if r.get("error"))
        print(f"[done] articles={len(rows)} errors={errs}", flush=True)

    if not args.no_save:
        save_articles_csv(rows, args.output)
    else:
        df = pd.DataFrame(rows)
        print(df.head(3).to_string(), flush=True)
        print(f"\nRows: {len(df)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
