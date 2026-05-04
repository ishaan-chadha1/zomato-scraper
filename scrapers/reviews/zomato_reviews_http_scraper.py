#!/usr/bin/env python3
"""Fetch Zomato restaurant reviews via HTTP (requests + BeautifulSoup).

Tries, in order:

1. **JSON-LD** in ``<script type="application/ld+json">`` (Restaurant → reviews).
2. **``window.__PRELOADED_STATE__``** embedded in HTML (``entities.REVIEWS``).

Pagination walks ``…/reviews?page=N&sort=…&filter=…``. When ``__PRELOADED_STATE__`` is
present, **``numberOfPages``** is read (page 1, else a one-time ``page=1007`` probe) so
we scrape through the **last listing page** without relying only on duplicate-page
detection. ``safety_max_pages`` still caps total pages per filter; empty/duplicate pages
stop the run early if state is missing.

Example::

    python3 zomato_reviews_http.py \\
      --url "https://www.zomato.com/bangalore/meghana-foods-marathahalli-bangalore" \\
      --sort new --filters reviews-dining --progress
"""

from __future__ import annotations

import argparse
import codecs
import json
import os
import re
import sys
import time
from urllib.parse import urlencode

import pandas as pd
import requests
from bs4 import BeautifulSoup, FeatureNotFound

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
}

SORT_QUERY = {"popular": "rd", "new": "dd"}
DEFAULT_FILTERS = ("reviews-dining", "reviews-delivery")
# Oversized page= is clamped by Zomato; state then exposes the real last page index.
PROBE_PAGE_FOR_PAGE_COUNT = 1007


def parse_html(text: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(text, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(text, "html.parser")


def _author_name(author) -> str | None:
    if author is None:
        return None
    if isinstance(author, dict):
        return author.get("name")
    if isinstance(author, str):
        return author
    return str(author)


def _extract_jsonld_review_rows(html_text: BeautifulSoup) -> list[tuple[str, tuple]]:
    """Return [(dedupe_key, (author, url, desc, rating)), ...]."""
    rows: list[tuple[str, tuple]] = []
    for script in html_text.find_all("script", type="application/ld+json"):
        if not script.string:
            continue
        try:
            parsed = json.loads(script.string)
        except json.JSONDecodeError:
            continue

        reviews: list | None = None
        if isinstance(parsed, dict):
            if isinstance(parsed.get("reviews"), list):
                reviews = parsed["reviews"]
            elif isinstance(parsed.get("review"), list):
                reviews = parsed["review"]
        elif isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict) and item.get("@type") == "Restaurant":
                    if isinstance(item.get("reviews"), list):
                        reviews = item["reviews"]
                        break
                    if isinstance(item.get("review"), list):
                        reviews = item["review"]
                        break

        if not reviews:
            continue

        for review in reviews:
            if not isinstance(review, dict):
                continue
            rating = review.get("reviewRating") or {}
            rv = rating.get("ratingValue") if isinstance(rating, dict) else None
            author = _author_name(review.get("author"))
            url = review.get("url") or review.get("@id") or ""
            desc = review.get("description") or ""
            key = str(url or f"ld:{author}|{desc[:160]}")
            rows.append((key, (author, url, desc, rv)))
        if rows:
            break
    return rows


def _slice_json_parse_string_literal(page_text: str) -> str | None:
    """Return raw JS-string contents of JSON.parse(\"...\") for __PRELOADED_STATE__."""
    marker = re.search(r"window\.__PRELOADED_STATE__\s*=\s*JSON\.parse\(\s*\"", page_text)
    if not marker:
        return None
    start = marker.end()
    i = start
    escaped = False
    while i < len(page_text):
        c = page_text[i]
        if escaped:
            escaped = False
            i += 1
            continue
        if c == "\\":
            escaped = True
            i += 1
            continue
        if c == '"':
            return page_text[start:i]
        i += 1
    return None


def parse_preloaded_state(page_text: str) -> dict | None:
    """Parse ``window.__PRELOADED_STATE__`` JSON object, or None."""
    raw = _slice_json_parse_string_literal(page_text)
    if not raw:
        return None
    try:
        decoded = codecs.decode(raw, "unicode_escape")
        return json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def _extract_number_of_pages(state: dict | None) -> int | None:
    """Zomato embeds total review listing pages under pages.restaurant.<resId>.sections."""
    if not isinstance(state, dict):
        return None

    pages_restaurant = state.get("pages", {}).get("restaurant")
    if isinstance(pages_restaurant, dict):
        for _res_id, node in pages_restaurant.items():
            if not isinstance(node, dict):
                continue
            sections = node.get("sections")
            if not isinstance(sections, dict):
                continue
            for sec_key in (
                "SECTION_REVIEWS",
                "SECTION_REVIEWS_DINING",
                "SECTION_REVIEWS_DELIVERY",
            ):
                sec = sections.get(sec_key)
                if isinstance(sec, dict):
                    n = sec.get("numberOfPages")
                    if isinstance(n, int) and n >= 1:
                        return n
                    if isinstance(n, float) and n >= 1:
                        return int(n)
            for key, sec in sections.items():
                if not isinstance(sec, dict):
                    continue
                if str(key).upper().startswith("SECTION_") and "REVIEW" in str(key).upper():
                    n = sec.get("numberOfPages")
                    if isinstance(n, int) and n >= 1:
                        return n
                    if isinstance(n, float) and n >= 1:
                        return int(n)

    current = state.get("pages", {}).get("current")
    if isinstance(current, dict):
        sections = current.get("sections")
        if isinstance(sections, dict):
            for sec_key in (
                "SECTION_REVIEWS",
                "SECTION_REVIEWS_DINING",
                "SECTION_REVIEWS_DELIVERY",
            ):
                sec = sections.get(sec_key)
                if isinstance(sec, dict):
                    n = sec.get("numberOfPages")
                    if isinstance(n, int) and n >= 1:
                        return n
                    if isinstance(n, float) and n >= 1:
                        return int(n)
            for key, sec in sections.items():
                if not isinstance(sec, dict):
                    continue
                if str(key).upper().startswith("SECTION_") and "REVIEW" in str(key).upper():
                    n = sec.get("numberOfPages")
                    if isinstance(n, int) and n >= 1:
                        return n
                    if isinstance(n, float) and n >= 1:
                        return int(n)

    return None


def _extract_preloaded_review_rows(page_text: str) -> list[tuple[str, tuple]]:
    state = parse_preloaded_state(page_text)
    if not state:
        return []

    reviews_obj = state.get("entities", {}).get("REVIEWS", {})
    if not isinstance(reviews_obj, dict):
        return []

    rows: list[tuple[str, tuple]] = []
    for rev_id, review in reviews_obj.items():
        if not isinstance(review, dict):
            continue
        key = str(review.get("reviewUrl") or review.get("reviewId") or rev_id)
        rows.append(
            (
                key,
                (
                    review.get("userName"),
                    review.get("reviewUrl"),
                    review.get("reviewText") or review.get("reviewTextSm") or "",
                    review.get("ratingV2"),
                ),
            )
        )
    return rows


def extract_review_rows_from_response(page_text: str) -> list[tuple[str, tuple]]:
    """Return list of (dedupe_key, (Author, Review URL, Description, Rating))."""
    html_text = parse_html(page_text)
    rows = _extract_jsonld_review_rows(html_text)
    if not rows:
        rows = _extract_preloaded_review_rows(page_text)
    return rows


def _normalize_restaurant_base_url(url: str) -> str:
    if not url:
        return ""
    u = url.strip().split("#")[0].split("?")[0].rstrip("/")
    if u.endswith("/reviews"):
        u = u[: -len("/reviews")]
    return u


def _reviews_listing_url(base: str, page: int, sort_code: str, review_filter: str) -> str:
    q = urlencode(
        {"page": page, "sort": sort_code, "filter": review_filter},
        safe="",
    )
    return f"{base}/reviews?{q}"


def save_df(file_name: str, df: pd.DataFrame) -> None:
    os.makedirs("Reviews", exist_ok=True)
    safe = re.sub(r'[<>:"/\\|?*]', "", file_name).strip() or "Unknown Restaurant"
    path = os.path.join("Reviews", f"{safe}.csv")
    df.to_csv(path, index=False)
    print(f"Wrote {path}", flush=True)


def sanitize_file_name(name: str, fallback: str = "Unknown Restaurant") -> str:
    if not name:
        name = fallback
    name = re.sub(r'[<>:"/\\|?*]', "", name).strip()
    return name or fallback


def restaurant_name_from_url(url: str) -> str:
    slug = _normalize_restaurant_base_url(url).rstrip("/").rsplit("/", maxsplit=1)[-1]
    return slug.replace("-", " ").title().strip() or "Unknown Restaurant"


def _title_to_restaurant_name(title: str, fallback: str) -> str:
    rn = (title or "").strip()
    if "User Reviews" in rn:
        restaurant_name = rn.split("User Reviews", 1)[0].strip(" -|")
    elif "|" in rn:
        restaurant_name = rn.split("|", 1)[0].strip()
    else:
        restaurant_name = rn.strip()

    if restaurant_name.lower().startswith("reviews of "):
        restaurant_name = restaurant_name[11:].strip()
    return sanitize_file_name(restaurant_name, fallback=fallback)


def get_reviews(
    url: str,
    max_reviews: int | None = None,
    sort: str = "popular",
    save: bool = True,
    save_empty: bool = False,
    filters: tuple[str, ...] | None = None,
    safety_max_pages: int = 5000,
    delay_sec: float = 0.25,
    session: requests.Session | None = None,
    progress_log: bool = False,
    detect_page_count: bool = True,
) -> pd.DataFrame:
    """
    Fetch reviews for a restaurant.

    When ``detect_page_count`` is True, reads ``numberOfPages`` from preloaded state
    (page 1, then optional ``page=1007`` probe) and scrapes through that last page
    (capped by ``safety_max_pages``).

    Stops when: empty extraction, duplicate page keys vs previous page,
    ``max_reviews`` reached, ``page`` past detected last page, or ``safety_max_pages``.
    """
    sort_key = sort if sort in SORT_QUERY else "popular"
    sort_code = SORT_QUERY[sort_key]

    base = _normalize_restaurant_base_url(url)
    if not base:
        return pd.DataFrame(columns=["Author", "Review URL", "Description", "Rating"])

    fallback_name = restaurant_name_from_url(url)
    sess = session or requests.Session()
    sess.headers.update(HEADERS)

    collected: list[tuple] = []
    seen_keys: set[str] = set()
    restaurant_name = fallback_name

    filter_list = tuple(filters) if filters is not None else DEFAULT_FILTERS

    for review_filter in filter_list:
        prev_page_keys: tuple[str, ...] | None = None
        page_ceiling: int | None = None
        page = 1

        while page <= safety_max_pages:
            if page_ceiling is not None and page > page_ceiling:
                break
            if max_reviews is not None and len(collected) >= max_reviews:
                break

            link = _reviews_listing_url(base, page, sort_code, review_filter)
            try:
                resp = sess.get(link, timeout=25)
            except requests.RequestException:
                break

            if resp.status_code != 200:
                break

            html_text = parse_html(resp.text)
            if html_text.title:
                title_txt = html_text.title.get_text(strip=True) or ""
                if title_txt:
                    restaurant_name = _title_to_restaurant_name(title_txt, fallback_name)

            if detect_page_count and page == 1 and page_ceiling is None:
                n_pages = _extract_number_of_pages(parse_preloaded_state(resp.text))
                if not n_pages:
                    probe_link = _reviews_listing_url(
                        base, PROBE_PAGE_FOR_PAGE_COUNT, sort_code, review_filter
                    )
                    try:
                        probe_resp = sess.get(probe_link, timeout=25)
                        if probe_resp.status_code == 200:
                            n_pages = _extract_number_of_pages(
                                parse_preloaded_state(probe_resp.text)
                            )
                    except requests.RequestException:
                        pass
                if n_pages and n_pages >= 1:
                    page_ceiling = min(int(n_pages), safety_max_pages)
                    if progress_log:
                        print(
                            f"[reviews] filter={review_filter} "
                            f"page_count={n_pages} -> scraping 1..{page_ceiling}",
                            flush=True,
                        )

            rows = extract_review_rows_from_response(resp.text)
            if not rows:
                break

            page_keys = tuple(sorted(k for k, _ in rows))
            if prev_page_keys is not None and page_keys == prev_page_keys:
                break
            prev_page_keys = page_keys

            new_on_page = 0
            for key, row in rows:
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                collected.append(row)
                new_on_page += 1
                if max_reviews is not None and len(collected) >= max_reviews:
                    break

            if progress_log:
                cap = f"/{page_ceiling}" if page_ceiling is not None else ""
                print(
                    f"[reviews] filter={review_filter} page={page}{cap} "
                    f"+{new_on_page} new | total={len(collected)}",
                    flush=True,
                )

            page += 1
            if delay_sec:
                time.sleep(delay_sec)

        if max_reviews is not None and len(collected) >= max_reviews:
            break

    columns = ["Author", "Review URL", "Description", "Rating"]
    review_df = pd.DataFrame(collected, columns=columns)
    restaurant_name = sanitize_file_name(restaurant_name, fallback=fallback_name)

    if save and (save_empty or not review_df.empty):
        save_df(restaurant_name, review_df)
    elif save and review_df.empty:
        print(f"[reviews] No reviews found for: {restaurant_name}", flush=True)

    return review_df


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("Example::")[0].strip())
    p.add_argument("--url", required=True, help="Restaurant URL (with or without /reviews)")
    p.add_argument("--max-reviews", type=int, default=None)
    p.add_argument("--sort", choices=("popular", "new"), default="new")
    p.add_argument(
        "--filters",
        nargs="*",
        default=list(DEFAULT_FILTERS),
        help="Zomato filter query values (default: reviews-dining reviews-delivery)",
    )
    p.add_argument(
        "--max-pages",
        type=int,
        default=5000,
        dest="safety_max_pages",
        help="Hard cap on pages per filter (default 5000). Detected page count is min(detected, this).",
    )
    p.add_argument(
        "--no-detect-pages",
        action="store_true",
        help="Do not read numberOfPages from preloaded state; stop on empty/duplicate only (capped by --max-pages).",
    )
    p.add_argument("--delay", type=float, default=0.25, help="Seconds between page requests")
    p.add_argument("--no-save", action="store_true", help="Do not write CSV under Reviews/")
    p.add_argument("--progress", action="store_true", help="Log each page to stdout")
    return p


def main() -> int:
    args = build_parser().parse_args()
    flt = tuple(args.filters) if args.filters else DEFAULT_FILTERS
    try:
        df = get_reviews(
            args.url,
            max_reviews=args.max_reviews,
            sort=args.sort,
            save=not args.no_save,
            filters=flt,
            safety_max_pages=args.safety_max_pages,
            delay_sec=args.delay,
            progress_log=args.progress,
            detect_page_count=not args.no_detect_pages,
        )
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 1
    print(df.head(10).to_string(), flush=True)
    print(f"\nRows: {len(df)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
