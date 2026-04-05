import os
import json
import re
import time
from urllib.parse import urlencode, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup, FeatureNotFound

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
}

SORT_QUERY = {"popular": "rd", "new": "dd"}
DEFAULT_FILTERS = ("reviews-dining", "reviews-delivery")


def parse_html(text):
    try:
        return BeautifulSoup(text, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(text, "html.parser")


def _author_name(author):
    if author is None:
        return None
    if isinstance(author, dict):
        return author.get("name")
    return author


def _extract_jsonld_review_rows(html_text):
    rows = []
    scripts = html_text.find_all("script", type="application/ld+json")
    for script in scripts:
        if not script.string:
            continue
        try:
            parsed = json.loads(script.string)
        except json.JSONDecodeError:
            continue

        reviews = []
        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict) and item.get("@type") == "Restaurant":
                    if isinstance(item.get("reviews"), list):
                        reviews = item["reviews"]
                        break
                    if isinstance(item.get("review"), list):
                        reviews = item["review"]
                        break
        elif isinstance(parsed, dict):
            if isinstance(parsed.get("reviews"), list):
                reviews = parsed["reviews"]
            elif isinstance(parsed.get("review"), list):
                reviews = parsed["review"]

        if not reviews:
            continue

        for review in reviews:
            if not isinstance(review, dict):
                continue
            rating = review.get("reviewRating") or {}
            if isinstance(rating, dict):
                rv = rating.get("ratingValue")
            else:
                rv = None
            author = _author_name(review.get("author"))
            url = review.get("url") or review.get("@id") or ""
            desc = review.get("description") or ""
            key = url or f"ld:{author}|{desc[:160]}"
            rows.append(
                (
                    key,
                    (author, url, desc, rv),
                )
            )
        if rows:
            break
    return rows


def _extract_preloaded_review_rows(page_text):
    match = re.search(
        r'window\.__PRELOADED_STATE__\s*=\s*JSON\.parse\("(.*)"\);',
        page_text,
        re.S,
    )
    if not match:
        return []

    try:
        decoded = bytes(match.group(1), "utf-8").decode("unicode_escape")
        state = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return []

    reviews_obj = state.get("entities", {}).get("REVIEWS", {})
    if not isinstance(reviews_obj, dict):
        return []

    rows = []
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


def extract_review_rows_from_response(page_text):
    """Return list of (dedupe_key, (Author, Review URL, Description, Rating))."""
    html_text = parse_html(page_text)
    rows = _extract_jsonld_review_rows(html_text)
    if not rows:
        rows = _extract_preloaded_review_rows(page_text)
    return rows


def _normalize_restaurant_base_url(url):
    if not url:
        return ""
    u = url.strip().split("?")[0].rstrip("/")
    if u.endswith("/reviews"):
        u = u[: -len("/reviews")]
    return u


def _reviews_listing_url(base, page, sort_code, review_filter):
    q = urlencode(
        {"page": page, "sort": sort_code, "filter": review_filter},
        safe="",
    )
    return f"{base}/reviews?{q}"


def save_df(file_name, df):
    if not os.path.exists("Reviews"):
        os.makedirs("Reviews")
    df.to_csv(f"Reviews/{file_name}.csv", index=False)


def sanitize_file_name(name, fallback="Unknown Restaurant"):
    if not name:
        name = fallback
    name = re.sub(r'[<>:"/\\|?*]', "", name).strip()
    return name or fallback


def restaurant_name_from_url(url):
    path = urlparse(url).path.strip("/")
    slug = path.split("/")[-1] if path else ""
    slug = slug.replace("-1", "")
    return slug.replace("-", " ").title().strip() or "Unknown Restaurant"


def _title_to_restaurant_name(title, fallback):
    rn = title or ""
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
    url,
    max_reviews=None,
    sort="popular",
    save=True,
    save_empty=False,
    filters=DEFAULT_FILTERS,
    safety_max_pages=5000,
    delay_sec=0.25,
    session=None,
    progress_log=False,
):
    """
    Fetch reviews for a restaurant. Pagination continues until a page is empty,
    a duplicate page is seen (same review keys as the previous page for that filter),
    or safety_max_pages / max_reviews is hit.

    Parameters
    ----------
    max_reviews : int or None
        If set, stop once this many unique reviews are collected (across all filters).
        None means no cap (still bounded by safety_max_pages per filter).
    filters : iterable of str
        Zomato `filter` query values, e.g. reviews-dining and reviews-delivery.
        Pass a single-element tuple to restrict.
    progress_log : bool
        If True, print each listing page to stderr/stdout so long runs show activity.
    """
    sort_key = sort if sort in SORT_QUERY else "popular"
    sort_code = SORT_QUERY[sort_key]

    base = _normalize_restaurant_base_url(url)
    if not base:
        return pd.DataFrame(columns=["Author", "Review URL", "Description", "Rating"])

    fallback_name = restaurant_name_from_url(url)
    sess = session or requests.Session()
    sess.headers.update(headers)

    collected = []
    seen_keys = set()
    restaurant_name = fallback_name

    filter_list = tuple(filters) if filters is not None else DEFAULT_FILTERS

    for review_filter in filter_list:
        prev_page_keys = None
        page = 1
        while page <= safety_max_pages:
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
            if html_text.title and html_text.title.text:
                restaurant_name = _title_to_restaurant_name(
                    html_text.title.text, fallback_name
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
                print(
                    f"[reviews] filter={review_filter} page={page} "
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

    if save and (save_empty or not review_df.empty):
        save_df(restaurant_name, review_df)
    elif save and review_df.empty:
        print(f"[reviews] No reviews found for: {restaurant_name}")

    return review_df


if __name__ == "__main__":
    get_reviews(
        "https://www.zomato.com/bangalore/meghana-foods-marathahalli-bangalore",
        max_reviews=30,
        sort="new",
        filters=("reviews-dining",),
        safety_max_pages=5,
    )
