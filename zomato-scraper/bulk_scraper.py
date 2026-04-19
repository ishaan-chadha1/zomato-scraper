import argparse
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlencode, urljoin

import pandas as pd
import requests

from info_scraper import get_info, parse_html
from menu_scraper import get_menu
from review_scraper import get_reviews

# Same style as review_scraper: plain HTTP + parse (no browser for listing pages).
LISTING_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
}

INFO_COLUMNS = [
    'Type', 'Name', 'URL', 'Opening_Hours',
    'Street', 'Locality', 'Region', 'PostalCode', 'Country',
    'Latitude', 'Longitude', 'Phone',
    'Price_Range', 'Payment_Methods',
    'Image_URL', 'Cuisine', 'Rating', 'Rating_Count'
]

BASE_URL = "https://www.zomato.com"

# Curated / hub listing slugs (not single-venue pages).
_COLLECTION_AND_HUB_SLUGS = frozenset(
    {
        "newly-opened",
        "omakase-bars",
        "picturesque-cafes-insta-worthy",
        "fine-dining-restaurants",
        "iconic-restaurants",
        "great-breakfasts",
        "best-bars-and-pubs",
        "finest-microbreweries",
        "collections",
        "central-bangalore-restaurants",
    }
)


def is_junk_listing_anchor_name(name: str) -> bool:
    """True for promo tiles, 'N Places' collection cards, etc. (link text on listing HTML)."""
    n = (name or "").strip()
    if not n or len(n) > 120:
        return True
    low = n.lower()
    if "right-triangle" in low:
        return True
    if re.search(r"\d+\s+places\b", low):
        return True
    if "% off" in low and len(n) < 60:
        return True
    if low.startswith("promoted ") and ("%" in n or " off" in low):
        return True
    if re.match(r"^flat\s+\d+", low):
        return True
    return False


def parse_rating(text):
    if not text:
        return None
    match = re.search(r'\b([1-4]\.\d|5\.0)\b', text)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def normalize_restaurant_url(url, city):
    if not url:
        return None
    absolute = urljoin("https://www.zomato.com", url.strip())
    absolute = absolute.split("?")[0].rstrip("/")
    if absolute.endswith("/order"):
        absolute = absolute[:-6]
    if not absolute.startswith("https://www.zomato.com/"):
        return None
    if f"/{city}/" not in absolute:
        return None

    blocked = [
        "/reviews", "/photos", "/menu", "/collections", "/delivery-in-",
        "/delivery/", "/restaurants", "/dish-", "/nearby", "/book",
        "/gold", "/events", "/nightlife",
    ]
    if any(token in absolute for token in blocked):
        return None

    last_segment = (absolute.rsplit("/", 1)[-1] or "").split("?")[0]
    if len(last_segment.split("-")) < 2:
        return None
    slug_l = last_segment.lower()
    if slug_l in _COLLECTION_AND_HUB_SLUGS:
        return None
    if slug_l.startswith("dine-out-in-"):
        return None
    return absolute


def discover_restaurants_from_seeds(
    city,
    seeds,
    max_restaurants=50,
    max_scrolls=35,
    *,
    listing_pages_without_growth_limit: int = 1,
    request_timeout_sec: float = 40.0,
    listing_sleep_sec: float = 0.35,
) -> list[dict]:
    """
    Paginate each seed URL (?sort=rating&page=N) and merge unique venue URLs.

    `seeds` are full https://www.zomato.com/{city}/... listing pages (area, dine-out, etc.).
    """
    session = requests.Session()
    session.headers.update(LISTING_HTTP_HEADERS)
    discovered: dict[str, dict] = {}

    for seed in seeds:
        seed = seed.strip()
        if not seed:
            continue
        stale_pages = 0
        for page_num in range(1, max_scrolls + 1):
            try:
                resp = session.get(
                    seed,
                    params={"sort": "rating", "page": page_num},
                    timeout=request_timeout_sec,
                )
            except requests.RequestException:
                break
            if resp.status_code != 200:
                break

            listing_page_url = resp.url
            soup = parse_html(resp.text)
            count_before = len(discovered)
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                name = re.sub(
                    r"\s+",
                    " ",
                    (a.get_text(separator=" ", strip=True) or "").strip(),
                )
                if not href or not name:
                    continue
                if is_junk_listing_anchor_name(name):
                    continue
                card = a.find_parent(["article", "section", "div"])
                card_text = ""
                if card:
                    card_text = re.sub(r"\s+", " ", card.get_text(" ", strip=True))
                normalized_url = normalize_restaurant_url(href, city)
                if not normalized_url:
                    continue
                rating = parse_rating(card_text)
                existing = discovered.get(normalized_url)
                if existing is None or (rating or 0) > (existing.get("rating") or 0):
                    discovered[normalized_url] = {
                        "url": normalized_url,
                        "name": name,
                        "rating": rating,
                        "source_page": listing_page_url,
                    }
                if len(discovered) >= max_restaurants:
                    break

            if len(discovered) >= max_restaurants:
                break
            if page_num > 1 and len(discovered) == count_before:
                stale_pages += 1
                if stale_pages >= listing_pages_without_growth_limit:
                    break
            else:
                stale_pages = 0
            if listing_sleep_sec:
                time.sleep(listing_sleep_sec)

        if len(discovered) >= max_restaurants:
            break

    if not discovered:
        raise RuntimeError(
            "No restaurants found on listing pages (HTTP). "
            "Check network, or open the city URL in a browser — Zomato may require a different region."
        )

    restaurants = list(discovered.values())
    restaurants.sort(key=lambda x: (x.get("rating") is not None, x.get("rating") or 0), reverse=True)
    for idx, row in enumerate(restaurants, start=1):
        row["rank"] = idx
    return restaurants[:max_restaurants]


def discover_restaurants(
    city,
    max_restaurants=50,
    max_scrolls=35,
    executable_path=None,
    *,
    listing_pages_without_growth_limit: int = 1,
):
    """
    Discover venues like review_scraper: requests.get + BeautifulSoup on listing HTML.
    No Playwright (avoids timeouts / bot wall on listing navigation).

    max_scrolls: reused as "max listing pages to fetch per source URL" (pagination).
    executable_path: ignored; kept for call compatibility.
    listing_pages_without_growth_limit: stop this listing source after this many
        consecutive listing pages that add no new venue URLs (1 matches legacy behavior).
    """
    del executable_path
    seeds = [
        f"https://www.zomato.com/{city}/delivery",
        f"https://www.zomato.com/{city}/restaurants",
        f"https://www.zomato.com/{city}/dine-out",
    ]
    return discover_restaurants_from_seeds(
        city,
        seeds,
        max_restaurants=max_restaurants,
        max_scrolls=max_scrolls,
        listing_pages_without_growth_limit=listing_pages_without_growth_limit,
    )


def collection_subpage_seeds_from_collections_index_html(html: str, city: str) -> list[str]:
    """
    From /{city}/collections HTML, find links to curated listing hubs (great-breakfasts, etc.)
    and regional dine-out hubs. These are paginated like other listing pages.
    """
    from urllib.parse import urlparse

    soup = parse_html(html)
    found: set[str] = set()
    city_l = city.lower()
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        full = urljoin(BASE_URL, href).split("?")[0].rstrip("/")
        path = urlparse(full).path.strip("/")
        parts = path.split("/")
        if len(parts) != 2 or parts[0].lower() != city_l:
            continue
        slug = parts[1].lower()
        if slug == "collections":
            continue
        if slug in _COLLECTION_AND_HUB_SLUGS or slug.startswith("dine-out-in-"):
            found.add(f"https://www.zomato.com/{city_l}/{slug}")
    return sorted(found)


def format_rating(value):
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.1f}"


def enrich_discovered_restaurants(restaurants):
    enriched = []
    for row in restaurants:
        merged = dict(row)
        try:
            info = get_info(row["url"])
            if not info or info[0] != "Restaurant" or not info[2]:
                continue
            merged["name"] = info[1] or row.get("name")
            merged["url"] = urljoin(BASE_URL, info[2]) if info[2] else row.get("url")
            merged["rating"] = pd.to_numeric(info[16], errors="coerce")
        except Exception:
            continue
        enriched.append(merged)

    enriched.sort(key=lambda x: (x.get("rating") is not None and not pd.isna(x.get("rating")), x.get("rating") or 0), reverse=True)
    for idx, row in enumerate(enriched, start=1):
        row["rank"] = idx
    return enriched


def print_top_list(restaurants):
    print("\nTop discovered restaurants (highest rating first):")
    print("-" * 110)
    for row in restaurants:
        print(f"{row['rank']:>2}. {row['name'][:42]:<42} | rating={format_rating(row.get('rating')):<4} | {row['url']}")
    print("-" * 110)


def ask_three_ranks(max_rank):
    """Prompt until the user enters exactly three distinct valid ranks."""
    while True:
        raw = input(
            "Enter exactly 3 ranks from the list above (e.g. 2, 15, 40): "
        ).strip()
        normalized = raw.replace(",", " ")
        parts = [p for p in normalized.split() if p]
        try:
            ranks = [int(p) for p in parts]
        except ValueError:
            print("Use only whole numbers separated by commas or spaces.")
            continue
        ranks = sorted(set(ranks))
        if len(ranks) != 3:
            print(f"You must pick exactly 3 different ranks (you entered {len(ranks)}).")
            continue
        invalid = [r for r in ranks if r < 1 or r > max_rank]
        if invalid:
            print(f"Each rank must be between 1 and {max_rank}. Invalid: {invalid}")
            continue
        return ranks


def ask_yes_no(prompt, default="y"):
    valid_defaults = {"y", "n"}
    default = default.lower()
    if default not in valid_defaults:
        default = "y"
    suffix = "[Y/n]" if default == "y" else "[y/N]"
    while True:
        raw = input(f"{prompt} {suffix}: ").strip().lower()
        if not raw:
            return default == "y"
        if raw in {"y", "yes"}:
            return True
        if raw in {"n", "no"}:
            return False
        print("Please enter y or n.")


def run_with_retries(fn, attempts=3, base_sleep=1.5):
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt < attempts:
                sleep_for = base_sleep * attempt
                time.sleep(sleep_for)
        except Exception as exc:
            # Keep original behavior for non-network errors.
            raise exc
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("retry loop failed without exception")


def scrape_one_restaurant(url, log_label=None):
    started = time.time()

    def log(msg):
        prefix = f"[interactive] {log_label}" if log_label else "[interactive]"
        print(f"{prefix} | {msg}", flush=True)

    result = {
        "url": url,
        "status": "ok",
        "error": "",
        "menu_rows": 0,
        "review_rows": 0,
        "elapsed_sec": 0.0,
        "info": None,
        "menu_df": None,
        "review_df": None,
    }
    log(
        "starting: info → menu → all reviews. "
        "Do not assume a hang; menu can take ~1–2 min, reviews often 5–30+ min per place."
    )
    try:
        log("info: fetching…")
        info = run_with_retries(lambda: get_info(url), attempts=3, base_sleep=1.5)
        result["info"] = info
        log("info: done")
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = f"info_error: {exc}"
        log(f"info: failed ({exc})")

    try:
        log(
            "menu: Playwright opening /order (often 30–120s; on failure we fall back to static HTML)…"
        )
        menu_df = run_with_retries(
            lambda: get_menu(url=url, save=False, use_playwright=True),
            attempts=2,
            base_sleep=2.0
        )
        result["menu_rows"] = len(menu_df)
        result["menu_df"] = menu_df
        log(f"menu: done ({result['menu_rows']} rows)")
    except Exception as exc:
        if result["status"] == "ok":
            result["status"] = "partial_failed"
            result["error"] = f"menu_error: {exc}"
        else:
            result["error"] = f"{result['error']} | menu_error: {exc}"
        log(f"menu: failed ({exc})")

    try:
        log(
            "reviews: paging /reviews (slowest step). "
            "Watch [reviews] filter=… lines until this rank finishes."
        )
        review_df = run_with_retries(
            lambda: get_reviews(
                url,
                max_reviews=None,
                sort="popular",
                save=True,
                save_empty=False,
                progress_log=bool(log_label),
            ),
            attempts=2,
            base_sleep=3.0,
        )
        result["review_df"] = review_df
        result["review_rows"] = len(review_df)
        log(f"reviews: done ({result['review_rows']} rows)")
    except Exception as exc:
        if result["status"] == "ok":
            result["status"] = "partial_failed"
            result["error"] = f"review_error: {exc}"
        else:
            result["error"] = f"{result['error']} | review_error: {exc}"
        log(f"reviews: failed ({exc})")

    if result["status"] == "partial_failed" and (
        result["menu_rows"] > 0 or result["review_rows"] > 0
    ):
        result["status"] = "ok"
        result["error"] = ""

    result["elapsed_sec"] = round(time.time() - started, 2)
    log(f"finished this place in {result['elapsed_sec']}s (status={result['status']})")
    return result


def scrape_selected(restaurants, selected_ranks, concurrency):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    selected_rows = [r for r in restaurants if r["rank"] in selected_ranks]
    workers = min(concurrency, max(1, len(selected_rows)))
    print(
        "[interactive] Scraping selected restaurants. Order per place: info (fast) → "
        "menu (browser) → all reviews (many pages).",
        flush=True,
    )
    print(
        f"[interactive] {len(selected_rows)} job(s); up to {workers} in parallel — "
        "each block below is one place; wait for 'finished this place'.",
        flush=True,
    )
    print(f"[interactive] scraping {len(selected_rows)} selected restaurants…\n", flush=True)

    summary_rows = []
    info_rows = []
    menu_frames = []
    review_frames = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_map = {}
        for row in selected_rows:
            label = f"rank={row['rank']} {row['name'][:50]}"
            fut = pool.submit(scrape_one_restaurant, row["url"], label)
            future_map[fut] = row
        completed = 0
        total = len(future_map)
        for future in as_completed(future_map):
            completed += 1
            meta = future_map[future]
            try:
                outcome = future.result()
            except Exception as exc:
                outcome = {
                    "url": meta["url"],
                    "status": "failed",
                    "error": f"worker_error: {exc}",
                    "menu_rows": 0,
                    "review_rows": 0,
                    "elapsed_sec": 0.0,
                    "info": None,
                    "review_df": None,
                }

            summary_rows.append({
                "rank": meta["rank"],
                "name": meta["name"],
                "discovered_rating": meta.get("rating"),
                "url": outcome["url"],
                "status": outcome["status"],
                "error": outcome["error"],
                "menu_rows": outcome["menu_rows"],
                "review_rows": outcome.get("review_rows", 0),
                "elapsed_sec": outcome["elapsed_sec"],
            })
            if outcome.get("info"):
                info_rows.append(outcome["info"])
            if outcome.get("menu_df") is not None and not outcome["menu_df"].empty:
                df = outcome["menu_df"].copy()
                df.insert(0, "Restaurant_Rank", meta["rank"])
                df.insert(1, "Restaurant_Name", meta["name"])
                df.insert(2, "Restaurant_URL", meta["url"])
                df.insert(3, "Restaurant_Rating", meta.get("rating"))
                menu_frames.append(df)

            rev_df = outcome.get("review_df")
            if rev_df is not None and not rev_df.empty:
                df = rev_df.copy()
                df.insert(0, "Restaurant_Rank", meta["rank"])
                df.insert(1, "Restaurant_Name", meta["name"])
                df.insert(2, "Restaurant_URL", meta["url"])
                df.insert(3, "Restaurant_Rating", meta.get("rating"))
                review_frames.append(df)

            print(
                f"[interactive] {completed}/{total} rank={meta['rank']} "
                f"menu={outcome['menu_rows']} "
                f"reviews={outcome.get('review_rows', 0)} "
                f"status={outcome['status']} | {meta['name']}"
            )

    if info_rows:
        info_df = pd.DataFrame(info_rows, columns=INFO_COLUMNS)
        info_df["Rating"] = pd.to_numeric(info_df["Rating"], errors="coerce")
        info_df = info_df.sort_values(by="Rating", ascending=False, na_position="last")
        if os.path.exists("Restaurants.csv"):
            existing = pd.read_csv("Restaurants.csv")
            info_df = pd.concat([existing, info_df], ignore_index=True)
            info_df = info_df.drop_duplicates(subset=["URL"], keep="last")
            info_df["Rating"] = pd.to_numeric(info_df["Rating"], errors="coerce")
            info_df = info_df.sort_values(by="Rating", ascending=False, na_position="last")
        info_df.to_csv("Restaurants.csv", index=False)
        print(f"[interactive] wrote Restaurants.csv ({len(info_df)} rows)")

    selected_df = pd.DataFrame(selected_rows)
    selected_df.to_csv(f"Selected_Restaurants_{ts}.csv", index=False)

    if menu_frames:
        combined = pd.concat(menu_frames, ignore_index=True)
        combined_path = f"Menus_Combined_{ts}.csv"
        combined.to_csv(combined_path, index=False)
        combined.to_csv("Menus_Combined.csv", index=False)
        print(f"[interactive] wrote {combined_path} and Menus_Combined.csv ({len(combined)} rows)")

    if review_frames:
        combined_rev = pd.concat(review_frames, ignore_index=True)
        rev_path = f"Reviews_Combined_{ts}.csv"
        combined_rev.to_csv(rev_path, index=False)
        combined_rev.to_csv("Reviews_Combined.csv", index=False)
        print(f"[interactive] wrote {rev_path} and Reviews_Combined.csv ({len(combined_rev)} rows)")

    summary_df = pd.DataFrame(summary_rows).sort_values(by="rank")
    summary_df.to_csv(f"Interactive_Run_Summary_{ts}.csv", index=False)
    print(f"[interactive] wrote Interactive_Run_Summary_{ts}.csv")


def run_interactive(city, top_count, concurrency, max_scrolls):
    print(f"[interactive] discovering top {top_count} places in {city}...")
    restaurants = discover_restaurants(
        city=city,
        max_restaurants=top_count,
        max_scrolls=max_scrolls,
    )
    if not restaurants:
        print("[interactive] no restaurants discovered; exiting")
        return

    restaurants = enrich_discovered_restaurants(restaurants)
    top_df = pd.DataFrame(restaurants).sort_values(by="rank")
    top_df.to_csv("Top_50_Restaurants.csv", index=False)
    print("[interactive] saved Top_50_Restaurants.csv")
    print_top_list(restaurants)

    while True:
        selected_ranks = ask_three_ranks(len(restaurants))
        print(f"[interactive] selected ranks: {selected_ranks}")
        if not ask_yes_no("Proceed with scraping these 3 (info, menu, all reviews)?", default="y"):
            if ask_yes_no("Choose three different ranks instead?", default="y"):
                continue
            print("[interactive] exiting without scraping.")
            break

        scrape_selected(restaurants, selected_ranks, concurrency)
        if not ask_yes_no("Scrape another set of 3 from the same top list?", default="n"):
            print("[interactive] done.")
            break
        print("[interactive] reusing the same discovered top list.")


def parse_args():
    parser = argparse.ArgumentParser(description="Interactive top-down Zomato scraper.")
    parser.add_argument("--city", default="bangalore", help="City slug in Zomato URL (example: bangalore).")
    parser.add_argument("--top-count", type=int, default=50, help="Number of top restaurants to discover first.")
    parser.add_argument("--concurrency", type=int, default=3, help="Parallel workers for scraping selected range.")
    parser.add_argument("--max-scrolls", type=int, default=35, help="Discovery page scroll iterations.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_interactive(
        city=args.city,
        top_count=args.top_count,
        concurrency=args.concurrency,
        max_scrolls=args.max_scrolls,
    )
