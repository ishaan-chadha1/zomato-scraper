import argparse
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urljoin

import pandas as pd
import requests

from info_scraper import get_info
from menu_scraper import get_menu

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None


DEFAULT_PLAYWRIGHT_EXECUTABLE = (
    "/Users/Ishaan/Library/Caches/ms-playwright/chromium-1208/chrome-mac-arm64/"
    "Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing"
)

INFO_COLUMNS = [
    'Type', 'Name', 'URL', 'Opening_Hours',
    'Street', 'Locality', 'Region', 'PostalCode', 'Country',
    'Latitude', 'Longitude', 'Phone',
    'Price_Range', 'Payment_Methods',
    'Image_URL', 'Cuisine', 'Rating', 'Rating_Count'
]

BASE_URL = "https://www.zomato.com"


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

    last_segment = absolute.rsplit("/", 1)[-1]
    if len(last_segment.split("-")) < 2:
        return None
    return absolute


def discover_restaurants(city, max_restaurants=50, max_scrolls=35, executable_path=None):
    if sync_playwright is None:
        raise RuntimeError("Playwright is not installed. Install it via pip.")

    exe = executable_path or DEFAULT_PLAYWRIGHT_EXECUTABLE
    if not os.path.exists(exe):
        raise RuntimeError(f"Playwright browser not found at: {exe}")

    pages = [
        f"https://www.zomato.com/{city}/delivery?sort=rating",
        f"https://www.zomato.com/{city}/restaurants?sort=rating",
        f"https://www.zomato.com/{city}/dine-out?sort=rating",
    ]
    discovered = {}

    eval_script = """
    () => {
      const out = [];
      const anchors = Array.from(document.querySelectorAll('a[href]'));
      for (const a of anchors) {
        const href = a.getAttribute('href') || '';
        const name = (a.textContent || '').trim().replace(/\\s+/g, ' ');
        if (!href || !name) continue;
        const card = a.closest('article,section,div');
        const cardText = card ? (card.textContent || '').replace(/\\s+/g, ' ') : '';
        out.push({ href, name, cardText });
      }
      return out;
    }
    """

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            executable_path=exe,
            args=['--disable-http2', '--disable-blink-features=AutomationControlled']
        )
        for page_url in pages:
            page = browser.new_page(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={'width': 1440, 'height': 2200}
            )
            page.goto(page_url, wait_until='domcontentloaded', timeout=90000)
            page.wait_for_timeout(2500)

            for _ in range(max_scrolls):
                page.mouse.wheel(0, 2400)
                page.wait_for_timeout(450)

            for item in page.evaluate(eval_script):
                normalized_url = normalize_restaurant_url(item.get("href"), city)
                if not normalized_url:
                    continue
                rating = parse_rating(item.get("cardText", ""))
                existing = discovered.get(normalized_url)
                if existing is None or (rating or 0) > (existing.get("rating") or 0):
                    discovered[normalized_url] = {
                        "url": normalized_url,
                        "name": item.get("name", "").strip(),
                        "rating": rating,
                        "source_page": page_url,
                    }
                if len(discovered) >= max_restaurants:
                    break
            page.close()
            if len(discovered) >= max_restaurants:
                break
        browser.close()

    restaurants = list(discovered.values())
    restaurants.sort(key=lambda x: (x.get("rating") is not None, x.get("rating") or 0), reverse=True)
    for idx, row in enumerate(restaurants, start=1):
        row["rank"] = idx
    return restaurants[:max_restaurants]


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


def parse_selection(selection_text, max_rank):
    selected = set()
    chunks = [x.strip() for x in selection_text.split(",") if x.strip()]
    for chunk in chunks:
        if "-" in chunk:
            left, right = chunk.split("-", 1)
            start = int(left)
            end = int(right)
            if start > end:
                start, end = end, start
            for rank in range(start, end + 1):
                if 1 <= rank <= max_rank:
                    selected.add(rank)
        else:
            rank = int(chunk)
            if 1 <= rank <= max_rank:
                selected.add(rank)
    return sorted(selected)


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


def scrape_one_restaurant(url):
    started = time.time()
    result = {
        "url": url,
        "status": "ok",
        "error": "",
        "menu_rows": 0,
        "elapsed_sec": 0.0,
        "info": None,
        "menu_df": None,
    }
    try:
        info = run_with_retries(lambda: get_info(url), attempts=3, base_sleep=1.5)
        result["info"] = info
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = f"info_error: {exc}"

    try:
        menu_df = run_with_retries(
            lambda: get_menu(url=url, save=False, use_playwright=True),
            attempts=2,
            base_sleep=2.0
        )
        result["menu_rows"] = len(menu_df)
        result["menu_df"] = menu_df
    except Exception as exc:
        if result["status"] == "ok":
            result["status"] = "partial_failed"
            result["error"] = f"menu_error: {exc}"
        else:
            result["error"] = f"{result['error']} | menu_error: {exc}"

    if result["status"] == "partial_failed" and result["menu_rows"] > 0:
        result["status"] = "ok"
        result["error"] = ""
    result["elapsed_sec"] = round(time.time() - started, 2)
    return result


def scrape_selected(restaurants, selected_ranks, concurrency):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    selected_rows = [r for r in restaurants if r["rank"] in selected_ranks]
    print(f"[interactive] scraping {len(selected_rows)} selected restaurants...")

    summary_rows = []
    info_rows = []
    menu_frames = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        future_map = {
            pool.submit(scrape_one_restaurant, row["url"]): row
            for row in selected_rows
        }
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
                }

            summary_rows.append({
                "rank": meta["rank"],
                "name": meta["name"],
                "discovered_rating": meta.get("rating"),
                "url": outcome["url"],
                "status": outcome["status"],
                "error": outcome["error"],
                "menu_rows": outcome["menu_rows"],
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

            print(
                f"[interactive] {completed}/{total} rank={meta['rank']} "
                f"menu={outcome['menu_rows']} "
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
        while True:
            raw = input("Enter rank selection (examples: 1-10 or 1-5,8,12-15): ").strip()
            try:
                selected_ranks = parse_selection(raw, len(restaurants))
                if not selected_ranks:
                    raise ValueError("No valid ranks selected")
                break
            except Exception:
                print("Invalid selection format. Try again.")

        print(f"[interactive] selected ranks: {selected_ranks}")
        if not ask_yes_no("Proceed with scraping these ranks?", default="y"):
            if ask_yes_no("Do you want to choose another range?", default="y"):
                continue
            print("[interactive] exiting without scraping.")
            break

        scrape_selected(restaurants, selected_ranks, concurrency)
        if not ask_yes_no("Scrape another range from the same top list?", default="n"):
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
