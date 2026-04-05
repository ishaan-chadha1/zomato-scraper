import os
import json
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup, FeatureNotFound
from urllib.parse import urlparse

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None

from playwright_chromium import chromium_launch_kwargs

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh;'
                         ' Intel Mac OS X 10_15_4)'
                         ' AppleWebKit/537.36 (KHTML, like Gecko)'
                         ' Chrome/83.0.4103.97 Safari/537.36'}


def parse_html(text):
    try:
        return BeautifulSoup(text, 'lxml')
    except FeatureNotFound:
        return BeautifulSoup(text, 'html.parser')


def get_description(html_text):
    """ Gets the Menu Item along with Description and Category """

    menu = {}
    scripts = html_text.find_all('script', type='application/ld+json')
    for script in scripts:
        if not script.string:
            continue
        try:
            parsed = json.loads(script.string)
        except json.JSONDecodeError:
            continue

        if isinstance(parsed, dict) and isinstance(parsed.get('hasMenuSection'), list):
            menu = parsed
            break

    if not menu:
        columns = ['Name', 'Description', 'Category']
        return pd.DataFrame([], columns=columns)

    data = []
    for section in menu['hasMenuSection']:
        name = section['name']
        if name == "Recommended":
            continue
        menu_items = section['hasMenuSection'][0]['hasMenuItem']
        for item in menu_items:
            data.append((
                item['name'],
                item['description'],
                name,
            ))

    # Creating the dataframe
    columns = ['Name', 'Description', 'Category']
    return pd.DataFrame(data, columns=columns)


def get_price_tags(html_text):
    """ Gets the Menu Item along with Price and Tags """
    
    menu_items = [div for div in html_text.find_all('div') if div.find('h4', recursive=False)]
    data = []
    for item in menu_items:
        item = item.find_all(text=True)
        name = item[0]
        price = item[-1].replace("₹", "Rs ")
        tags = ", ".join(item[1:-1])
        data.append((name, price, tags))

    # Creating the dataframe
    columns = ['Name', 'Price', 'Tags']
    df = pd.DataFrame(data, columns=columns)
    df = df[~df.duplicated(['Name'])]
    df = df.reset_index().drop(columns='index')
    return df


def save_df(name, df):
    """ Save the dataframe """
    
    if not os.path.exists("Menu"):
        os.makedirs("Menu")
    df.to_csv(f"Menu/{name}.csv", index=False)


def _clean_line(text):
    return re.sub(r'\s+', ' ', text).strip()


def _is_probable_item_name(text):
    if not text or len(text) > 85:
        return False
    lowered = text.lower()
    if lowered in {"order online", "download the app", "search within menu"}:
        return False
    if lowered.startswith("serves "):
        return False
    if "read more" in lowered:
        return False
    if "." in text and text.count(".") >= 2:
        return False
    if text.endswith(")") and re.search(r'\(\d+\)$', text):
        return False
    return True


def _extract_menu_from_rendered_text(page_text):
    lines = [_clean_line(line) for line in page_text.splitlines()]
    lines = [line for line in lines if line]

    # Category list appears as "Category Name (N)" before actual item listing.
    category_counts = []
    for line in lines:
        match = re.match(r'^(.*?)\s*\((\d+)\)$', line)
        if match:
            name = _clean_line(match.group(1))
            if name.lower() not in {"menu", "photos", "reviews", "overview"}:
                category_counts.append(name)

    category_names = list(dict.fromkeys(category_counts))
    category_set = set(category_names)

    start_idx = 0
    for marker in ("Search within menu", "Order Online"):
        if marker in lines:
            start_idx = max(start_idx, lines.index(marker) + 1)

    rows = []
    current_category = None
    i = start_idx
    while i < len(lines):
        line = lines[i]

        if line in category_set:
            current_category = line
            i += 1
            continue

        if _is_probable_item_name(line) and current_category:
            next_line = lines[i + 1] if i + 1 < len(lines) else ""
            description = ""
            if next_line and next_line not in category_set and not _is_probable_item_name(next_line):
                description = next_line

            price = ""
            tags = ""
            # Price is often absent in desktop web; keep blank when unavailable.
            for j in range(i + 1, min(i + 4, len(lines))):
                if re.search(r'₹\s*\d+', lines[j]):
                    price = lines[j]
                    break

            rows.append((line, description, current_category, price, tags))
        i += 1

    # Fallback: some pages don't expose category list (no "(N)" lines). In that case,
    # still extract probable item rows under a generic category.
    if not rows and lines:
        current_category = "Menu"
        i = start_idx
        while i < len(lines):
            line = lines[i]
            if _is_probable_item_name(line):
                next_line = lines[i + 1] if i + 1 < len(lines) else ""
                description = ""
                if next_line and not _is_probable_item_name(next_line):
                    description = next_line
                rows.append((line, description, current_category, "", ""))
            i += 1

    columns = ['Name', 'Description', 'Category', 'Price', 'Tags']
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        return df

    df = df.drop_duplicates(subset=['Name', 'Category']).reset_index(drop=True)
    return df


def _restaurant_name_from_url(url):
    path = urlparse(url).path.strip("/")
    slug = path.split("/")[-1] if path else "restaurant"
    return slug.replace("-", " ").title()


def _restaurant_name_from_title(title_text, fallback):
    title = (title_text or "").strip()
    if "," in title:
        return title.split(",")[0].strip()
    if "|" in title:
        return title.split("|")[0].strip()
    return fallback


def _get_menu_via_playwright(url, executable_path=None):
    if sync_playwright is None:
        return None, None, "playwright_not_installed"

    fallback_name = _restaurant_name_from_url(url)

    target = url if url.endswith("/order") else f"{url}/order"
    with sync_playwright() as p:
        browser = p.chromium.launch(**chromium_launch_kwargs(executable_path))
        page = browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={'width': 1440, 'height': 2200}
        )
        # Speed + reliability: block heavy resources.
        try:
            page.route(
                "**/*",
                lambda route, request: route.abort()
                if request.resource_type in {"image", "media", "font"}
                else route.continue_()
            )
        except Exception:
            pass

        print("[menu] Navigating to /order (same as browser; can take 30–120s)…", flush=True)
        last_exc = None
        for attempt in range(2):
            try:
                page.goto(target, wait_until="commit", timeout=120000)
                try:
                    page.wait_for_load_state("domcontentloaded", timeout=45000)
                except Exception:
                    pass
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                page.wait_for_timeout(1200)
        if last_exc is not None:
            browser.close()
            raise last_exc

        page.wait_for_timeout(4000)
        for _ in range(8):
            page.mouse.wheel(0, 2400)
            page.wait_for_timeout(800)

        title_text = page.title()
        page_text = page.evaluate("() => document.body ? document.body.innerText : ''")
        browser.close()

    restaurant_name = _restaurant_name_from_title(title_text, fallback_name)
    menu_df = _extract_menu_from_rendered_text(page_text)
    return menu_df, restaurant_name, None


def get_menu(url, save=True, use_playwright=True, executable_path=None):
    """Get menu items from URL, optionally using Playwright."""

    if use_playwright:
        try:
            menu_df, restaurant_name, error = _get_menu_via_playwright(url, executable_path=executable_path)
            if error:
                print(f"[menu] Playwright unavailable ({error}). Falling back to static HTML parser.")
            elif menu_df is not None:
                print(f"[menu] Playwright extracted {len(menu_df)} menu rows for: {restaurant_name}")
                if save and not menu_df.empty:
                    save_df(restaurant_name, menu_df)
                return menu_df
        except Exception as exc:
            print(f"[menu] Playwright scraping failed: {exc}. Falling back to static HTML parser.")

    # Static fallback for environments where browser automation is unavailable.
    target_url = url if url.endswith('/order') else f"{url}/order"
    webpage = requests.get(target_url, headers=headers, timeout=5)
    html_text = parse_html(webpage.text)
    title_text = html_text.head.find('title').text if html_text.head and html_text.head.find('title') else ""
    restaurant_name = _restaurant_name_from_title(title_text, _restaurant_name_from_url(url))

    df1 = get_description(html_text)
    df2 = get_price_tags(html_text)
    if df1.empty or df2.empty:
        print(f"[menu] Menu items not available in page source for: {restaurant_name}")
        columns = ['Name', 'Description', 'Category', 'Price', 'Tags']
        return pd.DataFrame([], columns=columns)

    menu_df = df1.merge(df2, on='Name')
    if save:
        save_df(restaurant_name, menu_df)
    return menu_df


if __name__ == "__main__":
    link = "https://www.zomato.com/bangalore/voosh-thalis-bowls-1-bellandur-bangalore"
    dframe = get_menu(link, save=True)
