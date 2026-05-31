#!/usr/bin/env python3
"""
Phase 1: Discover all relevant 1mg URLs via sitemaps and save to a file.

Output:
    output/one_mg_urls.txt  (one URL per line)
"""

import os
import urllib.parse
from typing import List, Set

import requests
from lxml import etree

BASE_DOMAIN = "www.1mg.com"
BASE_URL = f"https://{BASE_DOMAIN}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UrlDiscovery/1.0; +https://example.com/bot-info)",
    "Accept-Language": "en-US,en;q=0.9",
}

# What we want to keep
ALLOWED_PATH_PREFIXES = [
    "/otc/",                 # OTC products / SKUs
    "/drug-store/",          # branded SKUs
    "/drugs/",               # brand drugs
    "/generics/",            # generic drugs
    "/drugs-all-medicines",  # global listing
    "/drugs-therapeutic-classes",
    "/categories/",          # retail categories
    "/marketer/",            # marketers
    "/cancer-care/",         # cancer-care resources
    "/all-diseases",         # diseases
    "/labs/",                # lab tests
    "/doctors/",             # doctors
]

# What to skip hard
DISALLOWED_PREFIXES = [
    "/cart",
    "/admin",
    "/checkout",
    "/checkout/prescription",
    "/checkout/address",
    "/checkout/confirm",
    "/search",
    "/checkDrugInteraction",
    "/checkdruginteraction",
    "/articles/",
    "/community",
    "/trends",
    "/drugs-usage",
    "/drugs-ailments",
    "/psp",
    "/vendor-details",
    "/corporate/",
]

# Sitemaps to read (main index + important children)
SITEMAP_URLS = [
    f"{BASE_URL}/sitemap.xml",                 # sitemap index (drugs, otc, hi_drugs, etc.)
    f"{BASE_URL}/labs/sitemap.xml",           # labs
    f"{BASE_URL}/doctors/sitemap.xml",        # doctors (may be empty / NO_RESP sometimes)
    f"{BASE_URL}/sitemap_miscellaneous_1.xml",# misc (diseases, categories, etc.)
    f"{BASE_URL}/sitemap_generics_2.xml",     # generics subset
]

session = requests.Session()
session.headers.update(HEADERS)


def allowed_by_path(path: str) -> bool:
    for d in DISALLOWED_PREFIXES:
        if path.startswith(d):
            return False
    return any(path.startswith(p) for p in ALLOWED_PATH_PREFIXES)


def fetch(url: str) -> requests.Response | None:
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"[fetch] {url} -> {resp.status_code}")
            return None
        return resp
    except requests.RequestException as e:
        print(f"[fetch] {url} error: {e}")
        return None


def parse_sitemap(url: str) -> List[str]:
    resp = fetch(url)
    if not resp:
        print(f"[sitemap] failed {url}")
        return []

    try:
        root = etree.fromstring(resp.content)
    except Exception as e:
        print(f"[sitemap] parse error {url}: {e}")
        # fallback: plain-text URL per line
        urls: List[str] = []
        for line in resp.text.splitlines():
            line = line.strip()
            if line.startswith("http"):
                urls.append(line.split()[0])
        return urls

    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls: List[str] = []

    # if sitemap index: <sitemap><loc>...</loc></sitemap>
    for loc in root.findall(".//sm:sitemap/sm:loc", ns):
        child = loc.text.strip()
        urls.extend(parse_sitemap(child))

    # if URL set: <url><loc>...</loc></url>
    for loc in root.findall(".//sm:url/sm:loc", ns):
        if loc.text:
            urls.append(loc.text.strip())

    return urls


def main():
    os.makedirs("output", exist_ok=True)

    all_urls: Set[str] = set()

    for sm in SITEMAP_URLS:
        urls = parse_sitemap(sm)
        print(f"[sitemap] {sm} -> {len(urls)} URLs")
        for u in urls:
            parsed = urllib.parse.urlparse(u)
            if parsed.netloc != BASE_DOMAIN:
                continue
            path = parsed.path
            if not allowed_by_path(path):
                continue
            normalized = urllib.parse.urlunparse(
                ("https", BASE_DOMAIN, path, "", "", "")
            )
            all_urls.add(normalized)

    print(f"[seed] total filtered URLs: {len(all_urls)}")

    out_path = "output/one_mg_urls.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        for u in sorted(all_urls):
            f.write(u + "\n")

    print(f"[done] wrote filtered URLs to {out_path}")


if __name__ == "__main__":
    main()