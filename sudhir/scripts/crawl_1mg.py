#!/usr/bin/env python3
"""
Full-site crawler for 1mg.com focused on:
- Enumerating all product/SKU pages and drug generics
- Capturing category, marketer, lab, doctor, and misc resource pages via XML sitemaps

IMPORTANT:
- This script is designed for research and testing.
- You MUST respect 1mg's robots.txt and terms of use before running at scale.
- Adjust RATE_LIMIT, CONCURRENCY, and ALLOWED_PATH_PREFIXES before production use.

Test strategy:
- Start with DRY_RUN = True and LIMIT_URLS to sanity-check.
- Inspect output JSON/NDJSON files before widening scope to "everything".
"""

import json
import os
import time
import queue
import threading
import urllib.parse
from dataclasses import dataclass, asdict
from typing import Set, Dict, Optional, List

import requests
from bs4 import BeautifulSoup
from lxml import etree

BASE_DOMAIN = "www.1mg.com"
BASE_URL = f"https://{BASE_DOMAIN}"
ROBOTS_URL = f"{BASE_URL}/robots.txt"

# Tuning knobs
RATE_LIMIT = 1.0  # seconds between requests per thread (be polite)
CONCURRENCY = 4
REQUEST_TIMEOUT = 10 
MAX_RETRIES = 3
DRY_RUN = False       # set False after testing
LIMIT_URLS = None  # max URLs to crawl in test runs; set None for no cap

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ResearchCrawler/1.0; +https://example.com/bot-info)",
    "Accept-Language": "en-US,en;q=0.9",
}

# What we DO want (main surfaces)
ALLOWED_PATH_PREFIXES = [
    "/otc/",                 # OTC SKUs, e.g. biotin tablets
    "/drug-store/",          # branded SKU URLs
    "/drugs/",               # brand drugs (if used)
    "/generics/",            # generic medicine pages (from sitemap_generics_2.xml)
    "/drugs-all-medicines",  # global listing
    "/drugs-therapeutic-classes",
    "/categories/",          # retail categories (vitamins, etc.)
    "/marketer/",            # marketer product lists
    "/cancer-care/",         # cancer-care resources
    "/all-diseases",         # disease encyclopedia
    "/labs/",                # lab tests
    "/doctors/",             # doctors directory
]

# What we explicitly avoid (aligned with robots.txt)
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

# Known sitemap endpoints (from robots.txt + manual inspection)
SITEMAP_URLS = [
    f"{BASE_URL}/sitemap.xml",                 # main sitemap index
    f"{BASE_URL}/labs/sitemap.xml",           # labs
    f"{BASE_URL}/doctors/sitemap.xml",        # doctors
    f"{BASE_URL}/sitemap_miscellaneous_1.xml",# misc resources (cancer-care, all-diseases, etc.)
    f"{BASE_URL}/sitemap_generics_2.xml",     # generics / drug taxonomy
]

@dataclass
class PageRecord:
    url: str
    status: int
    content_type: str
    title: Optional[str]
    kind: str  # product, generic, category, marketer, lab, doctor, misc


session = requests.Session()
session.headers.update(HEADERS)


def allowed_by_path(path: str) -> bool:
    """
    Check path against allowed/disallowed lists.
    """
    for d in DISALLOWED_PREFIXES:
        if path.startswith(d):
            return False
    return any(path.startswith(p) for p in ALLOWED_PATH_PREFIXES)


def fetch(url: str) -> Optional[requests.Response]:
    """
    Fetch with simple retry + backoff on 5xx and network errors.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code >= 500:
                # backoff on 5xx (server / protection issues)
                time.sleep(RATE_LIMIT * attempt)
                continue
            return resp
        except requests.RequestException:
            time.sleep(RATE_LIMIT * attempt)
            continue
    return None


def parse_sitemap(url: str) -> List[str]:
    """
    Parse sitemap or sitemap index. Handles proper XML sitemaps and
    also plain-text URL lists (fallback).
    """
    resp = fetch(url)
    if not resp or resp.status_code != 200:
        print(f"[sitemap] failed {url} status={resp.status_code if resp else 'NO_RESP'}")
        return []
    try:
        root = etree.fromstring(resp.content)
    except Exception as e:
        print(f"[sitemap] parse error {url}: {e}")
        # Fallback: treat as plain text list
        urls: List[str] = []
        for line in resp.text.splitlines():
            line = line.strip()
            if line.startswith("http"):
                parts = line.split()
                urls.append(parts[0])
        return urls

    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls: List[str] = []

    # Sitemap index case
    for loc in root.findall(".//sm:sitemap/sm:loc", ns):
        child = loc.text.strip()
        urls.extend(parse_sitemap(child))

    # URL set case
    for loc in root.findall(".//sm:url/sm:loc", ns):
        if loc.text:
            urls.append(loc.text.strip())

    # Fallback if parsed but no <url> found (rare)
    if not urls and b"http" in resp.content:
        for line in resp.text.splitlines():
            line = line.strip()
            if line.startswith("http"):
                parts = line.split()
                urls.append(parts[0])

    return urls


def classify_url(url: str) -> str:
    """
    Label URLs into a small set of kinds for downstream processing.
    """
    path = urllib.parse.urlparse(url).path
    if path.startswith("/otc/") or path.startswith("/drug-store/"):
        return "product"
    if path.startswith("/generics/"):
        return "generic"
    if (
        path.startswith("/categories/")
        or path.startswith("/drugs-therapeutic-classes")
        or path == "/drugs-all-medicines"
    ):
        return "category"
    if path.startswith("/marketer/"):
        return "marketer"
    if path.startswith("/labs/"):
        return "lab"
    if path.startswith("/doctors/"):
        return "doctor"
    return "misc"


def extract_title_and_links(resp: requests.Response) -> (Optional[str], Set[str]):
    """
    From an HTML response, get the <title> and internal links to enqueue.
    """
    ctype = resp.headers.get("Content-Type", "")
    if "text/html" not in ctype:
        return None, set()
    soup = BeautifulSoup(resp.text, "html.parser")
    title = soup.title.text.strip() if soup.title else None
    links: Set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("#"):
            continue
        parsed = urllib.parse.urlparse(urllib.parse.urljoin(BASE_URL, href))
        if parsed.netloc != BASE_DOMAIN:
            continue
        if not allowed_by_path(parsed.path):
            continue
        normalized = urllib.parse.urlunparse(
            ("https", BASE_DOMAIN, parsed.path, "", "", "")
        )
        links.add(normalized)
    return title, links


class Crawler:
    def __init__(self):
        self.visited: Set[str] = set()
        self.to_visit: "queue.Queue[str]" = queue.Queue()
        self.records: Dict[str, PageRecord] = {}
        self.lock = threading.Lock()

    def add_seed(self, url: str):
        if url not in self.visited:
            self.to_visit.put(url)

    def worker(self):
        while True:
            try:
                url = self.to_visit.get(timeout=3)
            except queue.Empty:
                return

            with self.lock:
                if url in self.visited:
                    self.to_visit.task_done()
                    continue
                self.visited.add(url)
                if LIMIT_URLS and len(self.visited) > LIMIT_URLS:
                    self.to_visit.task_done()
                    return

            if DRY_RUN:
                print(f"[DRY_RUN] would fetch: {url}")
                self.to_visit.task_done()
                continue

            resp = fetch(url)
            status = resp.status_code if resp else 0
            ctype = resp.headers.get("Content-Type", "") if resp else ""
            title, links = (None, set())
            if resp:
                title, links = extract_title_and_links(resp)

            kind = classify_url(url)
            with self.lock:
                self.records[url] = PageRecord(
                    url=url,
                    status=status,
                    content_type=ctype,
                    title=title,
                    kind=kind,
                )

            for link in links:
                if link not in self.visited:
                    self.to_visit.put(link)

            time.sleep(RATE_LIMIT)
            self.to_visit.task_done()

    def run(self, seed_urls: List[str]):
        for u in seed_urls:
            self.add_seed(u)

        threads: List[threading.Thread] = []
        for _ in range(CONCURRENCY):
            t = threading.Thread(target=self.worker, daemon=True)
            t.start()
            threads.append(t)

        self.to_visit.join()
        for t in threads:
            t.join(timeout=0)


def main():
    # Ensure output dir exists
    os.makedirs("output", exist_ok=True)

    # Build initial URL universe from sitemaps
    all_sitemap_urls: Set[str] = set()
    for sm in SITEMAP_URLS:
        urls = parse_sitemap(sm)
        print(f"[sitemap] {sm} -> {len(urls)} URLs")
        for u in urls:
            parsed = urllib.parse.urlparse(u)
            if parsed.netloc == BASE_DOMAIN and allowed_by_path(parsed.path):
                normalized = urllib.parse.urlunparse(
                    ("https", BASE_DOMAIN, parsed.path, "", "", "")
                )
                all_sitemap_urls.add(normalized)

    print(f"[seed] total candidate URLs from sitemaps (after filtering): {len(all_sitemap_urls)}")

    crawler = Crawler()
    crawler.run(sorted(all_sitemap_urls))

    if DRY_RUN:
        print(f"[DRY_RUN] visited (would fetch) {len(crawler.visited)} URLs")
        return

    # Persist results as NDJSON for downstream processing
    out_path = "output/one_mg_pages.ndjson"
    with open(out_path, "w", encoding="utf-8") as f:
        for rec in crawler.records.values():
            f.write(json.dumps(asdict(rec), ensure_ascii=False) + "\n")
    print(f"[done] wrote {len(crawler.records)} page records to {out_path}")


if __name__ == "__main__":
    main()