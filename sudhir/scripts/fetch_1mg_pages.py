#!/usr/bin/env python3
"""
Phase 2: Fetch each URL from output/one_mg_urls.txt and store basic metadata.

Input:
    output/one_mg_urls.txt  (from discover_1mg_urls.py)

Output:
    output/one_mg_pages.ndjson  (one JSON record per URL)
"""

import json
import os
import queue
import threading
import time
import urllib.parse
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Set, List

import requests
from bs4 import BeautifulSoup

BASE_DOMAIN = "www.1mg.com"
BASE_URL = f"https://{BASE_DOMAIN}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; HttpFetcher/1.0; +https://example.com/bot-info)",
    "Accept-Language": "en-US,en;q=0.9",
}

RATE_LIMIT = 1.0   # seconds between requests per thread
CONCURRENCY = 4
REQUEST_TIMEOUT = 10
MAX_RETRIES = 3
LIMIT_URLS = None  # set None for full run over all URLs


@dataclass
class PageRecord:
    url: str
    status: int
    content_type: str
    title: Optional[str]


session = requests.Session()
session.headers.update(HEADERS)


def fetch(url: str) -> Optional[requests.Response]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            # simple retry on 5xx
            if resp.status_code >= 500:
                time.sleep(RATE_LIMIT * attempt)
                continue
            return resp
        except requests.RequestException:
            time.sleep(RATE_LIMIT * attempt)
            continue
    return None


def extract_title(resp: requests.Response) -> Optional[str]:
    ctype = resp.headers.get("Content-Type", "")
    if "text/html" not in ctype:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    return soup.title.text.strip() if soup.title else None


class Fetcher:
    def __init__(self, urls: List[str]):
        self.urls = urls
        self.queue: "queue.Queue[str]" = queue.Queue()
        self.records: Dict[str, PageRecord] = {}
        self.lock = threading.Lock()

    def worker(self):
        while True:
            try:
                url = self.queue.get(timeout=3)
            except queue.Empty:
                return

            resp = fetch(url)
            status = resp.status_code if resp else 0
            ctype = resp.headers.get("Content-Type", "") if resp else ""
            title = extract_title(resp) if resp else None

            with self.lock:
                self.records[url] = PageRecord(
                    url=url,
                    status=status,
                    content_type=ctype,
                    title=title,
                )

            time.sleep(RATE_LIMIT)
            self.queue.task_done()

    def run(self):
        for i, u in enumerate(self.urls):
            if LIMIT_URLS is not None and i >= LIMIT_URLS:
                break
            self.queue.put(u)

        threads: List[threading.Thread] = []
        for _ in range(CONCURRENCY):
            t = threading.Thread(target=self.worker, daemon=True)
            t.start()
            threads.append(t)

        self.queue.join()
        for t in threads:
            t.join(timeout=0)


def main():
    os.makedirs("output", exist_ok=True)

    url_file = "output/one_mg_urls.txt"
    if not os.path.exists(url_file):
        raise FileNotFoundError(f"{url_file} not found. Run discover_1mg_urls.py first.")

    with open(url_file, encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    print(f"[fetch] loaded {len(urls)} URLs")

    fetcher = Fetcher(urls)
    fetcher.run()

    out_path = "output/one_mg_pages.ndjson"
    with open(out_path, "w", encoding="utf-8") as f:
        for rec in fetcher.records.values():
            f.write(json.dumps(asdict(rec), ensure_ascii=False) + "\n")

    print(f"[done] wrote {len(fetcher.records)} records to {out_path}")


if __name__ == "__main__":
    main()