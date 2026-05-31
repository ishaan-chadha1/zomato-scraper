#!/usr/bin/env python3
"""Shim: run ``scrapers/reviews/zomato_reviews_http_scraper.py`` from repo root."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

_SCRAPER = Path(__file__).resolve().parent / "scrapers" / "reviews" / "zomato_reviews_http_scraper.py"

if __name__ == "__main__":
    if not _SCRAPER.is_file():
        print(f"Missing {_SCRAPER}", file=sys.stderr)
        raise SystemExit(1)
    runpy.run_path(str(_SCRAPER), run_name="__main__")
