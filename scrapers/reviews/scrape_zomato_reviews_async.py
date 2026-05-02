#!/usr/bin/env python3
"""Async Playwright: Zomato dining reviews — DOM max page + per-card DOM extraction (no Followers gate).

Writes ``data/reviews/json/<restaurant-slug>_reviews_final.json`` (and checkpoint JSON with the same slug)
where ``<restaurant-slug>`` is the last path segment of the restaurant URL (before ``/reviews``).
"""

import asyncio
import json
import random
import sys
from pathlib import Path

from playwright.async_api import async_playwright

REPO_ROOT = Path(__file__).resolve().parents[2]
REVIEW_JSON_DIR = REPO_ROOT / "data" / "reviews" / "json"


def restaurant_slug_from_reviews_url(base_url: str) -> str:
    """e.g. .../the-biere-club-lavelle-road-bangalore/reviews -> the-biere-club-lavelle-road-bangalore"""
    u = base_url.rstrip("/")
    if u.endswith("/reviews"):
        u = u[: -len("/reviews")]
    slug = u.rsplit("/", maxsplit=1)[-1] if u else ""
    return slug or "restaurant"


async def scrape_zomato_reviews(base_url: str) -> None:
    slug = restaurant_slug_from_reviews_url(base_url)
    checkpoint_json = REVIEW_JSON_DIR / f"{slug}_reviews_checkpoint.json"
    final_json = REVIEW_JSON_DIR / f"{slug}_reviews_final.json"

    REVIEW_JSON_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Slug: {slug}\nCheckpoint: {checkpoint_json}\nFinal: {final_json}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()

        start_url = f"{base_url}?page=1000&sort=dd&filter=reviews-dining"
        print("Navigating to start URL to detect max page...")

        await page.goto(start_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(4000)

        page_nums = await page.evaluate(
            """() => {
          const vals = [];
          document.querySelectorAll('a[href*="/reviews?page="], button, span').forEach((el) => {
            const txt = (el.textContent || '').trim();
            if (!/^\\d+$/.test(txt)) return;
            const n = Number(txt);
            if (n >= 1 && n <= 5000) vals.push(n);
          });
          document.querySelectorAll('a[href*="/reviews?page="]').forEach((a) => {
            const h = a.getAttribute('href') || '';
            const m = h.match(/[?&]page=(\\d+)/);
            if (m) vals.push(Number(m[1]));
          });
          return vals;
        }"""
        )
        raw_nums = [int(n) for n in (page_nums or []) if isinstance(n, (int, float))]
        max_page = max(raw_nums) if raw_nums else 1

        if max_page == 1:
            print("Could not parse pagination. Defaulting to known max page: 332")
            max_page = 332
        else:
            print(f"Detected Max Page from DOM: {max_page}")

        all_reviews: list[dict] = []

        for current_p in range(max_page, 0, -1):
            target_url = f"{base_url}?page={current_p}&sort=dd&filter=reviews-dining"
            print(f"Scraping Page {current_p}...")

            try:
                await page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(3000)

                # Card-based extraction: any visible dining/delivery review with a permalink
                # (same idea as scrape_dining_reviews DOM card extraction — no "Followers" gate).
                page_reviews = await page.evaluate(
                    """() => {
          const out = [];
          const seen = new Set();

          const hasHelpful = (t) => {
            if (/votes?\\s+for\\s+helpful/i.test(t)) return true;
            if (/\\d+\\s+helpful\\s+votes?/i.test(t)) return true;
            if (/\\d+\\s+helpful\\s+vote\\b/i.test(t)) return true;
            if (/\\d+\\s+votes?\\s+for\\s+helpful/i.test(t)) return true;
            if (/\\bhelpful\\b/i.test(t) && /\\bvotes?\\b/i.test(t)) return true;
            return false;
          };

          const hasMode = (t) =>
            /\\bDINING\\b/.test(t) || /\\bDELIVERY\\b/.test(t);

          const anchors = Array.from(document.querySelectorAll('a[href*="/reviews/"]'));
          const cardRoots = new Set();
          for (const a of anchors) {
            const h = a.getAttribute('href') || '';
            if (!/\\/reviews\\/\\d/.test(h)) continue;
            let el = a;
            for (let i = 0; i < 22 && el; i++) {
              const t = (el.innerText || '').trim();
              if (t && hasHelpful(t) && hasMode(t) && t.length >= 35 && t.length < 12000) {
                cardRoots.add(el);
                break;
              }
              el = el.parentElement;
            }
          }

          const nodes = cardRoots.size
            ? Array.from(cardRoots)
            : Array.from(document.querySelectorAll('div'));

          for (const el of nodes) {
            const t = (el.innerText || '').trim();
            if (!t) continue;
            if (!hasHelpful(t)) continue;
            if (!hasMode(t)) continue;
            if (t.length < 35 || t.length > 12000) continue;

            const lines = t.split('\\n').map(x => x.trim()).filter(Boolean);
            if (!lines.length) continue;

            let reviewUrl = '';
            for (const a of el.querySelectorAll('a[href]')) {
              const h = a.getAttribute('href') || '';
              if (!h || !h.includes('/reviews/')) continue;
              if (!/\\/reviews\\/\\d/.test(h)) continue;
              reviewUrl = h.startsWith('http') ? h : ('https://www.zomato.com' + h);
              break;
            }
            const dedupeKey = reviewUrl || t.slice(0, 280);
            if (seen.has(dedupeKey)) continue;
            seen.add(dedupeKey);

            const user = lines[0] || '';
            const ratingMatch = t.match(/\\n([1-5](?:\\.\\d)?)\\n\\s*(?:DINING|DELIVERY)\\s*\\n/i);
            const rating = ratingMatch ? ratingMatch[1] : '';

            let text = '';
            const voteIdx = lines.findIndex((x) => hasHelpful(x) || /\\bcomments\\b/i.test(x));
            const modeIdx = lines.findIndex((x) => x === 'DINING' || x === 'DELIVERY');
            if (modeIdx >= 0 && voteIdx > modeIdx + 1) {
              text = lines.slice(modeIdx + 2, voteIdx).join(' ').trim();
            }
            if (!text) {
              text = lines.slice(3, Math.max(4, lines.length - 3)).join(' ').trim();
            }

            const date =
              modeIdx >= 0 && lines[modeIdx + 1] && lines[modeIdx + 1] !== user
                ? lines[modeIdx + 1]
                : '';

            out.push({
              user: user,
              rating: rating,
              date: date,
              text: text || 'N/A',
              review_url: reviewUrl,
            });
          }
          return out;
        }"""
                )

                for r in page_reviews:
                    r["page"] = current_p
                    all_reviews.append(r)

                print(f"Extracted {len(page_reviews)} reviews from page {current_p}.")

                await asyncio.sleep(random.uniform(2, 4))

                if current_p % 10 == 0:
                    with checkpoint_json.open("w", encoding="utf-8") as f:
                        json.dump(all_reviews, f, indent=4)

            except Exception as e:
                print(f"Error on page {current_p}: {e}")
                continue

        with final_json.open("w", encoding="utf-8") as f:
            json.dump(all_reviews, f, indent=4)

        print(
            f"Scraping complete. Total reviews collected: {len(all_reviews)} -> {final_json}"
        )
        await browser.close()


def main() -> int:
    restaurant_url = (
        "https://www.zomato.com/bangalore/the-biere-club-lavelle-road-bangalore/reviews"
    )
    if len(sys.argv) > 1:
        restaurant_url = sys.argv[1].rstrip("/")
    asyncio.run(scrape_zomato_reviews(restaurant_url))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
