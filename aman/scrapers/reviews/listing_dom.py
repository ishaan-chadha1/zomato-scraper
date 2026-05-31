"""Restaurant review listing: Share+clipboard (zoma.to) first, /reviews/<id> anchors fallback."""

from __future__ import annotations

import asyncio
import random
import re

from playwright.async_api import Page

CLIPBOARD_URL_RE = re.compile(
    r"https?://(?:www\.)?(?:zoma\.to|zomato\.me)/[^\s'\"<>]+",
    re.I,
)

TAG_LISTING_VIA_SHARE_JS = r"""() => {
  const hasHelpful = (t) => {
    if (/votes?\s+for\s+helpful/i.test(t)) return true;
    if (/\d+\s+votes?\s+for\s+helpful/i.test(t)) return true;
    if (/\d+\s+helpful\s+votes?/i.test(t)) return true;
    if (/\d+\s+helpful\s+vote\b/i.test(t)) return true;
    if (/\d+\s+Helpful vote/i.test(t)) return true;
    if (/\bhelpful\b/i.test(t) && /\bvotes?\b/i.test(t)) return true;
    return false;
  };
  const hasMode = (t) => /\b(DINING|DELIVERY)\b/i.test(t);

  document.querySelectorAll('[data-zs-share-idx]').forEach((e) => e.removeAttribute('data-zs-share-idx'));

  const junkUser = new Set([
    'log in', 'sign up', 'home', 'order', 'english', 'india', 'follow',
    'the biere club dining reviews', 'dining reviews', 'delivery reviews',
    'zomato gold', 'book a table', 'menu', 'photos', 'reviews'
  ]);

  const badShareContext = (s) =>
    /twitter|facebook|whatsapp|instagram|shareholders/i.test(s);

  /** Zomato often uses a plain div/span with visible text "Share", not role=button. */
  const looksLikeShare = (el) => {
    const raw = (el.innerText || '').replace(/\s+/g, ' ').trim();
    const aria = (
      (el.getAttribute('aria-label') || '') +
      ' ' +
      (el.getAttribute('title') || '') +
      ' ' +
      (el.getAttribute('data-tooltip') || '')
    ).toLowerCase();
    let p = el.parentElement;
    let extra = '';
    for (let u = 0; u < 6 && p; u++) {
      extra += ' ' + (p.getAttribute('aria-label') || '').toLowerCase();
      p = p.parentElement;
    }
    const blob = (raw + ' ' + aria + extra).toLowerCase();
    if (!blob.includes('share')) return false;
    if (badShareContext(blob)) return false;
    if (raw.length > 120) return false;
    return true;
  };

  const collectShareElements = () => {
    const out = [];
    const seen = new Set();
    const add = (el) => {
      if (!el || seen.has(el)) return;
      if (!looksLikeShare(el)) return;
      seen.add(el);
      out.push(el);
    };

    document
      .querySelectorAll('button, [role="button"], a[role="button"], div[role="button"]')
      .forEach(add);

    document.querySelectorAll('div, span, a, p, li').forEach((el) => {
      const t = (el.textContent || '').replace(/\s+/g, ' ').trim();
      if (!/^share$/i.test(t)) return;
      if (t.length > 12) return;
      add(el);
    });

    return out;
  };

  const userProfileLinkCount = (el) =>
    el.querySelectorAll('a[href*="/users/"]').length;

  /** Smallest qualifying ancestor = one review card; larger blocks merge many rows and break text/user extraction. */
  const pickSmallestReviewCard = (cand) => {
    if (!cand.length) return null;
    cand.sort((a, b) => a.len - b.len || a.depth - b.depth);
    return cand[0].el;
  };

  const MIN_CARD_LEN = 100;

  const bestCardForShare = (btn) => {
    const cand = [];
    let el = btn;
    for (let j = 0; j < 40 && el; j++) {
      const t = (el.innerText || '').trim();
      if (
        t.length >= MIN_CARD_LEN &&
        t.length <= 14000 &&
        hasHelpful(t) &&
        hasMode(t)
      ) {
        cand.push({ el, len: t.length, depth: j, uc: userProfileLinkCount(el) });
      }
      el = el.parentElement;
    }
    return pickSmallestReviewCard(cand);
  };

  /** Skip header/footer Share: must sit under some review-sized block. */
  const hasReviewAncestor = (btn) => {
    let el = btn;
    for (let j = 0; j < 36 && el; j++) {
      const t = (el.innerText || '').trim();
      if (
        t.length >= 160 &&
        t.length <= 20000 &&
        hasHelpful(t) &&
        hasMode(t)
      ) {
        return true;
      }
      el = el.parentElement;
    }
    return false;
  };

  /** Keep innermost Share only: skip outer wrapper if another collected Share sits inside it. */
  const isInnermostShareAmongCollected = (btn, allBtns) => {
    for (const o of allBtns) {
      if (o !== btn && btn.contains(o)) return false;
    }
    return true;
  };

  const extractListingFields = (card) => {
    const empty = { user: '', rating: '', date: '', text: 'N/A' };
    if (!card) return empty;
    const full = (card.innerText || '').trim();
    const lines = full.split('\n').map((x) => x.trim()).filter(Boolean);
    let user = '';
    const uA = card.querySelector('a[href*="/users/"]');
    if (uA) user = (uA.innerText || '').trim().split('\n')[0].trim();
    if (!user && lines.length) user = lines[0] || '';
    const userNorm = user.toLowerCase().trim();
    if (!userNorm || junkUser.has(userNorm) || user.length > 80) user = '';
    const ratingMatch = full.match(/([1-5](?:\.\d)?)\s*(?:\n|\r\n|\s)+(DINING|DELIVERY)/i);
    const rating = ratingMatch ? ratingMatch[1] : '';
    const voteIdx = lines.findIndex((x) => hasHelpful(x) || /\bcomments\b/i.test(x));
    const modeIdx = lines.findIndex((x) => /^(DINING|DELIVERY)$/i.test(x));
    let text = '';
    if (modeIdx >= 0 && voteIdx > modeIdx + 1) {
      text = lines.slice(modeIdx + 2, voteIdx).join(' ').trim();
    }
    if (!text) text = lines.slice(3, Math.max(4, lines.length - 3)).join(' ').trim();
    const date =
      modeIdx >= 0 && lines[modeIdx + 1] && lines[modeIdx + 1] !== user ? lines[modeIdx + 1] : '';
    return { user, rating, date, text: text || 'N/A' };
  };

  /** One listing row per Share control — no user/text dedupe (that collapsed all rows to one). */
  const hits = [];
  const allShareBtns = collectShareElements();

  for (const btn of allShareBtns) {
    if (!hasReviewAncestor(btn)) continue;
    if (!isInnermostShareAmongCollected(btn, allShareBtns)) continue;
    const card = bestCardForShare(btn);
    const fields = extractListingFields(card);
    const idx = hits.length;
    btn.setAttribute('data-zs-share-idx', String(idx));
    hits.push({
      user: fields.user,
      rating: fields.rating,
      date: fields.date,
      text: fields.text,
      share_index: idx,
    });
  }

  if (hits.length === 0) {
    const helpEls = Array.from(document.querySelectorAll('p, span, div')).filter((el) => {
      const t = (el.textContent || '').trim();
      return (
        t.length < 140 &&
        /votes?\s+for\s+helpful/i.test(t) &&
        /comment/i.test(t)
      );
    });
    const seenShareBtn = new Set();
    for (const he of helpEls) {
      const hc = [];
      let p = he;
      for (let j = 0; j < 28 && p; j++) {
        const t = (p.innerText || '').trim();
        if (t.length >= MIN_CARD_LEN && t.length <= 14000 && hasHelpful(t) && hasMode(t)) {
          hc.push({ el: p, len: t.length, depth: j, uc: userProfileLinkCount(p) });
        }
        p = p.parentElement;
      }
      const card = pickSmallestReviewCard(hc);
      if (!card) continue;
      const shareBtn = allShareBtns.find((b) => card.contains(b));
      if (!shareBtn || seenShareBtn.has(shareBtn)) continue;
      seenShareBtn.add(shareBtn);
      if (!hasReviewAncestor(shareBtn)) continue;
      const fields = extractListingFields(card);
      const idx = hits.length;
      shareBtn.setAttribute('data-zs-share-idx', String(idx));
      hits.push({
        user: fields.user,
        rating: fields.rating,
        date: fields.date,
        text: fields.text,
        share_index: idx,
      });
    }
  }

  return hits;
}"""

LISTING_CARDS_LEGACY_JS = r"""() => {
  const out = [];
  const seen = new Set();

  const hasHelpful = (t) => {
    if (/votes?\s+for\s+helpful/i.test(t)) return true;
    if (/\d+\s+votes?\s+for\s+helpful/i.test(t)) return true;
    if (/\d+\s+helpful\s+votes?/i.test(t)) return true;
    if (/\d+\s+helpful\s+vote\b/i.test(t)) return true;
    if (/\d+\s+Helpful vote/i.test(t)) return true;
    if (/\bhelpful\b/i.test(t) && /\bvotes?\b/i.test(t)) return true;
    return false;
  };

  const hasMode = (t) => /\b(DINING|DELIVERY)\b/i.test(t);

  const junkUser = new Set([
    'log in', 'sign up', 'home', 'order', 'english', 'india', 'follow',
    'the biere club dining reviews', 'dining reviews', 'delivery reviews',
    'zomato gold', 'book a table', 'menu', 'photos', 'reviews'
  ]);

  const reviewAnchors = Array.from(document.querySelectorAll('a[href*="/reviews/"]')).filter((a) => {
    const h = a.getAttribute('href') || '';
    if (/[?&]page=/.test(h)) return false;
    return /\/reviews\/\d+/.test(h);
  });

  for (const a of reviewAnchors) {
    const rawHref = (a.getAttribute('href') || '').trim();
    const pathOnly = rawHref.split('?')[0];
    const reviewUrl = pathOnly.startsWith('http')
      ? pathOnly
      : ('https://www.zomato.com' + pathOnly);
    if (seen.has(reviewUrl)) continue;

    let el = a;
    let card = null;
    for (let i = 0; i < 28 && el; i++) {
      const t = (el.innerText || '').trim();
      if (t.length >= 100 && t.length <= 14000 && hasHelpful(t) && hasMode(t)) {
        card = el;
        break;
      }
      el = el.parentElement;
    }
    if (!card) continue;

    const t = (card.innerText || '').trim();
    const lines = t.split('\n').map((x) => x.trim()).filter(Boolean);
    if (!lines.length) continue;

    let user = '';
    const uA = card.querySelector('a[href*="/users/"]');
    if (uA) {
      user = (uA.innerText || '').trim().split('\n')[0].trim();
    }
    if (!user) user = lines[0] || '';
    const userNorm = user.toLowerCase().trim();
    if (!userNorm || junkUser.has(userNorm) || user.length > 100) continue;

    const ratingMatch = t.match(/([1-5](?:\.\d)?)\s*(?:\n|\r\n|\s)+(DINING|DELIVERY)/i);
    const rating = ratingMatch ? ratingMatch[1] : '';

    let text = '';
    const voteIdx = lines.findIndex((x) => hasHelpful(x) || /\bcomments\b/i.test(x));
    const modeIdx = lines.findIndex((x) => /^(DINING|DELIVERY)$/i.test(x));
    if (modeIdx >= 0 && voteIdx > modeIdx + 1) {
      text = lines.slice(modeIdx + 2, voteIdx).join(' ').trim();
    }
    if (!text) {
      text = lines.slice(3, Math.max(4, lines.length - 3)).join(' ').trim();
    }

    const date =
      modeIdx >= 0 && lines[modeIdx + 1] && lines[modeIdx + 1] !== user ? lines[modeIdx + 1] : '';

    seen.add(reviewUrl);
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


def normalize_pasted_share_url(raw: str) -> str:
    s = (raw or "").strip().strip('"').strip("'").strip()
    m = CLIPBOARD_URL_RE.search(s)
    if m:
        return m.group(0).rstrip(".,);]")
    parts = s.split()
    if parts and parts[0].startswith("http") and "zoma" in parts[0].lower():
        return parts[0].rstrip(".,);]")
    return ""


async def try_read_share_url_from_modal(page: Page) -> str:
    for sel in (
        'input[value*="zoma"]',
        'input[value*="zomato.me"]',
        'input[type="text"]',
        "textarea",
    ):
        loc = page.locator(sel).first
        try:
            if await loc.count() == 0:
                continue
            v = await loc.input_value(timeout=1200)
            u = normalize_pasted_share_url(v)
            if u:
                return u
        except Exception:
            try:
                v = await loc.get_attribute("value")
                u = normalize_pasted_share_url(v or "")
                if u:
                    return u
            except Exception:
                continue
    return ""


LISTING_PAGE_DEBUG_JS = r"""() => {
  const txt = (document.body && document.body.innerText) ? document.body.innerText : '';
  const bad = (s) => /twitter|facebook|whatsapp|instagram/i.test(s);
  let shareish = 0;
  document
    .querySelectorAll('button, [role="button"], div[role="button"], div, span, a')
    .forEach((b) => {
      const raw = (b.innerText || '').replace(/\s+/g, ' ').trim();
      const s = (raw + ' ' + (b.getAttribute('aria-label') || '')).toLowerCase();
      if (!s.includes('share') || bad(s)) return;
      if (raw.length > 120) return;
      shareish++;
    });
  let exactShare = 0;
  document.querySelectorAll('div, span, a').forEach((el) => {
    const t = (el.textContent || '').replace(/\s+/g, ' ').trim();
    if (/^share$/i.test(t) && t.length <= 12) exactShare++;
  });
  return {
    bodyLen: txt.length,
    hasDiningWord: /\b(DINING|DELIVERY)\b/i.test(txt),
    hasHelpfulLine: /votes?\s+for\s+helpful/i.test(txt),
    shareLikeControls: shareish,
    exactShareLabels: exactShare,
  };
}"""


async def collect_review_listing_page(page: Page, base_url: str, page_no: int) -> list[dict]:
    target_url = f"{base_url}?page={page_no}&sort=dd&filter=reviews-dining"
    await page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(2500)
    try:
        await page.locator("text=/DINING|DELIVERY/i").first.wait_for(timeout=18000, state="attached")
    except Exception:
        pass
    for _ in range(2):
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(200)
    for _ in range(5):
        await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1000)
    await page.evaluate("() => window.scrollTo(0, 0)")
    await page.wait_for_timeout(700)

    via_share = await page.evaluate(TAG_LISTING_VIA_SHARE_JS)
    if not isinstance(via_share, list):
        via_share = []

    out: list[dict] = []
    if via_share:
        seen_share_urls: set[str] = set()
        for row in via_share:
            idx = int(row.get("share_index", -1))
            if idx < 0:
                continue
            base_row = {k: v for k, v in row.items() if k != "share_index"}
            share_url = ""
            err = ""
            loc = page.locator(f'[data-zs-share-idx="{idx}"]').first
            try:
                await loc.scroll_into_view_if_needed(timeout=15000)
                await loc.click(timeout=15000)
                await page.wait_for_timeout(900)
                try:
                    clip = await page.evaluate(
                        """async () => {
                        try { return (await navigator.clipboard.readText() || '').trim(); }
                        catch (e) { return ''; }
                    }"""
                    )
                except Exception:
                    clip = ""
                share_url = normalize_pasted_share_url(clip)
                if not share_url:
                    share_url = await try_read_share_url_from_modal(page)
                await page.keyboard.press("Escape")
                await page.wait_for_timeout(450)
            except Exception as e:
                err = str(e)
            base_row["share_url"] = share_url
            if err:
                base_row["share_click_error"] = err
            if share_url:
                base_row["review_url"] = share_url
                if share_url in seen_share_urls:
                    continue
                seen_share_urls.add(share_url)
            out.append(base_row)
            await asyncio.sleep(random.uniform(0.35, 0.85))
        return out

    legacy = await page.evaluate(LISTING_CARDS_LEGACY_JS)
    legacy = legacy if isinstance(legacy, list) else []
    if not legacy:
        dbg = await page.evaluate(LISTING_PAGE_DEBUG_JS)
        print(
            f"[listing_dom] page {page_no}: no rows. DOM debug: {dbg!r} "
            "(if hasDiningWord is false you may be on a login/geo/captcha wall — use headed browser, "
            "same network as manual Chrome, or pass saved storage_state.)",
            flush=True,
        )
    return legacy


async def grant_clipboard_permissions(context) -> None:
    for origin in (
        "https://www.zomato.com",
        "https://www.zoma.to",
        "https://zoma.to",
    ):
        try:
            await context.grant_permissions(
                ["clipboard-read", "clipboard-write"],
                origin=origin,
            )
        except Exception:
            pass
