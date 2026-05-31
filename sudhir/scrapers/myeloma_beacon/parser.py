"""HTML parsers for the Myeloma Beacon phpBB3 forum.

Pure, side-effect-free functions that take raw HTML strings and return
typed dicts. Easy to unit-test against saved fixture pages.

Two parsers:

* :func:`parse_index_page`  — sub-forum index (25 topics per page).
* :func:`parse_topic_page`  — topic view (10 posts per page).
"""

from __future__ import annotations

import json
import re
from typing import Any, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup, FeatureNotFound, Tag

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _soup(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:  # pragma: no cover
        return BeautifulSoup(html, "html.parser")


_T_ID_RE = re.compile(r"-t(\d+)(?:-(\d+))?\.html(?:[?#]|$)")
_P_ID_RE = re.compile(r"#p(\d+)")
_INT_RE = re.compile(r"\d+")


def _topic_id_from_href(href: str) -> Optional[int]:
    if not href:
        return None
    m = _T_ID_RE.search(href)
    return int(m.group(1)) if m else None


def _post_offset_from_href(href: str) -> Optional[int]:
    if not href:
        return None
    m = _T_ID_RE.search(href)
    if not m:
        return None
    return int(m.group(2)) if m.group(2) else 0


def _slug_from_topic_url(url: str) -> str:
    m = re.search(r"/forum/([a-z0-9-]+)-t\d+", url)
    return m.group(1) if m else ""


def _strip(text: Optional[str]) -> str:
    return (text or "").strip()


def _first_int(text: str) -> Optional[int]:
    m = _INT_RE.search(text or "")
    return int(m.group(0)) if m else None


# ---------------------------------------------------------------------------
# Index page
# ---------------------------------------------------------------------------


def parse_index_total_topics(html: str) -> Optional[int]:
    """Pull ``2776`` out of ``2776 topics`` from the index header."""
    m = re.search(r"([\d,]+)\s+topics\b", html, re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


def parse_index_total_pages(html: str) -> Optional[int]:
    """Pull ``112`` out of ``Page 1 of 112`` from the pagination block."""
    m = re.search(
        r"Page\s*<strong>\s*\d+\s*</strong>\s*of\s*<strong>\s*(\d+)\s*</strong>",
        html,
        re.IGNORECASE,
    )
    if m:
        return int(m.group(1))
    m = re.search(r"Page\s+\d+\s+of\s+(\d+)", html, re.IGNORECASE)
    return int(m.group(1)) if m else None


def parse_index_page(html: str, *, base_url: str = "https://myelomabeacon.org/forum/") -> dict[str, Any]:
    """Parse one sub-forum index page.

    Returns
    -------
    dict with keys:
        ``total_topics``  : int | None
        ``total_pages``   : int | None
        ``topics``        : list of topic dicts
    """
    soup = _soup(html)
    out: dict[str, Any] = {
        "total_topics": parse_index_total_topics(html),
        "total_pages": parse_index_total_pages(html),
        "topics": [],
    }

    for row in soup.select("ul.topiclist.topics li.row"):
        title_a = row.select_one("a.topictitle")
        if not title_a:
            continue
        href = urljoin(base_url, title_a.get("href", ""))
        topic_id = _topic_id_from_href(href)
        if topic_id is None:
            continue

        dt = row.find("dt")
        starter_name = None
        starter_profile = None
        start_date = None
        if dt:
            # The starter "by NAME on DATE" line is the trailing text of the <dt>.
            dt_text = dt.get_text(" ", strip=True)
            # Strip the title prefix:
            title_text = title_a.get_text(" ", strip=True)
            if dt_text.startswith(title_text):
                dt_text = dt_text[len(title_text):].strip()
            # Author profile link — last non-title <a> with member URL inside dt.
            for a in dt.find_all("a", href=True):
                ahref = a.get("href", "")
                if "/forum/member" in ahref and a is not title_a:
                    starter_name = _strip(a.get_text())
                    starter_profile = urljoin(base_url, ahref)
                    break
            m = re.search(r"by\s+.+?\s+on\s+(.+)$", dt_text)
            if m:
                start_date = _strip(m.group(1))

        replies = _first_int(_strip(row.select_one("dd.posts").get_text(" "))) if row.select_one("dd.posts") else None
        views = _first_int(_strip(row.select_one("dd.views").get_text(" "))) if row.select_one("dd.views") else None

        last_post_author = None
        last_post_profile = None
        last_post_date = None
        last_post_link = None
        lp = row.select_one("dd.lastpost")
        if lp:
            lp_text = lp.get_text(" ", strip=True)
            for a in lp.find_all("a", href=True):
                ahref = a.get("href", "")
                if "/forum/member" in ahref:
                    last_post_author = _strip(a.get_text())
                    last_post_profile = urljoin(base_url, ahref)
                    break
            for a in lp.find_all("a", href=True):
                ahref = a.get("href", "")
                if "#p" in ahref:
                    last_post_link = urljoin(base_url, ahref)
                    break
            m = re.search(r"on\s+(.+)$", lp_text)
            if m:
                last_post_date = _strip(m.group(1))

        has_attachment = bool(row.find("img", alt=re.compile("Attachment", re.I)))

        out["topics"].append(
            {
                "topic_id": topic_id,
                "slug": _slug_from_topic_url(href),
                "url": href,
                "title": _strip(title_a.get_text(" ", strip=True)),
                "starter_author": starter_name,
                "starter_profile_url": starter_profile,
                "start_date": start_date,
                "replies": replies,
                "views": views,
                "last_post_author": last_post_author,
                "last_post_profile_url": last_post_profile,
                "last_post_date": last_post_date,
                "last_post_link": last_post_link,
                "has_attachment": has_attachment,
            }
        )

    return out


# ---------------------------------------------------------------------------
# Topic page
# ---------------------------------------------------------------------------


def parse_topic_post_count(html: str) -> Optional[int]:
    """Pull ``473`` out of ``473 posts`` from the topic pagination block."""
    m = re.search(r"([\d,]+)\s+posts\b", html, re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


def parse_topic_total_pages(html: str) -> Optional[int]:
    """Pull ``48`` out of ``Page 1 of 48`` on a topic view."""
    m = re.search(
        r"Page\s*<strong>\s*\d+\s*</strong>\s*of\s*<strong>\s*(\d+)\s*</strong>",
        html,
        re.IGNORECASE,
    )
    return int(m.group(1)) if m else None


def parse_topic_title(html: str) -> Optional[str]:
    """First-post title (``<h3 class="first">``)."""
    soup = _soup(html)
    h3 = soup.select_one("h3.first")
    if not h3:
        return None
    a = h3.find("a")
    return _strip(a.get_text() if a else h3.get_text())


def _author_meta_from_profile(dl: Optional[Tag]) -> dict[str, str]:
    """Extract <strong>Key:</strong> <b>Value</b> rows from a postprofile dl."""
    meta: dict[str, str] = {}
    if not dl:
        return meta
    for dd in dl.find_all("dd", recursive=False):
        strong = dd.find("strong")
        bold = dd.find("b")
        if not strong:
            continue
        key = _strip(strong.get_text()).rstrip(":")
        val = _strip(bold.get_text()) if bold else _strip(
            dd.get_text(" ", strip=True).replace(strong.get_text(), "", 1)
        )
        if key:
            meta[key] = val
    return meta


def parse_topic_page(
    html: str,
    *,
    base_url: str = "https://myelomabeacon.org/forum/",
    topic_id: Optional[int] = None,
    post_offset: int = 0,
) -> dict[str, Any]:
    """Parse one topic-view page.

    Returns
    -------
    dict with keys:
        ``topic_id``     : int | None
        ``total_posts``  : int | None  (from pagination block on page 1)
        ``total_pages``  : int | None
        ``post_offsets`` : list[int]   (offsets of every page of this topic)
        ``posts``        : list[dict]
    """
    soup = _soup(html)

    out: dict[str, Any] = {
        "topic_id": topic_id,
        "total_posts": parse_topic_post_count(html),
        "total_pages": parse_topic_total_pages(html),
        "post_offsets": [],
        "posts": [],
    }

    # Find all pagination links → derive every page's post offset for this topic.
    offsets: set[int] = {post_offset}
    for a in soup.select("div.pagination a[href], strong.pagination a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        off = _post_offset_from_href(href)
        if off is not None:
            offsets.add(off)
    out["post_offsets"] = sorted(offsets)

    # Parse each post.
    for post in soup.select("div.post"):
        post_id_attr = post.get("id", "")
        m = re.match(r"p(\d+)", post_id_attr)
        post_id = int(m.group(1)) if m else None

        body = post.select_one("div.postbody")
        title = None
        author = None
        author_profile = None
        posted_at = None
        body_html = ""
        body_text = ""

        if body:
            h3 = body.find(["h3"])
            if h3:
                a = h3.find("a")
                title = _strip(a.get_text() if a else h3.get_text())

            p_author = body.select_one("p.author")
            if p_author:
                a = p_author.find("a", href=True)
                if a:
                    author = _strip(a.get_text())
                    author_profile = urljoin(base_url, a.get("href", ""))
                txt = p_author.get_text(" ", strip=True)
                m = re.search(r"by\s+.+?\s+on\s+(.+?)\s*$", txt)
                if m:
                    posted_at = _strip(m.group(1))

            # Body text/HTML lives in .postcontent2 (custom theme) or .content (vanilla phpBB).
            content = body.select_one("div.postcontent2") or body.select_one("div.content")
            if content:
                body_html = str(content)
                body_text = content.get_text("\n", strip=True)

        profile = post.select_one("dl.postprofile")
        author_meta = _author_meta_from_profile(profile)

        attachment_urls: list[str] = []
        if body:
            for img in body.find_all("img", src=True):
                src = img.get("src", "")
                if not src or src.startswith("./styles/") or "smilies" in src.lower():
                    continue
                attachment_urls.append(urljoin(base_url, src))
            for a in body.find_all("a", href=True):
                ahref = a.get("href", "")
                if "/download/file.php" in ahref or ahref.startswith("./download/"):
                    attachment_urls.append(urljoin(base_url, ahref))

        out["posts"].append(
            {
                "topic_id": topic_id,
                "post_id": post_id,
                "post_offset": post_offset,
                "title": title,
                "author": author,
                "author_profile_url": author_profile,
                "author_meta_json": json.dumps(author_meta, ensure_ascii=False) if author_meta else "",
                "posted_at": posted_at,
                "body_html": body_html,
                "body_text": body_text,
                "attachment_urls_json": json.dumps(attachment_urls, ensure_ascii=False)
                if attachment_urls
                else "",
            }
        )

    return out


__all__ = [
    "parse_index_page",
    "parse_index_total_pages",
    "parse_index_total_topics",
    "parse_topic_page",
    "parse_topic_post_count",
    "parse_topic_title",
    "parse_topic_total_pages",
]
