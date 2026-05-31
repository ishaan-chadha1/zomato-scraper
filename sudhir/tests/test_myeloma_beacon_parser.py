"""Parser tests for the Myeloma Beacon scraper.

Runs against captured fixture HTML in ``tests/fixtures/myeloma_beacon/``.
Run: ``python3 -m pytest tests/test_myeloma_beacon_parser.py -v``
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scrapers.myeloma_beacon.parser import parse_index_page, parse_topic_page

FIXTURES = ROOT / "tests" / "fixtures" / "myeloma_beacon"


@pytest.fixture(scope="module")
def index_html() -> str:
    return (FIXTURES / "index_p1.html").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def topic_html() -> str:
    return (FIXTURES / "topic_t1002_p1.html").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Index page
# ---------------------------------------------------------------------------


def test_index_totals(index_html: str) -> None:
    parsed = parse_index_page(index_html)
    assert parsed["total_topics"] == 2776
    assert parsed["total_pages"] == 112


def test_index_has_25_topics(index_html: str) -> None:
    parsed = parse_index_page(index_html)
    assert len(parsed["topics"]) == 25


def test_index_known_topic_present(index_html: str) -> None:
    parsed = parse_index_page(index_html)
    by_id = {t["topic_id"]: t for t in parsed["topics"]}

    assert 1002 in by_id, "Biking topic (t1002) should appear on page 1"
    biking = by_id[1002]
    assert biking["title"] == "Biking with multiple myeloma"
    assert biking["starter_author"] == "Ron Harvot"
    assert biking["replies"] == 472
    assert biking["views"] == 80784
    assert biking["url"].endswith("biking-with-multiple-myeloma-t1002.html")
    assert biking["slug"] == "biking-with-multiple-myeloma"
    assert biking["has_attachment"] is True
    assert "Apr 19, 2012" in (biking["start_date"] or "")


def test_index_topic_without_attachment(index_html: str) -> None:
    parsed = parse_index_page(index_html)
    by_id = {t["topic_id"]: t for t in parsed["topics"]}

    assert 10740 in by_id  # Kappa light chain
    kappa = by_id[10740]
    assert kappa["title"].startswith("Kappa light chain")
    assert kappa["starter_author"] == "MrPotatohead"
    assert kappa["replies"] == 3
    assert kappa["views"] == 2133
    assert kappa["has_attachment"] is False


def test_index_topic_ids_all_positive(index_html: str) -> None:
    parsed = parse_index_page(index_html)
    for t in parsed["topics"]:
        assert isinstance(t["topic_id"], int) and t["topic_id"] > 0
        assert t["url"].startswith("https://myelomabeacon.org/forum/")


# ---------------------------------------------------------------------------
# Topic page
# ---------------------------------------------------------------------------


def test_topic_totals(topic_html: str) -> None:
    parsed = parse_topic_page(topic_html, topic_id=1002, post_offset=0)
    assert parsed["total_posts"] == 473
    assert parsed["total_pages"] == 48


def test_topic_post_offsets_cover_full_range(topic_html: str) -> None:
    parsed = parse_topic_page(topic_html, topic_id=1002, post_offset=0)
    offsets = parsed["post_offsets"]
    # We expect at least 0, 10, 20, 30, 40 and the last page 470 from the visible pagination.
    for required in (0, 10, 20, 30, 40, 470):
        assert required in offsets, f"missing offset {required}; got {offsets}"


def test_topic_has_posts(topic_html: str) -> None:
    parsed = parse_topic_page(topic_html, topic_id=1002, post_offset=0)
    posts = parsed["posts"]
    assert len(posts) >= 8, "should parse ~10 posts on page 1"


def test_topic_first_post_fields(topic_html: str) -> None:
    parsed = parse_topic_page(topic_html, topic_id=1002, post_offset=0)
    first = parsed["posts"][0]

    assert first["post_id"] == 4785
    assert first["title"] == "Biking with multiple myeloma"
    assert first["author"] == "Ron Harvot"
    assert first["author_profile_url"].endswith("/forum/member1176.html")
    assert "Apr 19, 2012" in (first["posted_at"] or "")
    assert "Most of the posts on this site" in (first["body_text"] or "")
    assert "<br" in (first["body_html"] or "").lower()

    meta = json.loads(first["author_meta_json"])
    assert meta.get("Name") == "Ron Harvot"
    assert meta.get("Age at diagnosis") == "56"
    assert meta.get("When were you/they diagnosed?") == "Feb 2009"


def test_topic_second_post_is_reply(topic_html: str) -> None:
    parsed = parse_topic_page(topic_html, topic_id=1002, post_offset=0)
    second = parsed["posts"][1]
    assert second["post_id"] == 4790
    assert second["author"] == "lys2012"
    assert second["title"].startswith("Re: ")
