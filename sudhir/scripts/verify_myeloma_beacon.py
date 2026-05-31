#!/usr/bin/env python3
"""Verify the Myeloma Beacon scrape outputs end-to-end.

Read-only audit: compares ``posts.csv`` against ``topics.csv``,
``.completed_topics.txt`` and ``.failed_topics.txt``. Writes a
machine-readable JSON report and a human-readable console summary.

Exits non-zero if any check fails.

Usage::

    python3 scripts/verify_myeloma_beacon.py
    python3 scripts/verify_myeloma_beacon.py --data-dir data/MyelomaBeacon/multiple-myeloma
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd


PHPBB_DATE_RE = re.compile(
    r"^[A-Z][a-z]{2}\s+[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}\s+(?:am|pm)$"
)


def _green(s: str) -> str:
    return f"\033[32m{s}\033[0m"


def _red(s: str) -> str:
    return f"\033[31m{s}\033[0m"


def _yellow(s: str) -> str:
    return f"\033[33m{s}\033[0m"


def _row(name: str, status: str, detail: str = "") -> str:
    color = {"PASS": _green, "FAIL": _red, "WARN": _yellow, "INFO": lambda x: x}[status]
    return f"  {color(status):<14} {name:<48} {detail}"


def load_inputs(data_dir: Path) -> dict[str, Any]:
    topics = pd.read_csv(data_dir / "topics.csv")
    posts = pd.read_csv(data_dir / "posts.csv", low_memory=False)

    completed_path = data_dir / ".completed_topics.txt"
    failed_path = data_dir / ".failed_topics.txt"

    completed = set()
    if completed_path.exists():
        for line in completed_path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s.isdigit():
                completed.add(int(s))

    failed: dict[int, str] = {}
    if failed_path.exists():
        for line in failed_path.read_text(encoding="utf-8").splitlines():
            parts = line.strip().split("\t", 2)
            if parts and parts[0].isdigit():
                failed[int(parts[0])] = parts[1] if len(parts) > 1 else ""

    return {
        "topics": topics,
        "posts": posts,
        "completed": completed,
        "failed": failed,
    }


def check_per_topic_counts(
    topics: pd.DataFrame, posts: pd.DataFrame, failed: dict[int, str]
) -> dict[str, Any]:
    """Per-topic: expected = replies + 1 == actual posts captured."""
    posts_per_topic = posts.groupby("topic_id").size().to_dict()
    mismatches: list[dict[str, Any]] = []
    matched = 0
    for row in topics.itertuples(index=False):
        tid = int(row.topic_id)
        if tid in failed:
            continue
        replies = row.replies
        try:
            replies_int = int(replies) if pd.notna(replies) else None
        except (TypeError, ValueError):
            replies_int = None
        if replies_int is None:
            continue
        expected = replies_int + 1
        actual = int(posts_per_topic.get(tid, 0))
        if actual == expected:
            matched += 1
        else:
            mismatches.append(
                {
                    "topic_id": tid,
                    "title": row.title,
                    "expected_posts": expected,
                    "actual_posts": actual,
                    "delta": actual - expected,
                    "url": row.url,
                }
            )
    return {
        "name": "per_topic_counts",
        "passed": len(mismatches) == 0,
        "checked": matched + len(mismatches),
        "matched": matched,
        "mismatches": mismatches,
    }


def check_global_post_id_uniqueness(posts: pd.DataFrame) -> dict[str, Any]:
    counts = posts["post_id"].value_counts()
    dupes = counts[counts > 1]
    return {
        "name": "global_post_id_uniqueness",
        "passed": len(dupes) == 0,
        "total_posts": int(len(posts)),
        "unique_post_ids": int(posts["post_id"].nunique()),
        "duplicate_count": int(len(dupes)),
        "examples": [
            {"post_id": int(pid), "occurrences": int(n)}
            for pid, n in dupes.head(20).items()
        ],
    }


def check_per_topic_post_id_uniqueness(posts: pd.DataFrame) -> dict[str, Any]:
    g = posts.groupby("topic_id")["post_id"]
    dup_topics: list[dict[str, Any]] = []
    for tid, ser in g:
        counts = ser.value_counts()
        d = counts[counts > 1]
        if len(d) > 0:
            dup_topics.append(
                {
                    "topic_id": int(tid),
                    "duplicate_post_ids": [int(x) for x in d.index.tolist()],
                }
            )
    return {
        "name": "per_topic_post_id_uniqueness",
        "passed": len(dup_topics) == 0,
        "topics_with_dupes": len(dup_topics),
        "examples": dup_topics[:20],
    }


def check_coverage(
    topics: pd.DataFrame, completed: set[int], failed: dict[int, str]
) -> dict[str, Any]:
    """Every topic in topics.csv should be in completed ∪ failed."""
    all_topic_ids = set(int(x) for x in topics["topic_id"].tolist())
    accounted = completed | set(failed.keys())
    unaccounted = sorted(all_topic_ids - accounted)
    extra_completed = sorted(completed - all_topic_ids)
    return {
        "name": "coverage",
        "passed": len(unaccounted) == 0,
        "topics_total": len(all_topic_ids),
        "topics_completed": len(completed),
        "topics_failed": len(failed),
        "unaccounted_count": len(unaccounted),
        "unaccounted_examples": unaccounted[:20],
        "extra_completed_count": len(extra_completed),
        "extra_completed_examples": extra_completed[:20],
    }


def check_position_contiguity(posts: pd.DataFrame) -> dict[str, Any]:
    """Within each topic, position_in_topic should run 0..N-1 with no gaps."""
    bad: list[dict[str, Any]] = []
    for tid, group in posts.groupby("topic_id"):
        positions = sorted(int(x) for x in group["position_in_topic"].tolist())
        expected = list(range(len(positions)))
        if positions != expected:
            bad.append(
                {
                    "topic_id": int(tid),
                    "n_posts": len(positions),
                    "first_5_positions": positions[:5],
                    "last_5_positions": positions[-5:],
                }
            )
    return {
        "name": "position_contiguity",
        "passed": len(bad) == 0,
        "topics_with_gaps": len(bad),
        "examples": bad[:20],
    }


def check_body_sanity(posts: pd.DataFrame) -> dict[str, Any]:
    """Empty body_text is rare-but-possible (deleted post). Just count."""
    body = posts["body_text"].fillna("")
    empty = int((body.str.len() == 0).sum())
    return {
        "name": "body_sanity",
        "passed": True,  # informational only
        "total_posts": int(len(posts)),
        "empty_bodies": empty,
        "empty_pct": round(100 * empty / max(len(posts), 1), 3),
    }


def check_date_parseability(posts: pd.DataFrame) -> dict[str, Any]:
    """Verify phpBB date strings match the expected format."""
    series = posts["posted_at"].fillna("")
    matched = int(series.str.match(PHPBB_DATE_RE).fillna(False).sum())
    total = int(len(series))
    failed = total - matched
    bad_examples: list[str] = []
    if failed:
        bad_mask = ~series.str.match(PHPBB_DATE_RE).fillna(False)
        bad_examples = (
            series[bad_mask].head(10).astype(str).tolist()
        )
    return {
        "name": "date_parseability",
        "passed": failed == 0,
        "total_posts": total,
        "parseable": matched,
        "unparseable": failed,
        "unparseable_pct": round(100 * failed / max(total, 1), 3),
        "examples": bad_examples,
    }


def write_console_summary(report: dict[str, Any]) -> int:
    print(f"\n{'=' * 70}")
    print("  Myeloma Beacon scrape verification")
    print(f"{'=' * 70}")
    print(_row("posts.csv rows", "INFO", f"{report['inputs']['posts_rows']}"))
    print(_row("topics.csv rows", "INFO", f"{report['inputs']['topics_rows']}"))
    print(_row("completed topics", "INFO", f"{report['inputs']['completed']}"))
    print(_row("failed topics", "INFO", f"{report['inputs']['failed']}"))
    print(f"{'-' * 70}")

    failures = 0
    for c in report["checks"]:
        name = c["name"]
        status = "PASS" if c["passed"] else "FAIL"
        if name == "per_topic_counts":
            detail = f"matched {c['matched']}/{c['checked']} topics"
            if c["mismatches"]:
                detail += f", {len(c['mismatches'])} mismatch(es)"
        elif name == "global_post_id_uniqueness":
            detail = f"{c['unique_post_ids']}/{c['total_posts']} unique"
        elif name == "per_topic_post_id_uniqueness":
            detail = f"{c['topics_with_dupes']} topic(s) with duplicate post_ids"
        elif name == "coverage":
            detail = (
                f"{c['topics_completed']} completed + {c['topics_failed']} failed "
                f"= {c['topics_completed'] + c['topics_failed']}/{c['topics_total']}"
            )
        elif name == "position_contiguity":
            detail = f"{c['topics_with_gaps']} topic(s) with positional gaps"
        elif name == "body_sanity":
            detail = f"{c['empty_bodies']} empty body(s) ({c['empty_pct']}%)"
            status = "INFO" if c["empty_bodies"] else "PASS"
        elif name == "date_parseability":
            detail = (
                f"{c['parseable']}/{c['total_posts']} parseable "
                f"({100 - c['unparseable_pct']:.2f}%)"
            )
        else:
            detail = ""
        print(_row(name, status, detail))
        if not c["passed"] and status == "FAIL":
            failures += 1

    print(f"{'=' * 70}")
    if failures:
        print(_row("OVERALL", "FAIL", f"{failures} check(s) failed"))
    else:
        print(_row("OVERALL", "PASS", "all checks green"))
    print(f"{'=' * 70}\n")
    return failures


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("Usage::")[0].strip())
    p.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data") / "MyelomaBeacon" / "multiple-myeloma",
        help="Scrape output directory (default: data/MyelomaBeacon/multiple-myeloma)",
    )
    p.add_argument(
        "--report",
        type=Path,
        default=Path("analysis") / "myeloma_beacon" / "verification.json",
        help="Where to write the JSON report",
    )
    return p


def main() -> int:
    args = build_argparser().parse_args()
    inp = load_inputs(args.data_dir)
    topics = inp["topics"]
    posts = inp["posts"]
    completed = inp["completed"]
    failed = inp["failed"]

    checks = [
        check_per_topic_counts(topics, posts, failed),
        check_global_post_id_uniqueness(posts),
        check_per_topic_post_id_uniqueness(posts),
        check_coverage(topics, completed, failed),
        check_position_contiguity(posts),
        check_body_sanity(posts),
        check_date_parseability(posts),
    ]

    report = {
        "inputs": {
            "data_dir": str(args.data_dir),
            "posts_rows": int(len(posts)),
            "topics_rows": int(len(topics)),
            "completed": len(completed),
            "failed": len(failed),
        },
        "checks": checks,
    }

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")

    failures = write_console_summary(report)
    print(f"  wrote {args.report}\n")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
