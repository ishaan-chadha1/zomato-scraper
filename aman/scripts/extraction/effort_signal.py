"""Deterministic review_effort_signal — zero LLM cost.

Classifies review text before extraction. Python result overrides any LLM
guess for review_effort_signal (authoritative pre-processing layer).
"""

from __future__ import annotations

import re

_DISMISSIVE_PATTERN = (
    r"^(great|good|nice|awesome|amazing|best|perfect|ok|okay|fine|bad|worst|"
    r"terrible|horrible|loved it|hated it|must visit|must try|not good|"
    r"don't go|do not go|never again|5/5|4/5|3/5|2/5|1/5)[\s!.]*$"
)

_ENGAGED_PATTERN = (
    r"\b(dal|biryani|dosa|idli|paneer|chicken|fish|mutton|thali|coffee|"
    r"ambience|ambiance|service|staff|wait|bill|price|portion|crispy|"
    r"spicy|home|nostalg|surpris|disappoint|rude|slow|crowd|noise|"
    r"birthday|date|family|friends|anniversary)\b"
)


def classify_review_effort(text: str) -> str:
    """Return review_effort_signal enum value."""

    raw = (text or "").strip()
    if not raw:
        return "one_liner"

    alpha = re.sub(r"[^\w\s]", "", raw, flags=re.UNICODE).strip()
    if not alpha:
        return "one_liner"

    words = raw.split()
    n_words = len(words)
    n_chars = len(raw)

    sentences = re.split(r"[.!?]+", raw)
    n_sentences = len([s for s in sentences if s.strip()])

    if n_words >= 80 or n_sentences >= 4 or n_chars >= 400:
        return "high_effort_structured"

    if n_words >= 25 or n_sentences >= 2:
        return "medium_effort"

    if n_words <= 8:
        lower = raw.lower().strip()
        if re.match(_DISMISSIVE_PATTERN, lower, re.IGNORECASE):
            return "low_effort_emotional_DISMISSIVE"
        if re.search(_ENGAGED_PATTERN, lower, re.IGNORECASE):
            return "low_effort_emotional_ENGAGED"
        if n_words <= 3:
            return "one_liner"
        return "low_effort_emotional_DISMISSIVE"

    if re.search(_ENGAGED_PATTERN, raw.lower()):
        return "low_effort_emotional_ENGAGED"
    return "medium_effort"
