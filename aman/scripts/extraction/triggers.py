"""Hard keyword triggers for Tier-2 extraction fields.

Python scans review text; LLM only extracts T2 fields when triggers fire.
Do NOT let the model decide whether triggers are "sufficient".
"""

from __future__ import annotations

import re
from typing import Iterable

# field_name -> list of regex patterns (case-insensitive)
T2_TRIGGERS: dict[str, list[str]] = {
    "alcohol_consumed_this_visit": [
        r"\b(beer|wine|cocktail|whisky|whiskey|vodka|rum|gin|peg|shot|shots|"
        r"sundowner|bar|drink|drinks|tipsy|drunk|buzzed|high on)\b",
    ],
    "ordering_risk_tolerance": [
        r"\b(first time|never tried|always order|my regular|usual order|"
        r"stick to|adventur|experiment|try something new|something different)\b",
    ],
    "sensory_load": [
        r"\b(overwhelm|overwhelming|calming|calm atmosphere|stimulat|"
        r"exhausting|peaceful|chaotic|sensory)\b",
    ],
    "social_override_signal": [
        r"\b(company made|friends made|had so much fun|great time despite|"
        r"wouldn't come for food alone|would not come for food alone|"
        r"atmosphere of the place|vibe was great)\b",
    ],
    "delayed_dissatisfaction_signal": [
        r"\b(food was great but|enjoyed until|would have been perfect if|"
        r"good but the service|good but service|loved the food but)\b",
    ],
    "satisfaction_attribution": [
        r"\b(because of the company|company made it|ambience made it|"
        r"food was the star|atmosphere made|price made it|value made)\b",
    ],
    "emotional_aftertaste": [
        r"\b(left feeling|went home feeling|still feel|came away feeling|"
        r"days later|felt heavy|felt light|felt drained|felt energized)\b",
    ],
    "recovery_quality": [
        r"\b(needed this|decompression|recharged|draining|exhausted after|"
        r"restorative|wind down|stress relief)\b",
    ],
    "dish_function": [
        r"\b(go-to|go to|like home|comfort food|comfort meal|signature dish|"
        r"had to try|for the photo|instagram|show off|impress)\b",
    ],
    "delivery_quality_signal": [
        r"\b(delivery|delivered|packaging|leaked|soggy|cold when|"
        r"held well|in transit|zomato order|swiggy)\b",
    ],
    "alcohol_identity_signal": [
        r"\b(craft beer|cocktail bar|wine list|whisky|nightlife|"
        r"after work drinks|sundowner|pub|brewery)\b",
    ],
    "bar_quality_signal": [
        r"\b(bar|cocktail|drinks menu|bartender|happy hour|peg|pitcher)\b",
    ],
    "visit_day": [
        r"\b(weekend|weekday|monday|tuesday|wednesday|thursday|friday|"
        r"saturday|sunday)\b",
    ],
    "review_language": [
        r"[\u0900-\u097F]",  # Devanagari
        r"[\u0C80-\u0CFF]",  # Kannada
        r"[\u0B80-\u0BFF]",  # Tamil
    ],
}


def scan_triggers(text: str, extra_fields: Iterable[str] | None = None) -> list[str]:
    """Return sorted list of T2 field names whose keyword triggers match."""

    raw = text or ""
    fired: set[str] = set()
    for field, patterns in T2_TRIGGERS.items():
        for pat in patterns:
            if re.search(pat, raw, re.IGNORECASE):
                fired.add(field)
                break
    if extra_fields:
        fired.update(extra_fields)
    return sorted(fired)


def cross_dish_consistency_trigger(text: str) -> bool:
    """True when review likely mentions 2+ dishes with contrasting sentiment."""

    lower = text.lower()
    dish_words = len(re.findall(
        r"\b(biryani|dosa|idli|paneer|chicken|mutton|fish|thali|pizza|"
        r"pasta|burger|roll|momos|noodles|curry|dal|roti|naan|dessert|"
        r"starter|main|appetizer)\b",
        lower,
    ))
    contrast = bool(re.search(
        r"\b(but|however|whereas|while|unlike|except|only the|rest was)\b",
        lower,
    ))
    return dish_words >= 2 and contrast
