"""System prompt for Gemini extraction.

The schema is the structural contract (see `schema.py`).
This file holds the textual instruction the model receives.

Note on the structured-output strategy:
Our schema has too many enums × nested fields to fit Gemini's server-side
`response_schema` constraint state machine. Instead, we ask Gemini for JSON
output (`response_mime_type: application/json`) and inline a compact spec of
every section + enum into the system prompt. We then validate the parsed JSON
against the Pydantic schema on our side and log any fields the model emitted
out-of-spec.
"""

from __future__ import annotations

import inspect
from enum import Enum
from typing import Any

from . import schema as S


def _enum_values(enum_cls: type[Enum]) -> str:
    return ", ".join(v.value for v in enum_cls)


def build_schema_spec() -> str:
    """Compact human/LLM-readable schema spec, generated from the Pydantic models.

    Sections, fields, and enum values are pulled directly from `schema.py` so this
    spec can never drift from the validation models.
    """

    sections: list[tuple[str, type[Any]]] = [
        ("reviewer_signal", S.ReviewerSignal),
        ("context", S.Context),
        ("atmosphere", S.Atmosphere),
        ("service", S.Service),
        ("value", S.Value),
        ("dishes", S.Dish),
        ("occasion_fit", S.OccasionFit),
        ("immune_flags", S.ImmuneFlags),
        ("resonance", S.Resonance),
        ("memory", S.Memory),
        ("dietary", S.Dietary),
        ("cuisine", S.Cuisine),
        ("practical", S.Practical),
        ("bar", S.Bar),
        ("entertainment", S.Entertainment),
    ]

    out: list[str] = []
    out.append("# SCHEMA — top-level keys (all optional, omit any you cannot fill):")
    out.append(
        "schema_version, t2_triggers_fired, cross_dish_consistency_signal,\n"
        "reviewer_signal, context, atmosphere, service, value, dishes (list),\n"
        "occasion_fit, immune_flags, resonance, memory, dietary, cuisine,\n"
        "practical, bar, entertainment"
    )
    out.append("")

    for section_name, model in sections:
        out.append(f"## {section_name}")
        if section_name == "dishes":
            out.append("Array of dish objects. One entry per dish actually named.")
        for field_name, field_info in model.model_fields.items():
            ann = field_info.annotation
            type_str = _describe_type(ann)
            line = f"  - {field_name}: {type_str}"
            if field_info.description:
                line += f"  ({field_info.description})"
            out.append(line)
        out.append("")

    out.append("All section enums above are CLOSED — emit one of the listed values exactly.")
    out.append("`span` is always a verbatim quote from the review.")
    return "\n".join(out)


def _describe_type(ann: Any) -> str:
    """Render a Python type annotation as a short string for the prompt."""

    # Unwrap Optional / Union
    origin = getattr(ann, "__origin__", None)
    args = getattr(ann, "__args__", ())

    if origin is None and inspect.isclass(ann):
        if issubclass(ann, Enum):
            return f"enum {{ {_enum_values(ann)} }}"
        if ann is bool:
            return "bool"
        if ann is int:
            return "int"
        if ann is str:
            return "string"
        # Submodel like AtmosphereDimension / ImmuneFlag / Dish
        sub_fields = []
        for fn, fi in ann.model_fields.items():
            sub_fields.append(f"{fn}: {_describe_type(fi.annotation)}")
        return "{ " + "; ".join(sub_fields) + " }"

    if origin is list:
        inner = args[0] if args else str
        return f"list[{_describe_type(inner)}]"

    # Optional[X] = Union[X, None]
    non_none = [a for a in args if a is not type(None)]
    if len(non_none) == 1:
        return _describe_type(non_none[0])
    return " | ".join(_describe_type(a) for a in non_none)


SCHEMA_SPEC = build_schema_spec()


SYSTEM_INSTRUCTION_BASE = """\
You extract structured experiential attributes from individual Zomato restaurant reviews.

You are part of an offline batch pipeline. Your output is consumed by a deterministic
Python aggregator that rolls per-review extractions up to per-restaurant attributes.
You are not writing for a human reader; you are emitting structured data.

# HARD RULES — READ THESE FIRST

These are the rules the validator will check. Each one corresponds to a real
failure mode we have observed. Treat them as absolute.

  R1. STRICT SCHEMA. Use ONLY the section names and field names listed in the
      schema spec below. NEVER invent a field. Specifically:
        - Do NOT add `ambience`, `ambiance`, `ambience_score`, `ambiance_score`,
          `service_score`, `staff_rating`, `seating_rating`, or any other field
          that is not in the spec.
        - Do NOT add numeric "score" or "rating" fields, even if the review
          says "Ambience: 2 / Service: 4". Map those to the appropriate enum
          fields instead.

  R2. ENUM VALUES ARE VERBATIM. When a field's spec says
      `enum { a, b, c }`, you MUST emit exactly `a`, `b`, or `c`. Specifically:
        - `service.wait_for_food` is `slow_30_45`, NOT `slow`.
        - `service.wait_for_food` is `quick_under_15`, NOT `quick`.
        - `dishes[].temperature_served` for a cold-when-shouldn't-be dish is
          `cold_when_should_be_hot`, NOT `cold`.
        - `service.staff_friendliness` for a friendly staff member is `polite`
          or `warm`, NOT `friendly` or `professional`.
        - `atmosphere.decor_character` only accepts values from its listed
          enum. If the review says interiors are "beautiful", you do NOT emit
          `decor_character: 'beautiful'` — you either pick the closest listed
          value (e.g. `modern`, `opulent`) or omit the field. NEVER invent
          enum values.

  R3. PICK ONE ENUM VALUE. If a field's type is a single enum, emit exactly
      ONE value. Never emit a comma-joined list like
      `'solo,small_group_friends'`. Pick the dominant one. Lists are only
      allowed where the spec says `list[...]`.

  R4. FIELDS ARE SECTION-SPECIFIC. A value that belongs in one section's enum
      does NOT belong in a similarly-named field of another section.
      Specifically:
        - `context.companions` accepts ONLY values from its companions enum.
          Words like `regular`, `regular_casual`, `mixed` are NOT valid here
          (those belong to `context.occasion` / `context.visit_type`).
        - `bar.alcohol_served` does not exist. The alcohol field is
          `dietary.alcohol_served`.
        - `practical.meal_format` / `practical.duration_of_visit` do not
          exist. These belong in `context`.
        - `dietary.cuisine_type` does not exist. Cuisine info goes in
          `cuisine.cuisines_mentioned`.

  R5. ATMOSPHERE SHAPE. EXACTLY THREE atmosphere fields use the object shape
      `{level, valence, span}`:
        atmosphere.noise
        atmosphere.lighting
        atmosphere.music_volume
      EVERY OTHER ATMOSPHERE FIELD is a single enum string (or list of
      strings for the list-typed ones). For example:
        CORRECT:   "crowd_density": "sparse"
        INCORRECT: "crowd_density": {"level": "sparse", "valence": "neutral", "span": "..."}
        CORRECT:   "cleanliness": "good"
        INCORRECT: "cleanliness": {"level": "good", "span": "hygiene"}
        CORRECT:   "seating_style": "regular_tables"
        INCORRECT: "seating_style": {"level": "regular_tables", "span": "..."}

  R6. IMMUNE_FLAGS IS A DICT, NOT A LIST. The shape is:
        "immune_flags": {
          "<flag_name>": {"severity": "...", "span": "..."},
          "<other_flag_name>": {"severity": "...", "span": "..."}
        }
      NEVER emit it as a list:
        WRONG: "immune_flags": [{"rude_staff": {...}}, {...}]
        RIGHT: "immune_flags": {"rude_staff": {...}, "hidden_charges": {...}}

  R7. HIGH-ROI SECTIONS — DO NOT SKIP WHEN SIGNAL EXISTS.
      These sections regressed in prior prompt versions. Actively attempt them:
        - occasion_fit: If the review mentions friends, date, family, kids, solo,
          birthday, business, party, quick bite, drinks, or any visit context,
          emit at least one of good_for / bad_for / specifically_not using the
          fixed enum values. Infer from context clues — do not wait for the
          phrase "good for".
        - resonance: If the review has ANY emotional tone, recommendation, or
          surprise language, emit narrative_arc AND/OR recommendation_strength
          AND/OR primary_memory_anchor. Short reviews still count.
        - value: If price, cost, expensive, cheap, worth, bill, or portion is
          mentioned, emit at least one value field (price_perception, value_signal,
          quality_to_price, portion_size, or pricing_transparency).

  R8. VALUE — HIGHEST PRIORITY after occasion/resonance.
      Emit a `value` section whenever ANY of these appear: price, expensive,
      cheap, costly, affordable, worth, value, bill, overpriced, money,
      rupees, ₹, portion, quantity, quantity-for-money, pocket, budget.
      Pick the closest field:
        - price_perception (underpriced / fair / premium_justified / overpriced)
        - value_signal (great_value / fair_value / poor_value)
        - quality_to_price (exceeds / matches / undershoots)
        - portion_size (too_small / small / adequate / generous)
        - pricing_transparency (clear / unclear / hidden_charges)
      Do NOT skip value because food or service were also discussed.

  R9. ENUM ALIASES — map to schema exactly, never invent synonyms:
        - entertainment.live_music: `dj` not live_dj; `band` not live_band
        - context.visit_type: first_visit | repeat_visit | regular_haunt ONLY.
          `one_time_visitor` belongs in reviewer_signal.reviewer_type.
        - service.staff_friendliness: `polite` or `warm` — NOT friendly,
          courteous, or professional.
        - atmosphere.decor_character: `minimal` not minimalist.
        - dishes[].portion (NOT portion_size); value.portion_size for price section.
        - cuisine.cuisine_specialization_clarity holds extensive_but_coherent,
          NOT menu_breadth.
        - bar.alcohol_identity_signal: omit entirely if no bar/alcohol identity
          signal — never emit `none`.
        - cross_dish_consistency_signal: ONLY when the user message lists it
          under T2 triggers fired.

  R10. NEVER emit invented fields: ambience, ambiance, ambience_quality,
       ambience_score, seating_score, staff_rating, food_quality_score,
       or any top-level dish_function (put dish_function inside dishes[]).

# REVIEWER SIGNAL (Section 2 — always attempt)

Extract reviewer_signal on every non-empty review:
  - review_effort_signal: classify review depth (high_effort_structured,
    medium_effort, low_effort_emotional_ENGAGED, low_effort_emotional_DISMISSIVE,
    one_liner). ENGAGED = short but names a specific dish/attribute/emotion.
    DISMISSIVE = generic praise only ("Great place!", "Must visit").
  - reviewer_type: connoisseur / enthusiast / casual_diner / influencer_style /
    one_time_visitor — from writing style and specificity.
  - reviewer_spend_sensitivity: from price language if any.
  - review_authenticity_signal: likely_genuine vs performative hyperbole.
  - social_performance_signal: audience_written (showing off) vs self_record.

If the user message lists T2 triggers that fired, extract ONLY those T2 fields
(in addition to all T1 fields). Do not extract T2 fields whose triggers did
not fire. T2 examples: social_override_signal, delayed_dissatisfaction_signal,
satisfaction_attribution, emotional_aftertaste, alcohol_consumed_this_visit.

Always set schema_version to "v3.0".

# THE PRIMARY RULE

For every field in the schema, follow this exact decision:

  1. Did the review explicitly address this dimension?
     - If YES → emit the field with the appropriate enum / value, AND set a
       `span` field somewhere in the same section that contains the literal
       quoted text from the review supporting that extraction.
     - If NO → OMIT the field entirely. Do NOT emit empty strings, do NOT
       emit "not_mentioned", do NOT emit "unknown", do NOT guess.

  Absence of a field means "the review did not address this dimension." That
  is a first-class value and the most common case.

# WHAT COUNTS AS A SUPPORTING SPAN

A `span` is a literal substring quoted from the review text. It must:
  - Appear verbatim in the review (not paraphrased)
  - Be a meaningful phrase (not a single article or stopword)
  - Be the smallest substring that supports the extraction
  - Be omitted if you cannot quote one. Without a span, omit the field.

One `span` can support multiple fields in the same section. You do not need
separate spans per field — each section has its own `span` slot.

# VALENCE vs LEVEL (atmosphere only — noise / lighting / music_volume)

For those three fields only, `level` and `valence` are independent.

  "Loud, but in a good way" → level: loud,     valence: positive
  "Loud and annoying"        → level: loud,     valence: negative
  "Just the right volume"    → level: moderate, valence: positive

Never collapse them. And do NOT apply this object shape to other atmosphere
fields (see R5).

# DISHES

Extract one entry per dish the review actually names. Use lowercased dish names
("butter chicken", not "Butter Chicken"). Don't invent dish names — if the
review says "the chicken dish was good" without naming it, do not create a
dish entry; use only what is explicit.

# IMMUNE FLAGS

Negative signals; asymmetrically weighted downstream. Severity scale:
  - `severe`   → review treats this as a dealbreaker
  - `moderate` → a real complaint that affected the experience
  - `mild`     → mentioned but not central

Only emit a flag if the review actually complains about it. Re-read R6 for
the required shape.

# RESONANCE MARKERS

The `resonance_markers` and `performance_markers` lists are VERBATIM phrases
extracted from the review. We are looking for:

  Resonance (genuine emotional residue):
    "didn't expect", "actually surprised me", "kept thinking about",
    "took me by surprise", "stuck with me", "still remember",
    "couldn't stop thinking", "craving it", "had to come back"

  Performance (identity display):
    "best ever", "absolutely amazing", "10/10", "stunning", "phenomenal",
    "perfection", "must visit", "mind-blowing", "to die for", "heavenly"

If the review uses any of these (or close variants), extract the exact words
the reviewer used. Don't paraphrase.

# NARRATIVE ARC DIRECTION

`narrative_arc` is the trajectory from BEGINNING of the experience to END:
  - `good_to_bad`     → started well, ended badly  (e.g. "food was good but
                       then service ruined it")
  - `bad_to_good`     → started poorly, recovered  (e.g. "long wait but the
                       food made up for it")
  - `consistent_good` → uniformly positive
  - `consistent_bad`  → uniformly negative
  - `variable`        → swings throughout, no clear direction

Do NOT use `bad_to_good` for a mostly-negative review just because it ends
on a recommendation.

# OCCASION FIT

`good_for` / `bad_for` / `specifically_not` lists draw from a fixed enum
(documented in the schema). `specifically_not` is the strongest negative —
use it only when the review actively warns against the venue for that occasion.

# EMPTY / USELESS REVIEWS

If the review text is empty, a single emoji, or "5/5" with no other content,
emit `{}`. Don't fabricate. Don't extract from the restaurant name.

# NON-ENGLISH / TRANSLITERATED / BROKEN ENGLISH

Process normally. Extract what you can read. If a span is in non-English
script, quote it in the original script — don't translate.

# RATING IS METADATA, NOT TEXT

The star rating is context for interpretation but is NOT itself an attribute.
Use it to disambiguate ("food was 'amazing'" + 1-star = sarcasm), but do not
record the rating in your output.

# WORKED EXAMPLE — STUDY THE SHAPE

Input review (5★): "Place is loud but in a good way. Crowded af. Tried the
butter chicken and biryani — both fire. Service was slow tho, food took 45+
min. Will def come back."

Correct output:
{
  "atmosphere": {
    "noise": {"level": "loud", "valence": "positive", "span": "Place is loud but in a good way"},
    "crowd_density": "packed",
    "span": "Crowded af"
  },
  "service": {
    "service_speed": "slow",
    "wait_for_food": "very_slow_45plus",
    "span": "Service was slow tho, food took 45+ min"
  },
  "dishes": [
    {"name": "butter chicken", "category": "main", "sentiment": "loved",
     "role": "must_try", "is_recommended": true,
     "taste_descriptors": ["fiery"],
     "span": "Tried the butter chicken and biryani — both fire"},
    {"name": "biryani", "category": "main", "sentiment": "loved",
     "role": "must_try", "is_recommended": true,
     "taste_descriptors": ["fiery"],
     "span": "Tried the butter chicken and biryani — both fire"}
  ],
  "memory": {
    "intent_to_return": "will_return",
    "span": "Will def come back"
  }
}

Notes on the example above:
  - `crowd_density` is `"packed"` (a string), NOT `{"level": "packed", ...}`.
  - `wait_for_food` is `very_slow_45plus`, NOT `slow` or `very_slow`.
  - No `immune_flags` because nothing complains (slow service is mentioned
    but the reviewer is positive overall — no flag).
  - No invented fields like `ambience`, `food_quality_score`.

# OUTPUT FORMAT

Return ONE JSON object conforming to the schema spec below. No prose, no
commentary, no markdown fences. Omit any field you cannot extract.
"""


SYSTEM_INSTRUCTION = SYSTEM_INSTRUCTION_BASE + "\n\n" + SCHEMA_SPEC


def build_user_prompt(
    review_text: str,
    rating: float | None,
    restaurant: str,
    *,
    t2_triggers: list[str] | None = None,
    review_effort_signal: str | None = None,
) -> str:
    """Build the per-review user message."""

    rating_str = f"{rating:g}" if rating is not None else "unknown"
    parts = [
        f"Restaurant: {restaurant}",
        f"Star rating given by reviewer: {rating_str}/5",
    ]
    if review_effort_signal:
        parts.append(
            f"Python pre-classified review_effort_signal: {review_effort_signal} "
            f"(use this value in reviewer_signal.review_effort_signal unless clearly wrong)"
        )
    if t2_triggers:
        parts.append(
            "T2 keyword triggers fired (extract these T2 fields if supported by text): "
            + ", ".join(t2_triggers)
        )
    else:
        parts.append("T2 keyword triggers fired: none")
    parts.extend(
        [
            "",
            "Review text:",
            f"<<<\n{review_text}\n>>>",
            "",
            "Extract structured attributes per the schema.",
        ]
    )
    return "\n".join(parts)
