"""Tier registry for ARC extraction schema v3."""

from __future__ import annotations

SCHEMA_VERSION = "v3.0"

# Tier 1 — always extract (core ~50 fields across sections)
T1_SECTIONS = (
    "reviewer_signal",
    "context",
    "atmosphere",
    "service",
    "value",
    "dishes",
    "occasion_fit",
    "immune_flags",
    "resonance",
    "memory",
    "dietary",
    "cuisine",
    "practical",
)

T1_REVIEWER_SIGNAL = (
    "review_effort_signal",
    "reviewer_type",
    "reviewer_spend_sensitivity",
    "review_authenticity_signal",
    "social_performance_signal",
)

T1_CONTEXT = (
    "companions",
    "occasion",
    "arrival_state",
    "visit_type",
    "meal_format",
    "visit_time",
    "time_pressure",
)

T1_RESONANCE = (
    "narrative_arc",
    "emotional_lingering",
    "primary_memory_anchor",
    "expectations_vs_reality",
    "recommendation_strength",
)

T1_VALUE = (
    "price_perception",
    "value_signal",
    "quality_to_price",
    "portion_size",
    "pricing_transparency",
)

T1_OCCASION_FIT = ("good_for", "bad_for", "specifically_not")

# Tier 2 — conditional (see triggers.py)
T2_FIELDS = (
    "alcohol_consumed_this_visit",
    "ordering_risk_tolerance",
    "sensory_load",
    "social_override_signal",
    "delayed_dissatisfaction_signal",
    "satisfaction_attribution",
    "emotional_aftertaste",
    "recovery_quality",
    "dish_function",
    "delivery_quality_signal",
    "cross_dish_consistency_signal",
    "alcohol_identity_signal",
    "bar_quality_signal",
    "visit_day",
    "review_language",
)

# Tier 3 — sampled separately (not in default per-review prompt)
T3_FIELDS = (
    "stated_vs_observed_gap",
    "relational_anchor_signal",
    "ritualization_potential",
    "regret_curve_type",
    "sports_energy_modifier",
    "temporal_identity_shift_event",
    "dress_code_noted",
    "non_alcoholic_options",
    "spend_per_head_inr",
    "weather_causal_mention",
)
