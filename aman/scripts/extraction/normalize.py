"""Post-process LLM JSON before Pydantic validation (v2.2 polish)."""

from __future__ import annotations

from typing import Any


def _map_enum(obj: dict, key: str, mapping: dict[str, str]) -> None:
    if key in obj and isinstance(obj[key], str):
        obj[key] = mapping.get(obj[key], obj[key])


def normalize_extraction(parsed: dict, t2_triggers: list[str]) -> dict:
    """Fix common enum aliases and strip fields that violate tier rules."""

    if not parsed or not isinstance(parsed, dict):
        return parsed

    if "cross_dish_consistency_signal" in parsed:
        if "cross_dish_consistency_signal" not in t2_triggers:
            del parsed["cross_dish_consistency_signal"]

    ent = parsed.get("entertainment")
    if isinstance(ent, dict):
        _map_enum(
            ent,
            "live_music",
            {
                "live_dj": "dj",
                "live_band": "band",
                "live_acoustic": "acoustic",
                "variable": "none",
                "background": "none",
            },
        )

    ctx = parsed.get("context")
    if isinstance(ctx, dict):
        if ctx.get("visit_type") == "one_time_visitor":
            rs = parsed.setdefault("reviewer_signal", {})
            if isinstance(rs, dict) and not rs.get("reviewer_type"):
                rs["reviewer_type"] = "one_time_visitor"
            del ctx["visit_type"]

    svc = parsed.get("service")
    if isinstance(svc, dict):
        _map_enum(
            svc,
            "staff_friendliness",
            {"friendly": "warm", "courteous": "polite", "professional": "polite"},
        )

    atm = parsed.get("atmosphere")
    if isinstance(atm, dict):
        _map_enum(atm, "decor_character", {"minimalist": "minimal"})
        _map_enum(atm, "seating_comfort", {"comfortable": "acceptable"})
        _map_enum(
            atm,
            "music_type",
            {"live_tabla": "live_acoustic", "live_dj": "live_dj"},
        )
        for junk in (
            "ambience",
            "ambiance",
            "ambience_quality",
            "ambience_signal",
            "ambience_comment",
            "ambiance_score",
            "ambience_score",
            "seating_score",
        ):
            atm.pop(junk, None)

    val = parsed.get("value")
    if isinstance(val, dict):
        if val.get("portion_size") == "moderate":
            val["portion_size"] = "adequate"
        if val.get("portion_size") == "sufficient":
            val["portion_size"] = "adequate"

    bar = parsed.get("bar")
    if isinstance(bar, dict) and bar.get("alcohol_identity_signal") == "none":
        del bar["alcohol_identity_signal"]
        if not bar:
            parsed.pop("bar", None)

    cuisine = parsed.get("cuisine")
    if isinstance(cuisine, dict):
        if cuisine.get("menu_breadth") == "extensive_but_coherent":
            cuisine["cuisine_specialization_clarity"] = "extensive_but_coherent"
            del cuisine["menu_breadth"]

    dishes = parsed.get("dishes")
    if isinstance(dishes, list):
        for dish in dishes:
            if not isinstance(dish, dict):
                continue
            if "portion_size" in dish and "portion" not in dish:
                dish["portion"] = dish.pop("portion_size")
            _map_enum(
                dish,
                "portion",
                {"sufficient": "adequate", "moderate": "adequate"},
            )
            _map_enum(dish, "presentation", {"classic": "standard"})
            if "dish_function" in dish:
                fn = dish.pop("dish_function")
                if fn and "dish_function" not in dish:
                    dish["dish_function"] = fn

    rs = parsed.get("reviewer_signal")
    if isinstance(rs, dict):
        _map_enum(
            rs,
            "reviewer_type",
            {"business_traveler": "casual_diner", "food_blogger": "influencer_style"},
        )

    cds = parsed.get("cross_dish_consistency_signal")
    if isinstance(cds, str):
        _map_enum(
            parsed,
            "cross_dish_consistency_signal",
            {
                "consistent_good": "consistent_across_dishes",
                "consistent": "consistent_across_dishes",
                "inconsistent": "mixed_performance",
            },
        )

    return parsed
