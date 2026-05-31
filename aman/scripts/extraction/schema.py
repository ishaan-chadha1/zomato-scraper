"""Pydantic response schema for Gemini structured extraction.

All fields are Optional. The LLM MUST omit any field it cannot extract
(rather than emitting a `not_mentioned` value). Every non-null extracted
field MUST include a `span` that is a literal quote from the review.

This file is the source of truth for the extraction shape. The full
human-readable spec lives in `aman/extraction_schema.md`.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Shared enums
# ---------------------------------------------------------------------------


class Valence(str, Enum):
    positive = "positive"
    neutral = "neutral"
    negative = "negative"
    mixed = "mixed"


class Severity(str, Enum):
    mild = "mild"
    moderate = "moderate"
    severe = "severe"


# ---------------------------------------------------------------------------
# 1. CONTEXT
# ---------------------------------------------------------------------------


class Companions(str, Enum):
    solo = "solo"
    couple_romantic = "couple_romantic"
    couple_casual = "couple_casual"
    small_group_friends = "small_group_friends"
    small_group_family = "small_group_family"
    large_group_friends = "large_group_friends"
    large_group_family = "large_group_family"
    family_with_young_kids = "family_with_young_kids"
    family_with_teens = "family_with_teens"
    family_with_elders = "family_with_elders"
    mixed_generations = "mixed_generations"
    business_colleagues = "business_colleagues"
    business_client = "business_client"
    business_team = "business_team"


class Occasion(str, Enum):
    regular_casual = "regular_casual"
    first_date = "first_date"
    nth_date = "nth_date"
    proposal = "proposal"
    anniversary = "anniversary"
    birthday_self = "birthday_self"
    birthday_other = "birthday_other"
    engagement = "engagement"
    hen_bachelor = "hen_bachelor"
    farewell = "farewell"
    homecoming = "homecoming"
    promotion = "promotion"
    business_pitch = "business_pitch"
    business_team_meal = "business_team_meal"
    kitty_party = "kitty_party"
    office_party = "office_party"
    kids_birthday = "kids_birthday"
    festival_meal = "festival_meal"
    pre_movie = "pre_movie"
    post_movie = "post_movie"
    pre_concert = "pre_concert"
    cheat_meal = "cheat_meal"
    midweek_treat = "midweek_treat"
    weekend_indulgence = "weekend_indulgence"
    family_gathering = "family_gathering"


class VisitTime(str, Enum):
    breakfast = "breakfast"
    brunch = "brunch"
    lunch = "lunch"
    hi_tea = "hi_tea"
    early_dinner = "early_dinner"
    dinner = "dinner"
    late_night = "late_night"
    drinks_pre_dinner = "drinks_pre_dinner"
    drinks_post_dinner = "drinks_post_dinner"


class VisitDay(str, Enum):
    weekday = "weekday"
    weekend = "weekend"


class VisitType(str, Enum):
    first_visit = "first_visit"
    repeat_visit = "repeat_visit"
    regular_haunt = "regular_haunt"


class MealFormat(str, Enum):
    dine_in = "dine_in"
    takeaway = "takeaway"
    delivery = "delivery"
    drive_through = "drive_through"
    private_dining = "private_dining"
    chef_table = "chef_table"
    events_or_catering = "events_or_catering"


class DurationOfVisit(str, Enum):
    quick_under_30 = "quick_under_30"
    standard_30_90 = "standard_30_90"
    leisurely_90plus = "leisurely_90plus"


class TimePressure(str, Enum):
    relaxed = "relaxed"
    time_constrained = "time_constrained"


class ArrivalState(str, Enum):
    celebratory = "celebratory"
    casual = "casual"
    stressed = "stressed"
    very_hungry = "very_hungry"
    tired = "tired"
    excited = "excited"


class SeasonWeather(str, Enum):
    monsoon = "monsoon"
    summer_hot = "summer_hot"
    winter = "winter"
    pleasant = "pleasant"


class Context(BaseModel):
    companions: Optional[Companions] = None
    group_size_exact: Optional[int] = None
    occasion: Optional[Occasion] = None
    visit_time: Optional[VisitTime] = None
    visit_day: Optional[VisitDay] = None
    visit_type: Optional[VisitType] = None
    meal_format: Optional[MealFormat] = None
    duration_of_visit: Optional[DurationOfVisit] = None
    time_pressure: Optional[TimePressure] = None
    arrival_state: Optional[ArrivalState] = None
    season_weather: Optional[SeasonWeather] = None
    span: Optional[str] = None


# ---------------------------------------------------------------------------
# 2. ATMOSPHERE
# ---------------------------------------------------------------------------


class NoiseLevel(str, Enum):
    silent = "silent"
    library_quiet = "library_quiet"
    quiet = "quiet"
    moderate = "moderate"
    lively = "lively"
    loud = "loud"
    very_loud = "very_loud"


class LightingLevel(str, Enum):
    very_bright = "very_bright"
    bright = "bright"
    warm = "warm"
    moderate = "moderate"
    dim = "dim"
    very_dim = "very_dim"
    candlelit = "candlelit"
    harsh_fluorescent = "harsh_fluorescent"


class MusicVolume(str, Enum):
    none = "none"
    soft_background = "soft_background"
    ambient = "ambient"
    loud = "loud"
    very_loud = "very_loud"


class MusicType(str, Enum):
    none = "none"
    instrumental = "instrumental"
    bollywood = "bollywood"
    western_pop = "western_pop"
    edm_house = "edm_house"
    jazz_lounge = "jazz_lounge"
    indian_classical = "indian_classical"
    rock = "rock"
    live_acoustic = "live_acoustic"
    live_band = "live_band"
    live_dj = "live_dj"
    karaoke = "karaoke"
    variable = "variable"


class SeatingStyle(str, Enum):
    intimate_booths = "intimate_booths"
    private_rooms = "private_rooms"
    regular_tables = "regular_tables"
    communal_tables = "communal_tables"
    bar_seating = "bar_seating"
    counter_seating = "counter_seating"
    outdoor_seating = "outdoor_seating"
    floor_seating = "floor_seating"
    lounge_couch = "lounge_couch"
    mixed = "mixed"


class SeatingComfort(str, Enum):
    excellent = "excellent"
    good = "good"
    acceptable = "acceptable"
    uncomfortable = "uncomfortable"


class CrowdDensity(str, Enum):
    empty = "empty"
    sparse = "sparse"
    comfortable = "comfortable"
    busy = "busy"
    packed = "packed"
    overflowing = "overflowing"
    variable_by_time = "variable_by_time"


class CrowdType(str, Enum):
    couples_heavy = "couples_heavy"
    family_heavy = "family_heavy"
    young_crowd = "young_crowd"
    older_crowd = "older_crowd"
    mixed_ages = "mixed_ages"
    business_crowd = "business_crowd"
    tourist_heavy = "tourist_heavy"
    expat_heavy = "expat_heavy"
    regulars_heavy = "regulars_heavy"
    mixed = "mixed"


class DecorCharacter(str, Enum):
    minimal = "minimal"
    industrial = "industrial"
    rustic = "rustic"
    modern = "modern"
    opulent = "opulent"
    themed = "themed"
    traditional_indian = "traditional_indian"
    heritage = "heritage"
    boho = "boho"
    vintage = "vintage"
    artsy = "artsy"
    sports_bar = "sports_bar"
    family_diner = "family_diner"
    fine_dining_classical = "fine_dining_classical"
    quirky = "quirky"


class SpaceSize(str, Enum):
    tiny = "tiny"
    small = "small"
    medium = "medium"
    large = "large"
    very_large = "very_large"


class PrivacyLevel(str, Enum):
    very_private = "very_private"
    private = "private"
    semi_private = "semi_private"
    open = "open"
    very_open = "very_open"


class Cleanliness(str, Enum):
    poor = "poor"
    acceptable = "acceptable"
    good = "good"
    excellent = "excellent"
    pristine = "pristine"


class RestroomQuality(str, Enum):
    poor = "poor"
    acceptable = "acceptable"
    good = "good"
    excellent = "excellent"


class VentilationTemp(str, Enum):
    too_hot = "too_hot"
    too_cold = "too_cold"
    comfortable = "comfortable"
    variable = "variable"


class AirQuality(str, Enum):
    fresh = "fresh"
    stuffy = "stuffy"
    smoky = "smoky"
    food_aromatic = "food_aromatic"
    unpleasant_smell = "unpleasant_smell"


class ViewOrSetting(str, Enum):
    rooftop = "rooftop"
    street_view = "street_view"
    garden = "garden"
    beachfront = "beachfront"
    lakeside = "lakeside"
    hillside = "hillside"
    pool_view = "pool_view"
    indoor_only = "indoor_only"
    open_air = "open_air"
    industrial_loft = "industrial_loft"
    mall_setting = "mall_setting"
    standalone_villa = "standalone_villa"


class SignatureVisual(str, Enum):
    open_kitchen = "open_kitchen"
    wood_fired_oven = "wood_fired_oven"
    tandoor_view = "tandoor_view"
    bar_view = "bar_view"
    chef_action_counter = "chef_action_counter"
    live_grill = "live_grill"
    dessert_cart = "dessert_cart"
    aquarium = "aquarium"
    art_installations = "art_installations"
    none = "none"


class Instagrammability(str, Enum):
    high = "high"
    moderate = "moderate"
    low = "low"


class AmbientPace(str, Enum):
    relaxed = "relaxed"
    energetic = "energetic"
    hectic = "hectic"


class Accessibility(str, Enum):
    wheelchair_accessible = "wheelchair_accessible"
    lift_available = "lift_available"
    stairs_only = "stairs_only"
    narrow_entrance = "narrow_entrance"


class AtmosphereDimension(BaseModel):
    """Generic atmosphere dimension: level + valence + span."""

    level: Optional[str] = None
    valence: Optional[Valence] = None
    span: Optional[str] = None


class Atmosphere(BaseModel):
    noise: Optional[AtmosphereDimension] = None
    lighting: Optional[AtmosphereDimension] = None
    music_volume: Optional[AtmosphereDimension] = None
    music_type: Optional[MusicType] = None
    seating_style: Optional[SeatingStyle] = None
    seating_comfort: Optional[SeatingComfort] = None
    crowd_density: Optional[CrowdDensity] = None
    crowd_type: Optional[CrowdType] = None
    decor_character: Optional[DecorCharacter] = None
    space_size: Optional[SpaceSize] = None
    privacy_level: Optional[PrivacyLevel] = None
    cleanliness: Optional[Cleanliness] = None
    restroom_quality: Optional[RestroomQuality] = None
    ventilation_temp: Optional[VentilationTemp] = None
    air_quality: Optional[AirQuality] = None
    view_or_setting: Optional[ViewOrSetting] = None
    signature_visual: Optional[SignatureVisual] = None
    instagrammability: Optional[Instagrammability] = None
    ambient_pace: Optional[AmbientPace] = None
    accessibility: Optional[Accessibility] = None
    span: Optional[str] = None


# ---------------------------------------------------------------------------
# 3. SERVICE
# ---------------------------------------------------------------------------


class ServiceSpeed(str, Enum):
    very_slow = "very_slow"
    slow = "slow"
    moderate = "moderate"
    quick = "quick"
    very_quick = "very_quick"
    variable = "variable"


class ServiceAttentiveness(str, Enum):
    ignoring = "ignoring"
    minimal = "minimal"
    acceptable = "acceptable"
    good = "good"
    excellent = "excellent"
    overbearing = "overbearing"


class StaffFriendliness(str, Enum):
    hostile = "hostile"
    rude = "rude"
    indifferent = "indifferent"
    polite = "polite"
    warm = "warm"
    overly_familiar = "overly_familiar"


class ServiceKnowledge(str, Enum):
    clueless = "clueless"
    adequate = "adequate"
    knowledgeable = "knowledgeable"
    expert = "expert"


class ServiceProactive(str, Enum):
    absent = "absent"
    requires_chasing = "requires_chasing"
    responsive_when_called = "responsive_when_called"
    proactive = "proactive"
    anticipatory = "anticipatory"


class WaitForTable(str, Enum):
    none = "none"
    short_under_10 = "short_under_10"
    moderate_10_30 = "moderate_10_30"
    long_30_60 = "long_30_60"
    very_long_60plus = "very_long_60plus"


class WaitForFood(str, Enum):
    quick_under_15 = "quick_under_15"
    normal_15_30 = "normal_15_30"
    slow_30_45 = "slow_30_45"
    very_slow_45plus = "very_slow_45plus"


class WaitForBill(str, Enum):
    quick = "quick"
    normal = "normal"
    annoyingly_long = "annoyingly_long"


class ManagerQuality(str, Enum):
    excellent = "excellent"
    fine = "fine"
    problematic = "problematic"
    actively_negative = "actively_negative"


class MultilingualService(str, Enum):
    english_fluent = "english_fluent"
    english_basic = "english_basic"
    regional_only = "regional_only"


class TableManagement(str, Enum):
    efficient = "efficient"
    chaotic = "chaotic"
    dismissive = "dismissive"


class ComplaintHandling(str, Enum):
    excellent = "excellent"
    acceptable = "acceptable"
    poor = "poor"
    dismissive = "dismissive"


class BillAccuracy(str, Enum):
    accurate = "accurate"
    minor_error = "minor_error"
    major_error = "major_error"


class Service(BaseModel):
    service_speed: Optional[ServiceSpeed] = None
    service_attentiveness: Optional[ServiceAttentiveness] = None
    staff_friendliness: Optional[StaffFriendliness] = None
    service_knowledge: Optional[ServiceKnowledge] = None
    service_proactive: Optional[ServiceProactive] = None
    wait_for_table: Optional[WaitForTable] = None
    wait_for_food: Optional[WaitForFood] = None
    wait_for_bill: Optional[WaitForBill] = None
    manager_quality: Optional[ManagerQuality] = None
    multilingual_service: Optional[MultilingualService] = None
    table_management: Optional[TableManagement] = None
    complaint_handling: Optional[ComplaintHandling] = None
    bill_accuracy: Optional[BillAccuracy] = None
    span: Optional[str] = None


# ---------------------------------------------------------------------------
# 4. VALUE
# ---------------------------------------------------------------------------


class PriceTier(str, Enum):
    budget = "budget"
    mid_range = "mid_range"
    upscale = "upscale"
    luxury = "luxury"


class PricePerception(str, Enum):
    underpriced = "underpriced"
    fair = "fair"
    premium_justified = "premium_justified"
    overpriced = "overpriced"


class ValueSignal(str, Enum):
    great_value = "great_value"
    fair_value = "fair_value"
    poor_value = "poor_value"


class PortionSize(str, Enum):
    too_small = "too_small"
    small = "small"
    adequate = "adequate"
    generous = "generous"
    very_generous = "very_generous"


class QualityToPrice(str, Enum):
    exceeds = "exceeds"
    matches = "matches"
    undershoots = "undershoots"


class PricingTransparency(str, Enum):
    clear = "clear"
    unclear = "unclear"
    hidden_charges = "hidden_charges"


class TaxServicePerception(str, Enum):
    reasonable = "reasonable"
    high_but_disclosed = "high_but_disclosed"
    surprise_charges = "surprise_charges"


class Value(BaseModel):
    price_tier: Optional[PriceTier] = None
    price_perception: Optional[PricePerception] = None
    value_signal: Optional[ValueSignal] = None
    portion_size: Optional[PortionSize] = None
    quality_to_price: Optional[QualityToPrice] = None
    pricing_transparency: Optional[PricingTransparency] = None
    tax_service_perception: Optional[TaxServicePerception] = None
    spend_per_head_inr: Optional[int] = None
    span: Optional[str] = None


# ---------------------------------------------------------------------------
# 5. DISHES
# ---------------------------------------------------------------------------


class DishCategory(str, Enum):
    appetizer = "appetizer"
    main = "main"
    side = "side"
    dessert = "dessert"
    beverage = "beverage"
    cocktail = "cocktail"
    mocktail = "mocktail"
    bread = "bread"
    rice = "rice"
    combo = "combo"
    kids = "kids"
    accompaniment = "accompaniment"


class DishSentiment(str, Enum):
    loved = "loved"
    liked = "liked"
    neutral = "neutral"
    disliked = "disliked"
    hated = "hated"


class DishRole(str, Enum):
    hero = "hero"
    signature = "signature"
    must_try = "must_try"
    ordinary = "ordinary"
    mixed = "mixed"
    avoid = "avoid"
    disappointing = "disappointing"


class DishPortion(str, Enum):
    too_small = "too_small"
    small = "small"
    adequate = "adequate"
    generous = "generous"


class DishPresentation(str, Enum):
    beautiful = "beautiful"
    standard = "standard"
    poor = "poor"


class TempServed(str, Enum):
    hot = "hot"
    warm = "warm"
    room_temp_correctly = "room_temp_correctly"
    cold_when_should_be_hot = "cold_when_should_be_hot"
    ice_cold = "ice_cold"


class Freshness(str, Enum):
    very_fresh = "very_fresh"
    fresh = "fresh"
    stale = "stale"


class Authenticity(str, Enum):
    authentic = "authentic"
    adapted = "adapted"
    fusion = "fusion"
    inauthentic = "inauthentic"


class ConsistencyMention(str, Enum):
    always_good = "always_good"
    usually_good = "usually_good"
    hit_or_miss = "hit_or_miss"
    declining = "declining"


class Dish(BaseModel):
    name: str = Field(..., description="Lowercased dish name, e.g. 'butter chicken'")
    category: Optional[DishCategory] = None
    sentiment: Optional[DishSentiment] = None
    role: Optional[DishRole] = None
    is_recommended: Optional[bool] = None
    taste_descriptors: Optional[list[str]] = Field(
        default=None,
        description=(
            "Subset of: spicy, sweet, tangy, smoky, rich, light, fresh, fiery, "
            "bland, balanced, creamy, dry, earthy, herby, garlicky, buttery, "
            "charred, zingy, umami, fermented, crispy, soft"
        ),
    )
    portion: Optional[DishPortion] = None
    presentation: Optional[DishPresentation] = None
    temperature_served: Optional[TempServed] = None
    freshness: Optional[Freshness] = None
    authenticity: Optional[Authenticity] = None
    consistency_mention: Optional[ConsistencyMention] = None
    repeat_order_intent: Optional[bool] = None
    span: Optional[str] = None


# ---------------------------------------------------------------------------
# 6. OCCASION FIT
# ---------------------------------------------------------------------------


class OccasionFit(BaseModel):
    """What occasions the reviewer says this venue is good/bad/specifically-not for.

    Use values from:
      date_first, date_nth, date_anniversary, proposal,
      intimate_romantic, grand_romantic,
      family_with_young_kids, family_with_teens, family_with_elders, family_general,
      friends_casual, friends_party, friends_drinks,
      solo_quick, solo_leisurely, solo_remote_work, solo_with_book,
      celebration_birthday, celebration_anniversary, celebration_career,
      celebration_engagement,
      business_meeting_quiet, business_client_impress, business_team_meal,
      business_interview,
      group_8plus, kids_birthday_party, corporate_event,
      quick_meal, leisurely_meal, drinks_only, late_night_food,
      pre_movie, post_movie, brunch_social, brunch_solo,
      cheat_meal, health_meal, impress_outsiders,
      regular_haunt, tourist_first_visit, nostalgic_revisit
    """

    good_for: Optional[list[str]] = None
    bad_for: Optional[list[str]] = None
    specifically_not: Optional[list[str]] = None
    span: Optional[str] = None


# ---------------------------------------------------------------------------
# 7. IMMUNE FLAGS (CIST) — asymmetric-weight negatives
# ---------------------------------------------------------------------------


class ImmuneFlag(BaseModel):
    severity: Severity
    span: str


class ImmuneFlags(BaseModel):
    hygiene_concern: Optional[ImmuneFlag] = None
    unclean_restroom: Optional[ImmuneFlag] = None
    food_safety_concern: Optional[ImmuneFlag] = None
    pest_or_animal: Optional[ImmuneFlag] = None
    rude_staff: Optional[ImmuneFlag] = None
    discriminatory_treatment: Optional[ImmuneFlag] = None
    dignity_violation: Optional[ImmuneFlag] = None
    overcharging: Optional[ImmuneFlag] = None
    hidden_charges: Optional[ImmuneFlag] = None
    wrong_order_delivered: Optional[ImmuneFlag] = None
    missing_items_billed: Optional[ImmuneFlag] = None
    excessive_wait_table: Optional[ImmuneFlag] = None
    excessive_wait_food: Optional[ImmuneFlag] = None
    bait_and_switch: Optional[ImmuneFlag] = None
    aggressive_upselling: Optional[ImmuneFlag] = None
    pressure_to_leave: Optional[ImmuneFlag] = None
    noise_complaint_made: Optional[ImmuneFlag] = None
    reservation_issue: Optional[ImmuneFlag] = None
    payment_friction: Optional[ImmuneFlag] = None
    parking_problem: Optional[ImmuneFlag] = None
    safety_concern: Optional[ImmuneFlag] = None
    accessibility_failure: Optional[ImmuneFlag] = None
    temperature_complaint: Optional[ImmuneFlag] = None
    food_inconsistency: Optional[ImmuneFlag] = None
    mask_or_sanitation_lapse: Optional[ImmuneFlag] = None
    photo_or_phone_restriction: Optional[ImmuneFlag] = None


# ---------------------------------------------------------------------------
# 8. RESONANCE (shadow-layer signal)
# ---------------------------------------------------------------------------


class ExpectationsVsReality(str, Enum):
    exceeded = "exceeded"
    met = "met"
    under_delivered = "under_delivered"


class NarrativeArc(str, Enum):
    bad_to_good = "bad_to_good"
    good_to_bad = "good_to_bad"
    consistent_good = "consistent_good"
    consistent_bad = "consistent_bad"
    variable = "variable"


class RecommendationStrength(str, Enum):
    strong = "strong"
    moderate = "moderate"
    weak = "weak"
    negative = "negative"


class WouldRevisitWhen(str, Enum):
    special_occasion_only = "special_occasion_only"
    regular = "regular"
    never = "never"


class Resonance(BaseModel):
    """Distinguishes genuine emotional residue (resonance) from
    identity performance (performance markers). Real signal lives in
    the former."""

    resonance_markers: Optional[list[str]] = Field(
        default=None,
        description=(
            "Verbatim phrases from the review indicating genuine resonance, e.g. "
            "'didn't expect', 'actually surprised me', 'kept thinking about', "
            "'stuck with me', 'still remember', 'craving it', 'had to come back'."
        ),
    )
    performance_markers: Optional[list[str]] = Field(
        default=None,
        description=(
            "Verbatim phrases of identity performance, e.g. 'best ever', "
            "'absolutely amazing', '10/10', 'stunning', 'phenomenal', 'perfection', "
            "'must visit', 'mind-blowing', 'to die for', 'heavenly'."
        ),
    )
    emotional_lingering: Optional[bool] = None
    expectations_vs_reality: Optional[ExpectationsVsReality] = None
    narrative_arc: Optional[NarrativeArc] = None
    comparison_made: Optional[list[str]] = Field(
        default=None,
        description="Other restaurants/cuisines this review compared against.",
    )
    nostalgia_or_memory: Optional[bool] = None
    recommendation_strength: Optional[RecommendationStrength] = None
    would_revisit_when: Optional[WouldRevisitWhen] = None
    span: Optional[str] = None


# ---------------------------------------------------------------------------
# 9. MEMORY
# ---------------------------------------------------------------------------


class IntentToReturn(str, Enum):
    will_return = "will_return"
    undecided = "undecided"
    will_not_return = "will_not_return"


class ReferralIntent(str, Enum):
    will_recommend = "will_recommend"
    conditional = "conditional"
    will_not_recommend = "will_not_recommend"
    negative = "negative"  # active anti-recommendation (stronger than will_not_recommend)


class RecommendedToWhom(str, Enum):
    friends = "friends"
    family = "family"
    colleagues = "colleagues"
    dates = "dates"
    tourists = "tourists"
    kids = "kids"


class Memory(BaseModel):
    intent_to_return: Optional[IntentToReturn] = None
    already_returned: Optional[bool] = None
    times_visited_mentioned: Optional[int] = None
    referral_intent: Optional[ReferralIntent] = None
    companion_bring_intent: Optional[bool] = None
    recommended_to_whom: Optional[list[RecommendedToWhom]] = None
    span: Optional[str] = None


# ---------------------------------------------------------------------------
# 10. DIETARY
# ---------------------------------------------------------------------------


class VegetarianSignal(str, Enum):
    veg_only = "veg_only"
    veg_friendly = "veg_friendly"
    mixed = "mixed"
    non_veg_focused = "non_veg_focused"
    non_veg_only = "non_veg_only"


class VeganSignal(str, Enum):
    vegan_options_strong = "vegan_options_strong"
    some_vegan = "some_vegan"
    none = "none"


class TriState(str, Enum):
    yes = "yes"
    partial = "partial"
    no = "no"


class StrengthLevel(str, Enum):
    strong = "strong"
    some = "some"
    none = "none"


class HalalStatus(str, Enum):
    certified = "certified"
    claimed = "claimed"
    not_halal = "not_halal"


class AlcoholServed(str, Enum):
    full_bar = "full_bar"
    beer_wine_only = "beer_wine_only"
    none = "none"


class QualityLevel(str, Enum):
    excellent = "excellent"
    standard = "standard"
    poor = "poor"


class AwarenessLevel(str, Enum):
    strong = "strong"
    acceptable = "acceptable"
    poor = "poor"


class Dietary(BaseModel):
    vegetarian_signal: Optional[VegetarianSignal] = None
    vegan_signal: Optional[VeganSignal] = None
    jain_friendly: Optional[TriState] = None
    egg_eaters_only: Optional[bool] = None
    gluten_free_options: Optional[StrengthLevel] = None
    keto_friendly: Optional[StrengthLevel] = None
    halal_status: Optional[HalalStatus] = None
    alcohol_served: Optional[AlcoholServed] = None
    non_alcoholic_options_quality: Optional[QualityLevel] = None
    health_conscious_options: Optional[StrengthLevel] = None
    allergen_awareness: Optional[AwarenessLevel] = None
    span: Optional[str] = None


# ---------------------------------------------------------------------------
# 11. CUISINE
# ---------------------------------------------------------------------------


class CuisineSpecializationClarity(str, Enum):
    focused = "focused"
    scattered_menu = "scattered_menu"
    extensive_but_coherent = "extensive_but_coherent"


class MatchedExpectation(str, Enum):
    yes = "yes"
    no = "no"
    partial = "partial"


class FusionQuality(str, Enum):
    successful = "successful"
    forced = "forced"
    not_applicable = "not_applicable"


class MenuBreadth(str, Enum):
    narrow_focused = "narrow_focused"
    medium = "medium"
    extensive = "extensive"
    overwhelming = "overwhelming"


class Cuisine(BaseModel):
    cuisines_mentioned: Optional[list[str]] = None
    regional_specificity: Optional[list[str]] = Field(
        default=None,
        description=(
            "Specific regional cuisines if mentioned, e.g. karnataka, andhra, "
            "tamil_brahmin, mangalorean, kerala, awadhi, hyderabadi, mughlai, "
            "punjabi, bengali, marathi, goan, sicilian, neapolitan, cantonese, "
            "sichuan, thai_central, vietnamese_north, japanese_izakaya, lebanese, "
            "levantine, etc."
        ),
    )
    cuisine_specialization_clarity: Optional[CuisineSpecializationClarity] = None
    matched_expectation: Optional[MatchedExpectation] = None
    fusion_quality: Optional[FusionQuality] = None
    signature_dishes_mentioned: Optional[list[str]] = None
    menu_breadth: Optional[MenuBreadth] = None
    span: Optional[str] = None


# ---------------------------------------------------------------------------
# 12. PRACTICAL
# ---------------------------------------------------------------------------


class ReservationRequired(str, Enum):
    mandatory = "mandatory"
    recommended_weekends = "recommended_weekends"
    walk_in_fine = "walk_in_fine"


class ReservationEase(str, Enum):
    easy = "easy"
    difficult = "difficult"
    no_system = "no_system"


class WaitDuringPeak(str, Enum):
    none = "none"
    short = "short"
    moderate = "moderate"
    long = "long"
    very_long = "very_long"


class ParkingAvailability(str, Enum):
    valet = "valet"
    dedicated_lot = "dedicated_lot"
    street_easy = "street_easy"
    street_hard = "street_hard"
    none = "none"


class PaymentMethod(str, Enum):
    cards = "cards"
    upi = "upi"
    cash_only = "cash_only"
    wallet = "wallet"


class WifiQuality(str, Enum):
    strong = "strong"
    weak = "weak"
    none = "none"


class PhoneSignalInside(str, Enum):
    good = "good"
    weak = "weak"
    none = "none"


class ChildFacility(str, Enum):
    high_chair = "high_chair"
    kids_menu = "kids_menu"
    play_area = "play_area"
    changing_table = "changing_table"


class PetPolicy(str, Enum):
    pet_friendly = "pet_friendly"
    pets_allowed_outdoor = "pets_allowed_outdoor"
    no_pets = "no_pets"


class DressCode(str, Enum):
    formal = "formal"
    smart_casual = "smart_casual"
    casual = "casual"
    no_code = "no_code"


class PhotoPolicy(str, Enum):
    freely_allowed = "freely_allowed"
    restricted = "restricted"
    not_allowed = "not_allowed"


class Practical(BaseModel):
    reservation_required: Optional[ReservationRequired] = None
    reservation_ease: Optional[ReservationEase] = None
    wait_during_peak: Optional[WaitDuringPeak] = None
    parking_availability: Optional[ParkingAvailability] = None
    payment_methods: Optional[list[PaymentMethod]] = None
    wifi_quality: Optional[WifiQuality] = None
    solo_diner_comfortable: Optional[bool] = None
    phone_signal_inside: Optional[PhoneSignalInside] = None
    child_facilities: Optional[list[ChildFacility]] = None
    pet_policy: Optional[PetPolicy] = None
    dress_code: Optional[DressCode] = None
    photo_policy: Optional[PhotoPolicy] = None
    loyalty_program_mentioned: Optional[bool] = None
    span: Optional[str] = None


# ---------------------------------------------------------------------------
# 13. BAR (only if drinks served)
# ---------------------------------------------------------------------------


class BarQuality(str, Enum):
    excellent = "excellent"
    good = "good"
    poor = "poor"
    no_bar = "no_bar"


class ProgramLevel(str, Enum):
    innovative = "innovative"
    standard = "standard"
    poor = "poor"
    none = "none"


class SelectionLevel(str, Enum):
    extensive = "extensive"
    standard = "standard"
    limited = "limited"
    none = "none"


class BeerSelection(str, Enum):
    craft_extensive = "craft_extensive"
    standard = "standard"
    limited = "limited"
    none = "none"


class CocktailPricing(str, Enum):
    fair = "fair"
    premium = "premium"
    overpriced = "overpriced"


class BartenderSkill(str, Enum):
    excellent = "excellent"
    competent = "competent"
    poor = "poor"


class HappyHourQuality(str, Enum):
    worth_it = "worth_it"
    standard = "standard"


class Bar(BaseModel):
    bar_quality: Optional[BarQuality] = None
    cocktail_program: Optional[ProgramLevel] = None
    wine_program: Optional[SelectionLevel] = None
    whisky_selection: Optional[SelectionLevel] = None
    beer_selection: Optional[BeerSelection] = None
    cocktail_pricing: Optional[CocktailPricing] = None
    bartender_skill: Optional[BartenderSkill] = None
    happy_hour_quality: Optional[HappyHourQuality] = None
    span: Optional[str] = None


# ---------------------------------------------------------------------------
# 14. ENTERTAINMENT
# ---------------------------------------------------------------------------


class LiveMusic(str, Enum):
    none = "none"
    acoustic = "acoustic"
    band = "band"
    dj = "dj"
    karaoke = "karaoke"


class LiveMusicQuality(str, Enum):
    excellent = "excellent"
    good = "good"
    poor = "poor"


class SportsScreening(str, Enum):
    none = "none"
    casual = "casual"
    dedicated_setup = "dedicated_setup"


class DanceFloor(str, Enum):
    yes_active = "yes_active"
    yes_quiet = "yes_quiet"
    no = "no"


class PerformancesOther(str, Enum):
    standup_comedy = "standup_comedy"
    magic = "magic"
    poetry = "poetry"
    quiz = "quiz"
    none = "none"


class Entertainment(BaseModel):
    live_music: Optional[LiveMusic] = None
    live_music_quality: Optional[LiveMusicQuality] = None
    sports_screening: Optional[SportsScreening] = None
    events_or_themed_nights: Optional[bool] = None
    dance_floor: Optional[DanceFloor] = None
    performances_other: Optional[PerformancesOther] = None
    span: Optional[str] = None


# ---------------------------------------------------------------------------
# TOP-LEVEL EXTRACTION ENVELOPE
# ---------------------------------------------------------------------------


class ReviewExtraction(BaseModel):
    """The full structured extraction the LLM emits for one review.

    Every section is optional. Within each section, every field is also
    optional. If a dimension is not addressed in the review, omit the
    field entirely (do NOT emit `not_mentioned`). Every non-null
    extracted field with semantic content carries a `span` (literal quote
    from the review).
    """

    context: Optional[Context] = None
    atmosphere: Optional[Atmosphere] = None
    service: Optional[Service] = None
    value: Optional[Value] = None
    dishes: Optional[list[Dish]] = None
    occasion_fit: Optional[OccasionFit] = None
    immune_flags: Optional[ImmuneFlags] = None
    resonance: Optional[Resonance] = None
    memory: Optional[Memory] = None
    dietary: Optional[Dietary] = None
    cuisine: Optional[Cuisine] = None
    practical: Optional[Practical] = None
    bar: Optional[Bar] = None
    entertainment: Optional[Entertainment] = None
