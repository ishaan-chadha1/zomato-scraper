ARC — Restaurant Review Extraction Schema
==========================================

Per-review JSON shape that Gemini Flash Lite will emit. Every non-null
field carries a `supporting_span` (literal quote from the review).
Every enum includes `not_mentioned` as a first-class value (omitted
below for readability).

----------------------------------------------------------------------
1. CONTEXT — reviewer situation
----------------------------------------------------------------------

companions:
  solo, couple_romantic, couple_casual,
  small_group_friends (3-5), small_group_family,
  large_group_friends (6+), large_group_family,
  family_with_young_kids, family_with_teens,
  family_with_elders, mixed_generations,
  business_colleagues, business_client, business_team

group_size_exact: integer or null (when stated explicitly)

occasion:
  regular_casual, first_date, nth_date, proposal, anniversary,
  birthday_self, birthday_other, engagement, hen_bachelor,
  farewell, homecoming, promotion, business_pitch,
  business_team_meal, kitty_party, office_party,
  kids_birthday, festival_meal, pre_movie, post_movie,
  pre_concert, cheat_meal, midweek_treat, weekend_indulgence,
  family_gathering

visit_time:
  breakfast, brunch, lunch, hi_tea, early_dinner, dinner,
  late_night, drinks_pre_dinner, drinks_post_dinner

visit_day: weekday, weekend

visit_type: first_visit, repeat_visit, regular_haunt

meal_format:
  dine_in, takeaway, delivery, drive_through,
  private_dining, chef_table, events_or_catering

duration_of_visit:
  quick_under_30, standard_30_90, leisurely_90plus

time_pressure: relaxed, time_constrained

arrival_state:
  celebratory, casual, stressed, very_hungry, tired, excited

season_weather: monsoon, summer_hot, winter, pleasant


----------------------------------------------------------------------
2. ATMOSPHERE — each is {level, valence, span} or null
----------------------------------------------------------------------

valence everywhere: positive, neutral, negative, mixed

noise:
  silent, library_quiet, quiet, moderate, lively, loud, very_loud

lighting:
  very_bright, bright, warm, moderate, dim, very_dim,
  candlelit, harsh_fluorescent

music_volume:
  none, soft_background, ambient, loud, very_loud

music_type:
  none, instrumental, bollywood, western_pop, edm_house,
  jazz_lounge, indian_classical, rock, live_acoustic,
  live_band, live_dj, karaoke, variable

seating_style:
  intimate_booths, private_rooms, regular_tables,
  communal_tables, bar_seating, counter_seating,
  outdoor_seating, floor_seating, lounge_couch, mixed

seating_comfort: excellent, good, acceptable, uncomfortable

crowd_density:
  empty, sparse, comfortable, busy, packed,
  overflowing, variable_by_time

crowd_type:
  couples_heavy, family_heavy, young_crowd, older_crowd,
  mixed_ages, business_crowd, tourist_heavy, expat_heavy,
  regulars_heavy, mixed

decor_character:
  minimal, industrial, rustic, modern, opulent, themed,
  traditional_indian, heritage, boho, vintage, artsy,
  sports_bar, family_diner, fine_dining_classical, quirky

space_size: tiny, small, medium, large, very_large

privacy_level:
  very_private, private, semi_private, open, very_open

cleanliness: poor, acceptable, good, excellent, pristine

restroom_quality: poor, acceptable, good, excellent

ventilation_temp: too_hot, too_cold, comfortable, variable

air_quality:
  fresh, stuffy, smoky, food_aromatic, unpleasant_smell

view_or_setting:
  rooftop, street_view, garden, beachfront, lakeside,
  hillside, pool_view, indoor_only, open_air,
  industrial_loft, mall_setting, standalone_villa

signature_visual:
  open_kitchen, wood_fired_oven, tandoor_view, bar_view,
  chef_action_counter, live_grill, dessert_cart,
  aquarium, art_installations, none

instagrammability: high, moderate, low

ambient_pace: relaxed, energetic, hectic

accessibility:
  wheelchair_accessible, lift_available,
  stairs_only, narrow_entrance


----------------------------------------------------------------------
3. SERVICE — each {level, span} or null
----------------------------------------------------------------------

service_speed:
  very_slow, slow, moderate, quick, very_quick, variable

service_attentiveness:
  ignoring, minimal, acceptable, good, excellent, overbearing

staff_friendliness:
  hostile, rude, indifferent, polite, warm, overly_familiar

service_knowledge:
  clueless, adequate, knowledgeable, expert

service_proactive:
  absent, requires_chasing, responsive_when_called,
  proactive, anticipatory

wait_for_table:
  none, short_under_10, moderate_10_30,
  long_30_60, very_long_60plus

wait_for_food:
  quick_under_15, normal_15_30,
  slow_30_45, very_slow_45plus

wait_for_bill: quick, normal, annoyingly_long

manager_quality:
  excellent, fine, problematic, actively_negative

multilingual_service:
  english_fluent, english_basic, regional_only

table_management: efficient, chaotic, dismissive

complaint_handling: excellent, acceptable, poor, dismissive

bill_accuracy: accurate, minor_error, major_error


----------------------------------------------------------------------
4. VALUE
----------------------------------------------------------------------

price_tier: budget, mid_range, upscale, luxury

price_perception:
  underpriced, fair, premium_justified, overpriced

value_signal: great_value, fair_value, poor_value

portion_size:
  too_small, small, adequate, generous, very_generous

quality_to_price: exceeds, matches, undershoots

pricing_transparency: clear, unclear, hidden_charges

tax_service_perception:
  reasonable, high_but_disclosed, surprise_charges

spend_per_head_inr: integer or null


----------------------------------------------------------------------
5. DISHES — array, one entry per mentioned dish
----------------------------------------------------------------------

per dish:

  name: string (lowercased, e.g. "butter chicken")

  category:
    appetizer, main, side, dessert, beverage,
    cocktail, mocktail, bread, rice, combo,
    kids, accompaniment

  sentiment: loved, liked, neutral, disliked, hated

  role:
    hero, signature, must_try, ordinary,
    mixed, avoid, disappointing

  is_recommended: bool

  taste_descriptors (multi-select from):
    spicy, sweet, tangy, smoky, rich, light, fresh,
    fiery, bland, balanced, creamy, dry, earthy,
    herby, garlicky, buttery, charred, zingy, umami,
    fermented, crispy, soft

  portion: too_small, small, adequate, generous

  presentation: beautiful, standard, poor

  temperature_served:
    hot, warm, room_temp_correctly,
    cold_when_should_be_hot, ice_cold

  freshness: very_fresh, fresh, stale

  authenticity: authentic, adapted, fusion, inauthentic

  consistency_mention:
    always_good, usually_good, hit_or_miss, declining

  repeat_order_intent: bool

  span: string


----------------------------------------------------------------------
6. OCCASION_FIT
----------------------------------------------------------------------

good_for: list (from enum below)
bad_for: list (from same enum)
specifically_not: list (stronger flag)
span: string

enum (used across all three lists):
  date_first, date_nth, date_anniversary, proposal,
  intimate_romantic, grand_romantic,
  family_with_young_kids, family_with_teens,
  family_with_elders, family_general,
  friends_casual, friends_party, friends_drinks,
  solo_quick, solo_leisurely, solo_remote_work, solo_with_book,
  celebration_birthday, celebration_anniversary,
  celebration_career, celebration_engagement,
  business_meeting_quiet, business_client_impress,
  business_team_meal, business_interview,
  group_8plus, kids_birthday_party, corporate_event,
  quick_meal, leisurely_meal, drinks_only, late_night_food,
  pre_movie, post_movie, brunch_social, brunch_solo,
  cheat_meal, health_meal, impress_outsiders,
  regular_haunt, tourist_first_visit, nostalgic_revisit


----------------------------------------------------------------------
7. IMMUNE_FLAGS — each {severity, span} or null
                  severity: mild, moderate, severe
----------------------------------------------------------------------

hygiene_concern              — dirty cutlery, tables, surfaces
unclean_restroom             — bathroom complaint
food_safety_concern          — sick, foreign object, undercooked
pest_or_animal               — rat, cockroach, fly, stray
rude_staff                   — explicit rudeness
discriminatory_treatment     — identity-based
dignity_violation            — refused service, condescending
overcharging                 — bill higher than menu
hidden_charges               — not disclosed upfront
wrong_order_delivered        — wrong dishes
missing_items_billed         — charged for unreceived items
excessive_wait_table         — >30min, complained
excessive_wait_food          — >45min, complained
bait_and_switch              — menu/photos vs reality
aggressive_upselling         — pressured into expensive items
pressure_to_leave            — rushed out
noise_complaint_made         — found it intolerably loud
reservation_issue            — booking ignored, lost
payment_friction             — refused cards/UPI, double-charged
parking_problem              — major parking issue
safety_concern               — women's safety, unsafe area
accessibility_failure        — wheelchair/elderly/pregnant issue
temperature_complaint        — AC broken, too hot/cold
food_inconsistency           — great last time, bad now
mask_or_sanitation_lapse     — post-COVID hygiene
photo_or_phone_restriction   — overly restrictive policy


----------------------------------------------------------------------
8. RESONANCE — shadow-layer language signal
----------------------------------------------------------------------

resonance_markers: list of phrases extracted
performance_markers: list of phrases extracted
emotional_lingering: bool
expectations_vs_reality: exceeded, met, under_delivered
narrative_arc:
  bad_to_good, good_to_bad, consistent_good,
  consistent_bad, variable
comparison_made: list of restaurants/cuisines compared to
nostalgia_or_memory: bool
recommendation_strength: strong, moderate, weak, negative
would_revisit_when:
  special_occasion_only, regular, never
span: string

Resonance markers the LLM looks for:
  "didn't expect", "actually surprised me",
  "kept thinking about", "took me by surprise",
  "stuck with me", "still remember",
  "couldn't stop thinking", "caught off guard",
  "unexpected", "lingered", "craving it",
  "had to come back"

Performance markers the LLM looks for:
  "best ever", "absolutely amazing", "10/10",
  "stunning", "phenomenal", "perfection",
  "must visit" (standalone), "mind-blowing",
  "to die for", "out of this world", "heavenly"


----------------------------------------------------------------------
9. MEMORY
----------------------------------------------------------------------

intent_to_return:
  will_return, undecided, will_not_return

already_returned: bool

times_visited_mentioned: integer or null

referral_intent:
  will_recommend, conditional, will_not_recommend

companion_bring_intent: bool

recommended_to_whom: list from:
  friends, family, colleagues, dates, tourists, kids


----------------------------------------------------------------------
10. DIETARY — Indian-context comprehensive
----------------------------------------------------------------------

vegetarian_signal:
  veg_only, veg_friendly, mixed,
  non_veg_focused, non_veg_only

vegan_signal:
  vegan_options_strong, some_vegan, none

jain_friendly: yes, partial, no

egg_eaters_only: yes, no

gluten_free_options: strong, some, none

keto_friendly: strong, some, none

halal_status: certified, claimed, not_halal

alcohol_served: full_bar, beer_wine_only, none

non_alcoholic_options_quality: excellent, standard, poor

health_conscious_options: strong, some, none

allergen_awareness: strong, acceptable, poor


----------------------------------------------------------------------
11. CUISINE — claim consistency
----------------------------------------------------------------------

cuisines_mentioned: list of normalized cuisine tags

regional_specificity: list from:
  karnataka, andhra, tamil_brahmin, mangalorean, kerala,
  awadhi, hyderabadi, mughlai, punjabi, bengali, marathi,
  goan, sicilian, neapolitan, cantonese, sichuan,
  thai_central, vietnamese_north, japanese_izakaya,
  lebanese, levantine, ...

cuisine_specialization_clarity:
  focused, scattered_menu, extensive_but_coherent

matched_expectation: yes, no, partial

fusion_quality: successful, forced, not_applicable

signature_dishes_mentioned: list

menu_breadth:
  narrow_focused, medium, extensive, overwhelming


----------------------------------------------------------------------
12. PRACTICAL — operational/logistical facts
----------------------------------------------------------------------

reservation_required:
  mandatory, recommended_weekends, walk_in_fine

reservation_ease: easy, difficult, no_system

wait_during_peak:
  none, short, moderate, long, very_long

parking_availability:
  valet, dedicated_lot, street_easy,
  street_hard, none

payment_methods: list from:
  cards, upi, cash_only, wallet

wifi_quality: strong, weak, none

solo_diner_comfortable: bool

phone_signal_inside: good, weak, none

child_facilities: list from:
  high_chair, kids_menu, play_area, changing_table

pet_policy:
  pet_friendly, pets_allowed_outdoor, no_pets

dress_code: formal, smart_casual, casual, no_code

photo_policy: freely_allowed, restricted, not_allowed

loyalty_program_mentioned: bool


----------------------------------------------------------------------
13. BAR — only populated if drinks served
----------------------------------------------------------------------

bar_quality: excellent, good, poor, no_bar

cocktail_program: innovative, standard, poor, none

wine_program: extensive, standard, limited, none

whisky_selection: extensive, standard, limited, none

beer_selection:
  craft_extensive, standard, limited, none

cocktail_pricing: fair, premium, overpriced

bartender_skill: excellent, competent, poor

happy_hour_quality: worth_it, standard


----------------------------------------------------------------------
14. ENTERTAINMENT
----------------------------------------------------------------------

live_music: none, acoustic, band, dj, karaoke

live_music_quality: excellent, good, poor

sports_screening: none, casual, dedicated_setup

events_or_themed_nights: bool

dance_floor: yes_active, yes_quiet, no

performances_other:
  standup_comedy, magic, poetry, quiz, none


----------------------------------------------------------------------
RULES THE LLM MUST FOLLOW
----------------------------------------------------------------------

1. not_mentioned is the default. Never invent a value.
2. supporting_span is mandatory for every non-null field.
   Without it the field stays null.
3. Enums are coarse on purpose. Pick one, don't hedge.
4. Per-field valence on atmosphere captures
   "loud but in a good way." Don't conflate level and sentiment.
5. Lists are bounded (max ~8 items per list field).
6. Spans can be reused — one sentence can populate several fields.
7. Math is never done by the LLM. Aggregation is Python only.


----------------------------------------------------------------------
COST (Gemini Flash Lite, 100-review cap per restaurant)
----------------------------------------------------------------------

Approx $120–180 one-time for the full extraction across
10,921 restaurants × min(reviews, 100).

Cache every raw LLM response to data/llm_cache/ so
re-aggregation never re-calls the API.


----------------------------------------------------------------------
DOWNSTREAM (deterministic Python, no LLM)
----------------------------------------------------------------------

For each of the ~60 atomic fields the aggregator produces:
  - distribution of values across reviews
  - mean/mode + variance
  - evidence_n (how many reviews carried it non-null)
  - confidence derived from evidence_n + recency weight
  - time-bucket splits where the field permits
  - companion-bucket splits where the field permits

Final output:
  aman/data/restaurant_intelligence.parquet
  ~10,921 rows × ~60 aggregated columns
  + audit trail via data/llm_cache/

No labels, no scores surfaced. Pure substrate.
