# Zomato-Scraper

This repo is split into two fully self-contained sub-projects.

```
.
├── aman/      # Zomato / restaurant / review work
├── sudhir/    # Medical (Myeloma Beacon, MyMyelomaTeam, 1mg drugs)
├── requirements.txt   # shared Python deps (both sides use the same env)
├── .venv/ , venv/     # shared virtualenvs
└── .git/              # single repo, single history
```

## Important — how to run things

Every script keeps its **original relative paths**. To run anything, first
`cd` into the relevant sub-project:

```bash
# Food / Zomato work
cd aman
python3 -m pytest tests/
python3 scripts/run_intelligence_pipeline.py
python3 scripts/run_meta_pipeline.py

# Medical work
cd sudhir
python3 -m pytest tests/
python3 -m scrapers.myeloma_beacon.discover_topics --forum-slug multiple-myeloma
./scripts/run_myeloma_beacon_scrape.sh multiple-myeloma all
python3 scripts/clean_myeloma_beacon.py --data-dir data/MyelomaBeacon/multiple-myeloma
python3 scripts/combine_myeloma_beacon.py
```

## What's inside each side

### `aman/` — Food / Zomato

| folder | contents |
| --- | --- |
| `scrapers/restaurants/` | Zomato restaurant page scrapers (Beast + zomato-scraper) |
| `scrapers/reviews/` | Zomato review scrapers (HTTP scraper, listing DOM parser) |
| `intelligence/` | Dish detection, restaurant matching, scoring, meta-tagging, recommendations |
| `analysis/reviews/` | Review analysis report + figures + tables |
| `data/` | Restaurants/reviews data (CSVs, parquets ~770 MB total) |
| `tests/` | `test_intelligence_unit.py`, `test_ranking_enrich.py` |
| `scripts/` | Pipelines (`run_intelligence_pipeline.py`, `run_meta_pipeline.py`, `analyze_reviews_parquet.py`, `recommend.py`, etc.) |
| `Reviews/` | Loose review CSVs (Mykos Craft Kitchen) |
| `beast-zomato.py`, `zomato_reviews_batch.py`, `zomato_reviews_http.py`, `zomato_master_unique.csv`, `completed_areas.log` | Top-level legacy entrypoints / state |

### `sudhir/` — Medical

| folder | contents |
| --- | --- |
| `scrapers/myeloma_beacon/` | phpBB forum scraper for myelomabeacon.org (parameterised by sub-forum slug) |
| `scrapers/resources/` | MyMyelomaTeam article scrapers (HTTP + Playwright) |
| `analysis/myeloma_beacon/` | Verification audit JSON for Myeloma Beacon scrapes |
| `data/MyelomaBeacon/` | Scraped Myeloma Beacon data: per-sub-forum dirs (`multiple-myeloma/`, `treatments-side-effects/`) with `posts.csv`, `topics.csv`, cleaned parquets, raw HTML cache; combined `posts_with_topics_all.parquet` (39,719 rows) |
| `data/MyMyelomaTeam/` | MyMyelomaTeam scraped articles (CSV) |
| `data/1mg_drugs/` | 1mg drugs catalogue scrape (CSV, ~7.9 GB) |
| `tests/` | `test_myeloma_beacon_parser.py` + fixtures |
| `scripts/` | Myeloma Beacon: `run_myeloma_beacon_scrape.sh`, `verify_myeloma_beacon.py`, `clean_myeloma_beacon.py`, `combine_myeloma_beacon.py`. 1mg: `run_1mg_drugs_scrape.sh`, `scrape_1mg_drugs.py`, `crawl_1mg.py`, `discover_1mg_urls.py`, `fetch_1mg_pages.py`, `count_1mg_drugs.py`. |
| `scripts/output/` | `one_mg_urls.txt` (55 MB discovered 1mg URL list) |

## Why the split is safe

- **Zero cross-imports** between food and medical Python modules. Every
  `from intelligence.*` lives inside `aman/`; the only `from scrapers.*`
  reference is `sudhir/tests/test_myeloma_beacon_parser.py`.
- All scripts keep their original relative paths (e.g.
  `data/MyelomaBeacon/multiple-myeloma`), so no code inside any file
  needed to change — only its containing folder moved.
