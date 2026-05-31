"""Myeloma Beacon forum scraper (https://myelomabeacon.org/forum/).

A two-phase phpBB3 scraper, parameterised by sub-forum slug
(e.g. ``multiple-myeloma``, ``treatments-side-effects``):

1. ``discover_topics`` walks the chosen sub-forum's index pages
   (25 topics per page) and writes ``topic_urls.txt`` plus ``topics.csv``.
2. ``scrape_topics`` opens every topic URL, walks every post page
   (10 posts per page), and appends one row per post to ``posts.csv``.

Both phases are resumable: completed topic IDs are tracked in
``.completed_topics.txt`` and skipped on re-run. Each sub-forum gets
its own output sub-directory under ``data/MyelomaBeacon/<slug>/``.
"""
