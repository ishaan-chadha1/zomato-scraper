#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_PY="$ROOT_DIR/.venv/bin/python"

if [[ ! -x "$VENV_PY" ]]; then
  echo "Virtualenv not found at $ROOT_DIR/.venv"
  echo "Create it first: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
  exit 1
fi

CITY="${1:-bangalore}"
TOP_COUNT="${2:-50}"
CONCURRENCY="${3:-6}"
MAX_SCROLLS="${4:-35}"

cd "$ROOT_DIR/zomato-scraper"
exec "$VENV_PY" bulk_scraper.py \
  --city "$CITY" \
  --top-count "$TOP_COUNT" \
  --concurrency "$CONCURRENCY" \
  --max-scrolls "$MAX_SCROLLS"
