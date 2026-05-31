#!/usr/bin/env bash
# Scrape 1mg drug pages (/drugs/*) into data/1mg_drugs/drugs.csv
#
#   ./scripts/run_1mg_drugs_scrape.sh dry-run
#   ./scripts/run_1mg_drugs_scrape.sh pilot
#   ./scripts/run_1mg_drugs_scrape.sh run

set -euo pipefail
cd "$(dirname "$0")/.."

ROOT="$(pwd)"
URL_FILE="${ROOT}/scripts/output/one_mg_urls.txt"
OUTPUT_DIR="${ROOT}/data/1mg_drugs"

MODE="${1:-}"
shift || true
extra_args=("$@")

case "${MODE}" in
  dry-run)
    echo "=== 1mg drugs dry run (no HTTP) ==="
    if ((${#extra_args[@]})); then
      python3 scripts/scrape_1mg_drugs.py \
        --url-file "${URL_FILE}" \
        --output-dir "${OUTPUT_DIR}" \
        --dry-run \
        --limit 20 \
        "${extra_args[@]}"
    else
      python3 scripts/scrape_1mg_drugs.py \
        --url-file "${URL_FILE}" \
        --output-dir "${OUTPUT_DIR}" \
        --dry-run \
        --limit 20
    fi
    ;;
  pilot)
    echo "=== 1mg drugs pilot (100 URLs) ==="
    echo "Output: ${OUTPUT_DIR}/drugs.csv"
    if ((${#extra_args[@]})); then
      python3 scripts/scrape_1mg_drugs.py \
        --url-file "${URL_FILE}" \
        --output-dir "${OUTPUT_DIR}" \
        --limit 100 \
        --progress \
        "${extra_args[@]}"
    else
      python3 scripts/scrape_1mg_drugs.py \
        --url-file "${URL_FILE}" \
        --output-dir "${OUTPUT_DIR}" \
        --limit 100 \
        --progress
    fi
    ;;
  run)
    echo "=== 1mg drugs full scrape (~384k URLs) ==="
    echo "Output: ${OUTPUT_DIR}/drugs.csv"
    echo "Resume: ${OUTPUT_DIR}/.batch_completed_urls.txt"
    echo ""
    echo "Press Ctrl+C within 5s to abort..."
    sleep 5
    if ((${#extra_args[@]})); then
      python3 scripts/scrape_1mg_drugs.py \
        --url-file "${URL_FILE}" \
        --output-dir "${OUTPUT_DIR}" \
        --progress \
        "${extra_args[@]}"
    else
      python3 scripts/scrape_1mg_drugs.py \
        --url-file "${URL_FILE}" \
        --output-dir "${OUTPUT_DIR}" \
        --progress
    fi
    ;;
  *)
    echo "Usage: $0 {dry-run|pilot|run} [extra scrape_1mg_drugs.py args...]"
    exit 1
    ;;
esac
