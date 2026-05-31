#!/usr/bin/env bash
# Re-scrape restaurants with no dining reviews into a SEPARATE folder (does not touch data/Reviews).
#
# Review before running:
#   ./scripts/run_empty_review_rescrape.sh dry-run
#
# Start full run (after approval):
#   ./scripts/run_empty_review_rescrape.sh run
#
# Pilot (130 mismatch-only):
#   ./scripts/run_empty_review_rescrape.sh run --limit 130 --retry-status processed_no_csv_match

set -euo pipefail
cd "$(dirname "$0")/.."

ROOT="$(pwd)"
COVERAGE="${ROOT}/data/Reviews/review_coverage_report.csv"
OUTPUT_DIR="${ROOT}/data/Reviews_rescrape"
COMPLETED_LOG="${OUTPUT_DIR}/.batch_completed_urls.txt"
FAILED_LOG="${OUTPUT_DIR}/.batch_failed.csv"
RETRY_SUMMARY="${OUTPUT_DIR}/rescrape_summary.csv"

MODE="${1:-}"
shift || true

extra_args=("$@")

case "${MODE}" in
  dry-run)
    echo "=== Dry run (no HTTP, no writes) ==="
    echo "Source:  ${COVERAGE}"
    echo "Output:  ${OUTPUT_DIR}  (original data/Reviews is NOT modified)"
    if ((${#extra_args[@]})); then
      python3 zomato_reviews_batch.py \
        --output-dir "${OUTPUT_DIR}" \
        --coverage-report "${COVERAGE}" \
        --retry-status no_dining_reviews processed_no_csv_match \
        --force-retry \
        --rewrite-order-to-info \
        --dry-run \
        "${extra_args[@]}"
    else
      python3 zomato_reviews_batch.py \
        --output-dir "${OUTPUT_DIR}" \
        --coverage-report "${COVERAGE}" \
        --retry-status no_dining_reviews processed_no_csv_match \
        --force-retry \
        --rewrite-order-to-info \
        --dry-run
    fi
    ;;
  run)
    mkdir -p "${OUTPUT_DIR}"
    echo "=== Full empty review re-scrape ==="
    echo "Source:  ${COVERAGE}"
    echo "Output:  ${OUTPUT_DIR}"
    echo "Logs:    ${COMPLETED_LOG}"
    echo "         ${FAILED_LOG}"
    echo "Summary: ${RETRY_SUMMARY} (written after run)"
    echo ""
    echo "Press Ctrl+C within 5s to abort..."
    sleep 5
    if ((${#extra_args[@]})); then
      python3 zomato_reviews_batch.py \
        --output-dir "${OUTPUT_DIR}" \
        --completed-log "${COMPLETED_LOG}" \
        --failed-log "${FAILED_LOG}" \
        --coverage-report "${COVERAGE}" \
        --retry-status no_dining_reviews processed_no_csv_match \
        --force-retry \
        --rewrite-order-to-info \
        --retry-summary "${RETRY_SUMMARY}" \
        --between-restaurants 1.0 \
        --progress \
        "${extra_args[@]}"
    else
      python3 zomato_reviews_batch.py \
        --output-dir "${OUTPUT_DIR}" \
        --completed-log "${COMPLETED_LOG}" \
        --failed-log "${FAILED_LOG}" \
        --coverage-report "${COVERAGE}" \
        --retry-status no_dining_reviews processed_no_csv_match \
        --force-retry \
        --rewrite-order-to-info \
        --retry-summary "${RETRY_SUMMARY}" \
        --between-restaurants 1.0 \
        --progress
    fi
    echo ""
    echo "=== Regenerating coverage report for rescrape folder ==="
    python3 scripts/build_review_coverage_report.py \
      --reviews-dir "${OUTPUT_DIR}" \
      --completed-log "${COMPLETED_LOG}" \
      --output "${OUTPUT_DIR}/review_coverage_report.csv"
    echo "Done. Rescrape coverage: ${OUTPUT_DIR}/review_coverage_report.csv"
    ;;
  *)
    echo "Usage: $0 {dry-run|run} [extra zomato_reviews_batch.py args...]"
    echo "  e.g. $0 run --limit 50"
    exit 1
    ;;
esac
