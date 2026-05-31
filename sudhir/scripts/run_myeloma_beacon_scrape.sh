#!/usr/bin/env bash
# Scrape any sub-forum of myelomabeacon.org/forum/ into data/MyelomaBeacon/<slug>/
#
# Usage:
#   ./scripts/run_myeloma_beacon_scrape.sh <forum-slug> {discover|pilot|run|retry-failed|all} [extra args]
#
# Examples:
#   ./scripts/run_myeloma_beacon_scrape.sh multiple-myeloma       discover
#   ./scripts/run_myeloma_beacon_scrape.sh multiple-myeloma       all
#   ./scripts/run_myeloma_beacon_scrape.sh treatments-side-effects all
#   ./scripts/run_myeloma_beacon_scrape.sh mgus                    pilot

set -euo pipefail
cd "$(dirname "$0")/.."

ROOT="$(pwd)"

FORUM_SLUG="${1:-}"
MODE="${2:-}"
if [[ -z "${FORUM_SLUG}" || -z "${MODE}" ]]; then
  cat <<USAGE
Usage: $0 <forum-slug> {discover|pilot|run|retry-failed|all} [extra args]

  forum-slug     Sub-forum slug (e.g. multiple-myeloma, treatments-side-effects)
  discover       Walk all sub-forum index pages and write topic_urls.txt + topics.csv
  pilot          Scrape the first 5 topics from topic_urls.txt (smoke test)
  run            Scrape all topics (resumable; uses .completed_topics.txt)
  retry-failed   Re-attempt only topics listed in .failed_topics.txt
  all            discover, then run

Output: data/MyelomaBeacon/<forum-slug>/
USAGE
  exit 1
fi
shift 2 || true
# Guard against `set -u` + empty array expansion on bash 3.2 (macOS default).
EXTRA=("${@:-}")
if [[ "${#EXTRA[@]}" -eq 1 && -z "${EXTRA[0]:-}" ]]; then
  EXTRA=()
fi

OUTPUT_DIR="${ROOT}/data/MyelomaBeacon/${FORUM_SLUG}"
mkdir -p "${OUTPUT_DIR}"

discover() {
  echo "=== Myeloma Beacon: Phase 1 (discover topics) — slug=${FORUM_SLUG} ==="
  echo "Output: ${OUTPUT_DIR}/topic_urls.txt, ${OUTPUT_DIR}/topics.csv"
  python3 -m scrapers.myeloma_beacon.discover_topics \
    --forum-slug "${FORUM_SLUG}" \
    --output-dir "${OUTPUT_DIR}" \
    --progress \
    ${EXTRA[@]+"${EXTRA[@]}"}
}

case "${MODE}" in
  discover)
    discover
    ;;
  pilot)
    echo "=== Myeloma Beacon: Phase 2 pilot (5 topics) — slug=${FORUM_SLUG} ==="
    python3 -m scrapers.myeloma_beacon.scrape_topics \
      --forum-slug "${FORUM_SLUG}" \
      --output-dir "${OUTPUT_DIR}" \
      --limit 5 \
      --workers 3 \
      --progress \
      ${EXTRA[@]+"${EXTRA[@]}"}
    ;;
  run)
    echo "=== Myeloma Beacon: Phase 2 full run — slug=${FORUM_SLUG} ==="
    echo "Output:   ${OUTPUT_DIR}/posts.csv"
    echo "Resume:   ${OUTPUT_DIR}/.completed_topics.txt"
    echo "Failures: ${OUTPUT_DIR}/.failed_topics.txt"
    echo ""
    echo "Press Ctrl+C within 5s to abort..."
    sleep 5
    python3 -m scrapers.myeloma_beacon.scrape_topics \
      --forum-slug "${FORUM_SLUG}" \
      --output-dir "${OUTPUT_DIR}" \
      --workers 6 \
      --progress \
      ${EXTRA[@]+"${EXTRA[@]}"}
    ;;
  retry-failed)
    echo "=== Myeloma Beacon: re-running failed topics — slug=${FORUM_SLUG} ==="
    python3 -m scrapers.myeloma_beacon.scrape_topics \
      --forum-slug "${FORUM_SLUG}" \
      --output-dir "${OUTPUT_DIR}" \
      --workers 4 \
      --progress \
      --retry-failed \
      ${EXTRA[@]+"${EXTRA[@]}"}
    ;;
  all)
    discover
    echo ""
    echo "=== Myeloma Beacon: Phase 2 full run — slug=${FORUM_SLUG} ==="
    python3 -m scrapers.myeloma_beacon.scrape_topics \
      --forum-slug "${FORUM_SLUG}" \
      --output-dir "${OUTPUT_DIR}" \
      --workers 6 \
      --progress \
      ${EXTRA[@]+"${EXTRA[@]}"}
    ;;
  *)
    cat <<USAGE
Usage: $0 <forum-slug> {discover|pilot|run|retry-failed|all} [extra args]

  forum-slug     Sub-forum slug (e.g. multiple-myeloma, treatments-side-effects)
  discover       Walk all sub-forum index pages and write topic_urls.txt + topics.csv
  pilot          Scrape the first 5 topics from topic_urls.txt (smoke test)
  run            Scrape all topics (resumable; uses .completed_topics.txt)
  retry-failed   Re-attempt only topics listed in .failed_topics.txt
  all            discover, then run

Output: data/MyelomaBeacon/<forum-slug>/
USAGE
    exit 1
    ;;
esac
