#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEFAULT_PYTHON="${ROOT_DIR}/venv/bin/python"
PYTHON_BIN="${CRAWLER_STABILITY_PYTHON:-${DEFAULT_PYTHON}}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="python3"
fi

TEST_TARGETS=(
  "tests/test_schedule_crawler_stability.py"
  "tests/test_schedule_collection_service.py"
  "tests/test_schedule_season_id_mapping.py"
  "tests/test_game_detail_crawler_stability.py"
  "tests/test_game_detail_crawler_roster_fallback.py"
  "tests/test_request_throttle.py"
  "tests/test_playwright_retry.py"
  "tests/test_naver_relay_resolver.py"
  "tests/test_relay_recovery_service.py"
  "tests/test_relay_recovery.py"
  "tests/test_oci_sync_dirty_detection.py"
  "tests/test_run_daily_update.py"
  "tests/test_p0_readiness.py"
  "tests/test_broadcast_crawler.py"
  "tests/test_roster_transaction_crawler.py"
  "tests/test_retry_daily_failures.py"
  "tests/test_crawler_live_smoke.py"
  "tests/test_crawler_release_check.py"
  "tests/test_refresh_manifest.py"
  "tests/test_scheduler_alerting.py"
  "tests/test_game_collection_service.py"
  "tests/test_fixture_ingest_clis.py"
  "tests/test_game_id_normalization.py"
)

usage() {
  cat <<USAGE
Usage: $0 [--print-targets] [pytest args...]

Run the crawler stability regression gate for schedule, detail, relay/PBP,
OCI publish eligibility, and daily update orchestration.

Options:
  --print-targets  Print pytest target files and exit.
  -h, --help       Show this help.

Environment:
  CRAWLER_STABILITY_PYTHON  Python executable to use. Defaults to ./venv/bin/python.
  CRAWLER_SELECTOR_GATE_CONFIG  Selector gate JSON config. Defaults to Docs/references/crawler_selector_gate.json.
  CRAWLER_SELECTOR_GATE_OUTPUT_DIR  Selector artifacts directory. Defaults to output/playwright/selector-gate.
USAGE
}

if [[ "${1:-}" == "--print-targets" ]]; then
  printf '%s\n' "${TEST_TARGETS[@]}"
  exit 0
fi

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

cd "${ROOT_DIR}"
echo "🚦 Running crawler stability gate..."
echo "Python: ${PYTHON_BIN}"
"${PYTHON_BIN}" -m pytest "${TEST_TARGETS[@]}" -q "$@"

SELECTOR_CONFIG="${CRAWLER_SELECTOR_GATE_CONFIG:-Docs/references/crawler_selector_gate.json}"
SELECTOR_OUTPUT_DIR="${CRAWLER_SELECTOR_GATE_OUTPUT_DIR:-output/playwright/selector-gate}"
if [[ -f "${SELECTOR_CONFIG}" ]]; then
  echo "🔎 Running crawler selector gate..."
  "${PYTHON_BIN}" -m src.cli.crawler_selector_gate \
    --config "${SELECTOR_CONFIG}" \
    --output-dir "${SELECTOR_OUTPUT_DIR}" \
    --json
else
  echo "Skipping crawler selector gate: config not found (${SELECTOR_CONFIG})"
fi
