#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

cd "${ROOT_DIR}"
echo "🚦 Running crawler release check..."
"${ROOT_DIR}/scripts/verification/crawler_stability_gate.sh" "$@"

if [[ "${KBO_LIVE_SMOKE:-}" == "1" ]]; then
  if [[ -z "${KBO_LIVE_SMOKE_DATE:-}" ]]; then
    echo "KBO_LIVE_SMOKE_DATE is required when KBO_LIVE_SMOKE=1" >&2
    exit 2
  fi

  smoke_args=(
    "--date" "${KBO_LIVE_SMOKE_DATE}"
    "--scope" "${KBO_LIVE_SMOKE_SCOPE:-all}"
    "--limit" "${KBO_LIVE_SMOKE_LIMIT:-1}"
    "--allow-network"
  )
  if [[ -n "${KBO_LIVE_SMOKE_GAME_ID:-}" ]]; then
    smoke_args+=("--game-id" "${KBO_LIVE_SMOKE_GAME_ID}")
  fi

  echo "🔎 Running opt-in live smoke for ${KBO_LIVE_SMOKE_DATE}..."
  "${ROOT_DIR}/scripts/verification/crawler_live_smoke.sh" "${smoke_args[@]}"
else
  echo "ℹ️ Skipping live smoke. Set KBO_LIVE_SMOKE=1 and KBO_LIVE_SMOKE_DATE=YYYYMMDD to enable."
fi
