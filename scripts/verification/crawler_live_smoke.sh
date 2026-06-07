#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEFAULT_PYTHON="${ROOT_DIR}/venv/bin/python"
PYTHON_BIN="${CRAWLER_LIVE_SMOKE_PYTHON:-${DEFAULT_PYTHON}}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="python3"
fi

cd "${ROOT_DIR}"
echo "🔎 Running opt-in crawler live smoke..."
echo "Python: ${PYTHON_BIN}"
"${PYTHON_BIN}" -m src.cli.crawler_live_smoke "$@"
