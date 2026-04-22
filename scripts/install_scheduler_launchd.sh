#!/usr/bin/env bash
set -euo pipefail

LABEL="com.kbo-playwright.scheduler"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLIST_SRC="${ROOT_DIR}/scripts/launchd/${LABEL}.plist"
PLIST_DEST="${HOME}/Library/LaunchAgents/${LABEL}.plist"
SERVICE="gui/$(id -u)/${LABEL}"
REPLACE_RUNNING="false"

usage() {
  cat <<USAGE
Usage: $0 [--replace-running]

Install the KBO scheduler as a macOS launchd user service.

Options:
  --replace-running  Stop existing scheduler.py processes before loading launchd.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --replace-running)
      REPLACE_RUNNING="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -f "${PLIST_SRC}" ]]; then
  echo "Missing plist: ${PLIST_SRC}" >&2
  exit 1
fi

mkdir -p "${HOME}/Library/LaunchAgents" "${ROOT_DIR}/logs"

if [[ "${REPLACE_RUNNING}" == "true" ]]; then
  launchctl bootout "gui/$(id -u)" "${PLIST_DEST}" >/dev/null 2>&1 || true
  pkill -f "${ROOT_DIR}/scripts/scheduler.py" >/dev/null 2>&1 || true
else
  if pgrep -f "${ROOT_DIR}/scripts/scheduler.py" >/dev/null 2>&1; then
    echo "scheduler.py is already running. Re-run with --replace-running to avoid duplicate schedulers." >&2
    exit 1
  fi
fi

cp "${PLIST_SRC}" "${PLIST_DEST}"
launchctl bootstrap "gui/$(id -u)" "${PLIST_DEST}"
launchctl kickstart -k "${SERVICE}"

echo "Installed launchd service: ${LABEL}"
echo "Plist: ${PLIST_DEST}"
echo "Logs:"
echo "  ${ROOT_DIR}/logs/scheduler.launchd.out.log"
echo "  ${ROOT_DIR}/logs/scheduler.launchd.err.log"
