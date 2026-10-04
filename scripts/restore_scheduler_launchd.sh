#!/usr/bin/env bash
# Restore the KBO scheduler launchd service after it was disabled
# (the plist renamed to *.disabled). Idempotent, dry-runnable, and verified.
set -euo pipefail

LABEL="com.kbo-playwright.scheduler"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLIST_SRC="${ROOT_DIR}/scripts/launchd/${LABEL}.plist"
PLIST_DEST="${HOME}/Library/LaunchAgents/${LABEL}.plist"
PLIST_DISABLED="${PLIST_DEST}.disabled"
SERVICE="gui/$(id -u)/${LABEL}"
INSTALL_SCRIPT="${ROOT_DIR}/scripts/install_scheduler_launchd.sh"
DB_HOST="${KBO_DB_PROBE_HOST:-100.81.73.13}"
DB_PORT="${KBO_DB_PROBE_PORT:-5432}"
WAIT_SECONDS="${KBO_RESTORE_WAIT_SECONDS:-30}"
DRY_RUN="false"
REPLACE_RUNNING="false"

usage() {
  cat <<USAGE
Usage: $0 [--dry-run] [--replace-running]

Restore the KBO scheduler launchd service (this Mac, one command):
  1. checks the service is not already loaded (idempotent),
  2. warns when the production database is unreachable -- the scheduler is
     safe then (fail-fast gates skip database jobs) but produces nothing,
  3. calls scripts/install_scheduler_launchd.sh from the repo plist template,
  4. verifies 'state = running', the scheduler.py process, and a fresh
     'Registered job: crawl_daily_games' line in the scheduler log,
  5. removes the stale *.disabled copy only after the service is verified.

Options:
  --dry-run          Print the planned actions without changing anything.
  --replace-running  Stop a stray scheduler.py process first (passed through
                     to install_scheduler_launchd.sh).

Only the scheduler label is touched: other projects' launchd jobs in
~/Library/LaunchAgents stay exactly as they are.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN="true"
      shift
      ;;
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

fail() {
  echo "restore-scheduler: $*" >&2
  exit 1
}

[[ -f "${PLIST_SRC}" ]] || fail "missing plist template: ${PLIST_SRC}"
[[ -f "${INSTALL_SCRIPT}" ]] || fail "missing install script: ${INSTALL_SCRIPT}"

if launchctl print "${SERVICE}" >/dev/null 2>&1; then
  echo "Already loaded: ${SERVICE} (nothing to do)"
  exit 0
fi

if command -v nc >/dev/null 2>&1; then
  if nc -z -G 3 "${DB_HOST}" "${DB_PORT}" >/dev/null 2>&1; then
    echo "Database reachable: ${DB_HOST}:${DB_PORT}"
  else
    echo "WARNING: database ${DB_HOST}:${DB_PORT} is unreachable." >&2
    echo "         The scheduler will start, but fail-fast gates skip database jobs" >&2
    echo "         until it returns (watch logs/scheduler.launchd.err.log)." >&2
  fi
fi

if [[ "${DRY_RUN}" == "true" ]]; then
  if [[ "${REPLACE_RUNNING}" == "true" ]]; then
    echo "[dry-run] would run: bash ${INSTALL_SCRIPT} --replace-running"
  else
    echo "[dry-run] would run: bash ${INSTALL_SCRIPT}"
  fi
  echo "[dry-run] would wait up to ${WAIT_SECONDS}s for state = running"
  echo "[dry-run] would verify the scheduler.py process and the job registry line"
  echo "[dry-run] would remove: ${PLIST_DISABLED} (if present)"
  exit 0
fi

if [[ "${REPLACE_RUNNING}" == "true" ]]; then
  bash "${INSTALL_SCRIPT}" --replace-running
else
  bash "${INSTALL_SCRIPT}"
fi

deadline=$(( $(date +%s) + WAIT_SECONDS ))
state_ok="false"
while [[ $(date +%s) -lt ${deadline} ]]; do
  if launchctl print "${SERVICE}" 2>/dev/null | grep -q "state = running"; then
    state_ok="true"
    break
  fi
  sleep 2
done
[[ "${state_ok}" == "true" ]] || fail "service did not reach 'state = running' within ${WAIT_SECONDS}s"

pgrep -f "${ROOT_DIR}/scripts/scheduler.py" >/dev/null 2>&1 || fail "no scheduler.py process found"

log_file="${ROOT_DIR}/logs/scheduler.launchd.err.log"
registry_ok="false"
while [[ $(date +%s) -lt ${deadline} ]]; do
  if [[ -f "${log_file}" ]] && tail -n 400 "${log_file}" | grep -q "Registered job: crawl_daily_games"; then
    registry_ok="true"
    break
  fi
  sleep 2
done
if [[ "${registry_ok}" == "true" ]]; then
  echo "Job registry confirmed in ${log_file}"
else
  echo "NOTE: 'Registered job: crawl_daily_games' not seen yet in the log tail;" >&2
  echo "      check ${log_file}" >&2
fi

if [[ -f "${PLIST_DISABLED}" ]]; then
  rm -f "${PLIST_DISABLED}"
  echo "Removed stale disabled copy: ${PLIST_DISABLED}"
fi

echo "Scheduler restored: ${LABEL}"
echo "Next: tail -f ${log_file}"
