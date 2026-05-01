#!/bin/bash
set -euo pipefail

# Optional automatic init_db
if [[ "${RUN_INIT_DB:-0}" == "1" ]]; then
  echo "🔧 Running init_db.py"
  python init_db.py
fi

# Optional sync from source to OCI target (e.g., SQLite -> OCI PostgreSQL)
if [[ "${RUN_SYNC_OCI:-0}" == "1" ]]; then
  : "${TARGET_DATABASE_URL:=${OCI_DB_URL:-}}"
  if [[ -z "${TARGET_DATABASE_URL}" ]]; then
    echo "❌ RUN_SYNC_OCI=1 requires TARGET_DATABASE_URL or OCI_DB_URL"
    exit 1
  fi
  SRC_URL="${SOURCE_DATABASE_URL:-sqlite:///./data/kbo_dev.db}"
  echo "🚚 Syncing from ${SRC_URL} to ${TARGET_DATABASE_URL}"
  python -m src.cli.sync_oci \
    --source-url "${SRC_URL}" \
    --target-url "${TARGET_DATABASE_URL}" \
    $( [[ "${SYNC_TRUNCATE:-0}" == "1" ]] && echo "--truncate" )
fi

exec "$@"
