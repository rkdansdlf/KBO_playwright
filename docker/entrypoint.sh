#!/bin/bash
set -euo pipefail

# 1. Root privilege handling & Privilege dropping
if [ "$(id -u)" = '0' ]; then
    echo "🐳 Running as root. Adjusting volume permissions..."

    # PUID/PGID 환경변수가 제공되지 않은 경우, 볼륨의 소유자 UID/GID 감지
    TARGET_UID=${PUID:-}
    TARGET_GID=${PGID:-}

    if [ -z "$TARGET_UID" ] && [ -d "/app/data" ]; then
        TARGET_UID=$(stat -c '%u' /app/data)
        TARGET_GID=$(stat -c '%g' /app/data)
        # 감지된 UID가 root(0)인 경우, 쓰기 권한 획득을 위해 기본값 1000으로 조정
        if [ "$TARGET_UID" = "0" ]; then
            TARGET_UID=1000
            TARGET_GID=1000
        fi
    fi

    TARGET_UID=${TARGET_UID:-1000}
    TARGET_GID=${TARGET_GID:-1000}

    echo "🐳 Setting appuser to UID $TARGET_UID, GID $TARGET_GID"

    # appuser의 GID 수정 (그룹이 이미 있으면 변경, 충돌 시 무시)
    if getent group appuser >/dev/null; then
        groupmod -g "$TARGET_GID" appuser || true
    else
        groupadd -g "$TARGET_GID" appuser || true
    fi

    # appuser의 UID 수정
    if getent passwd appuser >/dev/null; then
        usermod -u "$TARGET_UID" -g "$TARGET_GID" appuser || true
    fi

    # 볼륨 디렉토리 소유권 변경
    for dir in "/app/data" "/app/logs" "/ms-playwright"; do
        if [ -d "$dir" ]; then
            echo "🐳 Chowning $dir to appuser:appuser"
            chown -R appuser:appuser "$dir"
        fi
    done

    # gosu로 appuser 권한으로 재실행
    if command -v gosu >/dev/null 2>&1; then
        exec gosu appuser "$0" "$@"
    else
        echo "⚠️ gosu not found, falling back to runuser"
        exec runuser -u appuser -- "$0" "$@"
    fi
fi

# ========================================================
# 2. 비루트(appuser) 진입 시 실행되는 영역 (Privileges Dropped)
# ========================================================
echo "🐳 Running as user $(id -un) ($(id -u):$(id -g))"

# 쓰기 권한 테스트
for dir in "/app/data" "/app/logs"; do
    if [ -d "$dir" ] && [ ! -w "$dir" ]; then
        echo "❌ ERROR: Directory $dir is not writable by $(id -un)."
        echo "Please check host volume permissions or run container as root to auto-fix."
        exit 1
    fi
done

# Optional automatic init_db (기존 설정 유지)
if [[ "${RUN_INIT_DB:-0}" == "1" ]]; then
  echo "🔧 Initializing database..."
  python -c "from src.db.engine import init_db; init_db()"
fi

# SQLite DB 상태 체크 및 자동 하이드레이션 (OCI 백업 DB로부터 복구)
if [[ -n "${OCI_DB_URL:-}" ]]; then
  DB_PATH=""
  # DATABASE_URL에서 SQLite 파일 경로 파싱
  if [[ "${DATABASE_URL:-}" =~ ^sqlite://(.*) ]]; then
      RAW_PATH="${BASH_REMATCH[1]}"
      DB_PATH=$(echo "$RAW_PATH" | sed 's|^/\+||')
      if [[ "$RAW_PATH" == "./"* ]]; then
          DB_PATH="./${DB_PATH#./}"
      else
          DB_PATH="/$DB_PATH"
      fi
  fi
  DB_PATH="${DB_PATH:-/app/data/kbo_dev.db}"

  # SQLite DB를 사용하는 경우에만 자동 하이드레이션 적용
  if [[ "${DATABASE_URL:-}" == *"sqlite"* ]] || [[ -z "${DATABASE_URL:-}" ]]; then
      HYDRATE_REQUIRED=0

      if [ ! -f "$DB_PATH" ] || [ ! -s "$DB_PATH" ]; then
          echo "⚠️ SQLite database file not found or empty at: $DB_PATH. Recovery needed."
          HYDRATE_REQUIRED=1
      else
          # AUTO_HYDRATE=1 인 경우 최종 수정 시간(mtime) 체크
          if [[ "${AUTO_HYDRATE:-0}" == "1" ]]; then
              MTIME_DIFF=$(python -c "
import os, time
db_path = '$DB_PATH'
if os.path.exists(db_path):
    print(int(time.time() - os.path.getmtime(db_path)))
else:
    print(0)
")
              # 기본 간격 24시간(86400초)
              INTERVAL=${HYDRATE_INTERVAL_SEC:-86400}
              if [ "$MTIME_DIFF" -gt "$INTERVAL" ]; then
                  echo "🕒 Database file is older than $((INTERVAL / 3600)) hours (${MTIME_DIFF}s old). Refresh needed."
                  HYDRATE_REQUIRED=1
              fi
          fi

          if [[ "${AUTO_HYDRATE_FORCE:-0}" == "1" ]]; then
              echo "🔄 AUTO_HYDRATE_FORCE is set. Forcing refresh."
              HYDRATE_REQUIRED=1
          fi
      fi

      if [ "$HYDRATE_REQUIRED" -eq 1 ]; then
          LOCKFILE="${DB_PATH}.lock"
          DB_DIR=$(dirname "$DB_PATH")
          mkdir -p "$DB_DIR"

          (
              echo "🔒 Acquiring database lock for hydration: $LOCKFILE"
              if flock -x -w 120 9; then
                  echo "🔑 Lock acquired. Double-checking hydration requirement..."

                  RE_CHECK_REQUIRED=0
                  if [ ! -f "$DB_PATH" ] || [ ! -s "$DB_PATH" ]; then
                      RE_CHECK_REQUIRED=1
                  else
                      MTIME_DIFF=$(python -c "
import os, time
db_path = '$DB_PATH'
if os.path.exists(db_path):
    print(int(time.time() - os.path.getmtime(db_path)))
else:
    print(0)
")
                      INTERVAL=${HYDRATE_INTERVAL_SEC:-86400}
                      if [ "$MTIME_DIFF" -gt "$INTERVAL" ]; then
                          RE_CHECK_REQUIRED=1
                      fi
                      if [[ "${AUTO_HYDRATE_FORCE:-0}" == "1" ]]; then
                          RE_CHECK_REQUIRED=1
                      fi
                  fi

                  if [ "$RE_CHECK_REQUIRED" -eq 1 ]; then
                      echo "🔧 Schema initialization before hydration..."
                      python -c "from src.db.engine import init_db; init_db()"

                      CURRENT_YEAR=$(date +%Y)
                      YEARS_TO_HYDRATE="${HYDRATE_YEARS:-$CURRENT_YEAR}"

                      IFS=',' read -ra ADDR <<< "$YEARS_TO_HYDRATE"
                      HYDRATE_SUCCESS=1
                      for year in "${ADDR[@]}"; do
                          echo "🚚 Running hydrate_runtime_from_oci for season $year..."
                          if python -m src.cli.hydrate_runtime_from_oci --year "$year" --source-url "$OCI_DB_URL"; then
                              echo "✅ Successfully hydrated season $year"
                          else
                              echo "❌ Failed to hydrate season $year"
                              HYDRATE_SUCCESS=0
                          fi
                      done

                      if [ "$HYDRATE_SUCCESS" -eq 1 ]; then
                          touch "$DB_PATH"
                          echo "🎉 Auto-hydration completed successfully."
                      else
                          echo "⚠️ Some hydration tasks failed. Checking DB integrity..."
                      fi
                  else
                      echo "ℹ️ Database hydration already completed by another process. Skipping."
                  fi
              else
                  echo "❌ Failed to acquire flock on $LOCKFILE within 120 seconds. Skipping hydration to avoid deadlock."
              fi
          ) 9>"$LOCKFILE"
      fi
  fi
fi

# Optional sync from source to OCI target (기존 설정 유지)
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
