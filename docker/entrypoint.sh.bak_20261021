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
    # /ms-playwright is a large (~1GB) named volume already owned by appuser from
    # the image build; a recursive chown on every start needlessly delays startup
    # (and, under restart: always, crash recovery). Only chown it when the top
    # level ownership is actually wrong. Recursive chown is opt-in for bind
    # mounts because large SQLite data directories can make Docker Desktop
    # exhaust memory while walking the host filesystem.
    for dir in "/app/data" "/app/logs"; do
        if [ -d "$dir" ]; then
            if [[ "${CHOWN_BIND_MOUNTS:-0}" == "1" ]]; then
                current_owner=$(stat -c '%u' "$dir")
                current_group=$(stat -c '%g' "$dir")
                if [ "$current_owner" != "$TARGET_UID" ] || [ "$current_group" != "$TARGET_GID" ]; then
                    echo "🐳 Chowning $dir to appuser:appuser (owner $current_owner:$current_group -> $TARGET_UID:$TARGET_GID)"
                    chown -R appuser:appuser "$dir"
                else
                    echo "🐳 Skipping $dir chown (already owned by $TARGET_UID:$TARGET_GID)"
                fi
            else
                echo "🐳 Skipping $dir recursive chown (set CHOWN_BIND_MOUNTS=1 to enable)"
            fi
        fi
    done
    if [ -d "/ms-playwright" ]; then
        current_owner=$(stat -c '%u' /ms-playwright)
        if [ "$current_owner" != "$TARGET_UID" ]; then
            echo "🐳 Chowning /ms-playwright to appuser:appuser (owner $current_owner -> $TARGET_UID)"
            chown -R appuser:appuser "/ms-playwright"
        else
            echo "🐳 Skipping /ms-playwright chown (already owned by $TARGET_UID)"
        fi
    fi

    # gosu로 appuser 권한으로 재실행
    # UID/GID를 직접 지정해 usermod가 실패(예: UID 충돌)하더라도
    # 호스트 볼륨 소유자 권한으로 실행되도록 보장한다.
    if command -v gosu >/dev/null 2>&1; then
        exec gosu "${TARGET_UID}:${TARGET_GID}" "$0" "$@"
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

run_sqlite_startup_guard() {
    if [[ "${SQLITE_STARTUP_GUARD:-1}" != "1" ]]; then
        return
    fi
    if [[ "${DATABASE_URL:-sqlite:////app/data/kbo_dev.db}" != sqlite* ]]; then
        return
    fi

    GUARD_OUT="/tmp/sqlite_integrity_guard.json"
    echo "🔎 Checking SQLite integrity before startup..."
    if python -m src.cli.sqlite_integrity_guard \
        --database-url "${DATABASE_URL:-sqlite:////app/data/kbo_dev.db}" \
        --action "${SQLITE_CORRUPT_ACTION:-quarantine}" \
        --notify \
        --json > "$GUARD_OUT"; then
        cat "$GUARD_OUT"
    else
        GUARD_CODE=$?
        cat "$GUARD_OUT" || true
        echo "❌ SQLite integrity guard failed with exit code $GUARD_CODE"
        exit "$GUARD_CODE"
    fi

    if grep -q '"status": "quarantined"' "$GUARD_OUT"; then
        export SQLITE_GUARD_QUARANTINED=1
        echo "⚠️ Corrupt SQLite database was quarantined before startup."
    fi
}

run_sqlite_startup_guard

# Optional automatic init_db (기존 설정 유지)
if [[ "${RUN_INIT_DB:-0}" == "1" ]]; then
  echo "🔧 Initializing database..."
  python -c "from src.db.engine import init_db; init_db()"
fi

if [[ "${SQLITE_GUARD_QUARANTINED:-0}" == "1" && "${RUN_INIT_DB:-0}" != "1" ]]; then
  echo "🔧 Recreating empty SQLite schema after quarantine..."
  python -c "from src.db.engine import init_db; init_db()"
fi

# CMD를 그대로 사용하는 경우(기본 --help 등)는 그대로 실행하고,
# docker run으로 CLI 인자만 전달된 경우(예: --season 2026 --month 08)는
# crawl_text_relay 명령으로 감싼다. exec는 옵션으로 시작하는 인자를 명령으로
# 해석할 수 없으므로 직접 조립한다.
if [ "$#" -gt 0 ] && [ "$1" = "python" ]; then
    exec "$@"
fi
exec python -m src.cli.crawl_text_relay "$@"
