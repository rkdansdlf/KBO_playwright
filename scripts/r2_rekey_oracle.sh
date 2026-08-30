#!/usr/bin/env bash
# R2 Natural-Key Rekey: Oracle Production Execution Script
# 실행 전 필수 환경 변수 설정 필요

set -euo pipefail

# =============================================================================
# 설정 (실행 전 반드시 수정)
# =============================================================================
ORACLE_TNS="<staging_or_prod_tns_alias>"           # 예: etyqpnpj0l1ep777_medium
WALLET_PATH="/path/to/oracle/wallet"               # 예: /opt/oracle/wallet
DB_USER="KBO_APP"
RAG_INDEX_VERSION="rag-v2"
OUTPUT_DIR="/data/r2_rekey"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
MANIFEST_FILE="${OUTPUT_DIR}/r2_manifest_${TIMESTAMP}.json"

# =============================================================================
# 환경 변수 설정
# =============================================================================
export DATABASE_URL="oracle+oracledb://${DB_USER}@${ORACLE_TNS}"
export TNS_ADMIN="${WALLET_PATH}"
export RAG_INDEX_VERSION="${RAG_INDEX_VERSION}"
export RAG_INDEX_ALLOW_WRITE=1
export RAG_INDEX_ALLOW_PRODUCTION_WRITE=1
export R2_REKEY_OUTPUT_DIR="${OUTPUT_DIR}"

# 출력 디렉토리 생성
mkdir -p "${OUTPUT_DIR}"

echo "========================================="
echo "R2 Rekey Production Execution"
echo "========================================="
echo "Database: ${DATABASE_URL}"
echo "Wallet: ${TNS_ADMIN}"
echo "Manifest: ${MANIFEST_FILE}"
echo "Output Dir: ${OUTPUT_DIR}"
echo "========================================="

# =============================================================================
# Phase 1: Census & Manifest 생성
# =============================================================================
echo ""
echo "[Phase 1] Census & Manifest Generation"
echo "-----------------------------------------"

if ! python -m src.cli.kbo rag census \
  --dry-run \
  --json \
  --output "${MANIFEST_FILE}" \
  --sample 50; then
    echo "ERROR: Census failed"
    exit 1
fi

echo "Manifest generated: ${MANIFEST_FILE}"
echo "Validating manifest..."

# Manifest header 검증
cat "${MANIFEST_FILE}" | jq '.manifest_header'
cat "${MANIFEST_FILE}" | jq '.totals'
cat "${MANIFEST_FILE}" | jq '.sources[] | {source_table, legacy_numeric_rows, safe_rekey_candidates, orphan_rows, collision_rows}'

read -p "Manifest looks correct? Continue to dry-run? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted by user"
    exit 1
fi

# =============================================================================
# Phase 2: Dry-run Apply (Compare-and-Set 검증)
# =============================================================================
echo ""
echo "[Phase 2] Dry-run Apply (Compare-and-Set Validation)"
echo "-----------------------------------------"

if ! python -m src.cli.rag.apply_rag_rekey \
  --manifest "${MANIFEST_FILE}" \
  --json; then
    echo "ERROR: Dry-run failed"
    exit 1
fi

read -p "Dry-run successful? Continue to production apply? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted by user"
    exit 1
fi

# =============================================================================
# Phase 3: Canary Apply (Source별 순차 적용)
# =============================================================================
echo ""
echo "[Phase 3] Canary Apply (Source별 순차 적용)"
echo "-----------------------------------------"

SOURCES_ORDER=(
    "futures_schedule"
    "awards"
    "team_history"
    "player_movements"
    "game_highlights"
    "game_play_by_play"
)

for SOURCE in "${SOURCES_ORDER[@]}"; do
    echo ""
    echo ">>> Applying ${SOURCE}..."

    # Single source manifest 추출
    SINGLE_MANIFEST="${OUTPUT_DIR}/r2_manifest_${SOURCE}_${TIMESTAMP}.json"

    python -c "
import json
with open('${MANIFEST_FILE}') as f:
    m = json.load(f)
m['entries'] = [e for e in m['entries'] if e['source_table'] == '${SOURCE}']
m['source_tables'] = ['${SOURCE}']
m['sources'] = [s for s in m['sources'] if s['source_table'] == '${SOURCE}']
# Update totals
total = m['totals']
src = m['sources'][0] if m['sources'] else {}
total['source_rows'] = src.get('source_rows', 0)
total['legacy_numeric_rows'] = src.get('legacy_numeric_rows', 0)
total['legacy_non_numeric_rows'] = src.get('legacy_non_numeric_rows', 0)
total['safe_source_matches'] = src.get('safe_source_matches', 0)
total['safe_rekey_candidates'] = src.get('safe_rekey_candidates', 0)
total['existing_natural_target'] = src.get('existing_natural_target', 0)
total['orphan_rows'] = src.get('orphan_rows', 0)
total['collision_keys'] = src.get('collision_keys', 0)
total['collision_rows'] = src.get('collision_rows', 0)
total['source_rows_missing_in_index'] = src.get('source_rows_missing_in_index', 0)
total['unsafe_entry_count'] = sum(1 for e in m['entries'] if e.get('disposition') != 'SAFE_REKEY')
with open('${SINGLE_MANIFEST}', 'w') as f:
    json.dump(m, f, ensure_ascii=False, indent=2)
"

    echo "Applying ${SOURCE} (${SINGLE_MANIFEST})..."

    if ! python -m src.cli.rag.apply_rag_rekey \
      --manifest "${SINGLE_MANIFEST}" \
      --apply --json; then
        echo "ERROR: Apply failed for ${SOURCE}"
        exit 1
    fi

    # Post-apply 검증
    echo "Post-apply validation for ${SOURCE}..."
    python -m src.cli.kbo rag census --source "${SOURCE}" --dry-run --json
    python -m src.cli.rag.audit_rag_index --require-postings --json
    python -m src.cli.rag.audit_rag_tombstones --fail-on-unexplained --json

    echo ">>> ${SOURCE} complete. Sleeping 10s..."
    sleep 10
done

# =============================================================================
# Phase 4: 전체 사후 검증
# =============================================================================
echo ""
echo "[Phase 4] Full Post-Apply Validation"
echo "-----------------------------------------"

python -m src.cli.rag.audit_rag_index --require-postings --json
python -m src.cli.rag.audit_rag_tombstones --fail-on-unexplained --json
python -m src.cli.kbo rag evaluate --golden-path Docs/references/rag_golden_queries.json --json

# 전체 census 재실행
python -m src.cli.kbo rag census --dry-run --json --output "${OUTPUT_DIR}/r2_post_apply_manifest_${TIMESTAMP}.json"

echo ""
echo "========================================="
echo "R2 Rekey Production Execution Complete"
echo "========================================="
echo "Manifest: ${MANIFEST_FILE}"
echo "Post-apply manifest: ${OUTPUT_DIR}/r2_post_apply_manifest_${TIMESTAMP}.json"
echo "Preimages/Rollbacks: ${OUTPUT_DIR}/"
echo "========================================="
