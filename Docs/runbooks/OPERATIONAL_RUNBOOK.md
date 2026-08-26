# KBO Data Platform - Operational Runbook

Last updated: 2026-08-17
Release Milestone: `CORE_IMPLEMENTATION_COMPLETE`

---

## 1. Core Runtime Architecture & Pipeline Wiring

시스템은 4대 실시간/자동화 런타임 와이어링 컴포넌트로 구동됩니다:

1. **Multi-Source Reconciler (`AutoHealer`)**:
   - `src.cli.auto_healer` 실행 시 `_reconcile_and_audit()`가 불일치 레코드를 교정하고 `correction_audit_trail` 테이블에 변경 이력을 영구 기록합니다.
2. **Adaptive Polling Engine (`LiveCrawler`)**:
   - `src.cli.live_crawler` 실행 시 경기 상황(이닝, 점수 차, 주자 상황)에 따라 5초~60초 동적 주기를 계산하여 호출합니다.
3. **PBP Realtime Event Stream (`PBPCrawler`)**:
   - `src.crawlers.pbp_crawler`가 KBO 문자중계 이벤트를 파싱할 때 `LivePbpEventStream`을 통해 실시간 메모리 브로드캐스트를 수행합니다.
4. **Selector Drift Sentinel (`Scheduler`)**:
   - `scripts/scheduler.py`의 `selector_drift_sentinel_job`이 매일 05:40 KST에 주요 5개 KBO 페이지 셀렉터를 카나리 점검합니다.

---

## 2. Data Integrity & Quarantine Operations

### 1) 격리 데이터 조회
무결성 위반 레코드는 삭제되지 않고 `quarantined_records` 테이블에 격리됩니다.
```bash
python3 -c "
import sqlite3
conn = sqlite3.connect('data/kbo_dev.db')
c = conn.cursor()
c.execute('SELECT quarantine_id, record_table, violation_rule, violation_reason, quarantined_at FROM quarantined_records')
for r in c.fetchall(): print(r)
"
```

### 2) 격리 데이터 복구 및 재처리
원천 데이터가 수정되거나 재크롤링된 후, 검증을 통과하면 자동으로 격리 해제(`resolved_at` 기록)됩니다.

---

## 3. Historical Lake Ingestion (1982-2000)

### 1) 공식 아카이브 데이터 인제스천
```bash
python3 -m src.cli.ingest_historical_archive \
  --file data/archives/kbo_1982.json \
  --manifest data/archives/kbo_1982.manifest.json \
  --season 1982 --dry-run --json
```

`scripts/converters/convert_kbo_archive_records.py`의 출력은
`synthetic_fixture`이며 공식 historical fact로 적재할 수 없다. 실제 archive
파일, 사용 승인, source URL, SHA-256 manifest를 확인한 뒤에만 `--dry-run`을
통과한 파일을 저장한다.

### 2) 역사 데이터 레이크 전 시즌 전수 감사
```bash
python3 -m src.cli.audit_historical_lake --start-year 1982 --end-year 2000
```

### 3) RAG 인덱스 역사 재인덱스 (프로덕션 갭 해소)

2026-08-23 reconciliation으로 확인된 프로덕션 갭(역사 game·standings 및
player-season identity)을 해소하는 절차. 계약 배경:
`Docs/references/RAG_RECONCILIATION.md`. **임베딩 비용이 발생하므로 dry-run →
샘플 시즌 → 나머지 순으로 단계 실행.** `--season`은 달력 연도(예: `1988`)이며,
빌더가 `kbo_seasons`의 정규시즌 ID를 조회한다. `198800`을 직접 전달하지 않는다.

역사 재색인의 1차 대상은 현재 원본이 존재하는 `games`, `standings`, `batting`,
`pitching`이다. `lineups`, `highlights`, `pbp`는 해당 역사 원본 행이 존재할 때만
추가한다. 2026-08-26 읽기 전용 사전 집계는 생성 14,299청크, 기존 동일 identity
3,409청크, 신규 10,890청크, 이전 stale identity 후보 2,021건이었다.

```bash
# 0) 비용 없는 1개 시즌 파이프라인 검증
RAG_TARGET_ENV=staging python3 -m src.cli.rag.build_rag_index \
  --source games --season 1982 --dry-run --embedding-mode deterministic
RAG_TARGET_ENV=staging python3 -m src.cli.rag.build_rag_index \
  --source batting --season 1982 --dry-run --embedding-mode deterministic
RAG_TARGET_ENV=staging python3 -m src.cli.rag.build_rag_index \
  --source pitching --season 1982 --dry-run --embedding-mode deterministic

# 1) 사전 집계 결과와 현재 target을 확인한 뒤, 승인된 provider로 샘플 적재
#    (OPENROUTER 임베딩 비용 및 production write gate 필요)
python3 -m src.cli.rag.build_rag_index --source games --season 1982 --limit 50 \
  --embedding-mode configured

# 2) 샘플 시즌 실적재 후 reconcile로 확인
python3 -m src.cli.rag.reconcile_rag_stores export --side primary \
  --out reports/rag_reconciliation/primary_1982.ndjson

# 3) 나머지 역사 대상 루프 (--skip-existing은 신규 identity만 처리)
for y in $(seq 1982 2000); do
  for source in games standings batting pitching; do
    python3 -m src.cli.rag.build_rag_index --source "$source" --season "$y" \
      --skip-existing || break 2
  done
done

# 4) 최종 검증: staging 기준 매니페스트와 compare
tar xzf data/archive/workspace_cleanup_20260823/rag_reconciliation_20260823_identity_ndjson.tar.gz \
  -C reports/rag_reconciliation/ staging_sparse_identity.ndjson
python3 -m src.cli.rag.reconcile_rag_stores export --side primary \
  --out reports/rag_reconciliation/primary_after.ndjson
python3 -m src.cli.rag.reconcile_rag_stores compare \
  --left reports/rag_reconciliation/primary_after.ndjson \
  --right reports/rag_reconciliation/staging_sparse_identity.ndjson \
  --fail-on-unexplained
```

롤백: 잘못 인덱싱한 시즌은 `tombstone_rag_chunks`로 무효화한다. 재색인 후
현재 원본에 없는 이전 identity는 stale 후보로 별도 manifest를 만들어 검토한 뒤
tombstone한다. `--skip-existing`은 기존 identity의 내용 변경을 갱신하지 않으므로,
사전 집계에서 `updated`가 있으면 해당 소스는 `--skip-existing` 없이 재색인한다.
player_season_*의 팀 코드 규약은 `Docs/references/RAG_IDENTITY_CONTRACT.md`의
현재 primary DB 저장값 원칙을 따른다.

---

## 4. OCI Live Database Sync & Diagnosis

### 1) OCI 연결 사전 진단
```bash
venv/bin/python3 scripts/diagnose_oci_connection.py --json
```

### 2) Oracle 검증 schema migration
검증 대상은 `OCI_DB_URL`로만 지정합니다. `DATABASE_URL`은 production primary URL이므로
검증 명령에 사용하지 않습니다.
```bash
venv/bin/python -m src.cli.apply_oracle_migrations \
  --url "$OCI_DB_URL" --include-safety-gated
venv/bin/python -m src.cli.apply_oracle_migrations \
  --url "$OCI_DB_URL" --include-safety-gated
venv/bin/python -m src.cli.apply_oracle_migrations \
  --url "$OCI_DB_URL" --include-safety-gated --check
```

### 3) Oracle schema audit
```bash
venv/bin/python scripts/verification/audit_oracle_schema.py \
  --url "$OCI_DB_URL" --json
```

### 4) Fresh disposable schema reset
현재 승인된 대상은 `ADMIN` 검증 schema입니다. 초기화는 destructive하므로 dry-run 확인 후
정확한 confirmation을 요구합니다. Oracle Database Tools 내부 객체는 보존됩니다.
```bash
venv/bin/python scripts/verification/reset_oracle_verification_schema.py \
  --dry-run --json
venv/bin/python scripts/verification/reset_oracle_verification_schema.py \
  --confirm ADMIN --json
```

초기화 후에는 위 migration 명령을 다시 실행합니다.

### 5) OCI repository and live E2E smoke
```bash
KBO_RUN_OCI_INTEGRATION=1 \
  venv/bin/python -m pytest tests/test_oracle_smoke.py \
  -m oci -q -o addopts=''
venv/bin/python scripts/verification/verify_oci_live_sync.py --json
```

---

## 5. Current Verification Status (2026-08-17)

- Oracle wallet files, TCP port 1522, and `KBO_APP` Thin-driver connectivity pass. The full Oracle migration chain, including safety-gated files, is applied on the disposable `ADMIN` verification schema, and the isolated live MERGE/UPDATE/idempotency/cleanup verification is `VERIFIED_COMPLETE`.
- The disposable `ADMIN` verification schema passes fresh ORM baseline creation, all safety-gated Oracle migrations, reapplication, `--check`, schema audit (`schema_drift=false`), and the repository rollback/upsert smoke profile.
- CI uses the dedicated `OCI_DB_URL` secret for Oracle acceptance checks. `DATABASE_URL` remains the application primary URL and must not be used by the destructive reset tool.
- The production `KBO_APP` schema is current through migrations `065_reconcile_model_indexes.sql` and `066_restore_rag_chunk_source_identity_unique.sql`; apply, reapply, and `--check` pass, and the final audit reports `schema_drift=false`.
- The isolated checkpoint recovery verification is also `VERIFIED_COMPLETE`: initial sync 3 rows, resumed sync 1 row, repeated sync 1 row, final target 4 rows, duplicates 0, and cleanup successful.
- The current Oracle historical audit reports complete game, inning, batting, and pitching row coverage for 1982-2000 with zero duplicates and zero quarantine rows, but the provenance audit classifies the loaded records as deterministic synthetic fixtures; source-verified batting/pitching coverage is 0 for every season.
- The current 8,236 historical metadata payloads do not contain verified manifest provenance fields (`source_url`, `authorization_ref`, and `sha256`). The strengthened audit therefore reports all 1982-2000 seasons as `PARTIAL` until an approved manifest is registered.
- No verified 1982-2000 archive manifest or usage authorization is currently registered. Historical archive writes remain blocked until provenance and permission evidence are recorded.
- Workspace inventory found no `data/archives` directory, archive payload, or `*.manifest.json` file. Checksum validation and `ingest_historical_archive --dry-run` cannot proceed until those inputs are supplied.
- RAG source inventory is structurally clean (`17` sources, `207,305` generated chunks, zero duplicate/invalid identities and missing metadata). The isolated recovery staging sparse/configured-vector indexes both contain `207,305` rows with zero orphan, vector-only, hash/version mismatch, missing-embedding, or stale rows and `consistent=true`; full/PBP HNSW plus date/text filter indexes are valid. The configured replay reports resolver-hybrid Recall@5 `0.9818`, MRR `0.8889`, hit rate `1.0`, p95 `386.91ms`, and routing `100/100` with zero false positives. Production RAG will use the Oracle `rag_chunks` table for both sparse and dense state through native `VECTOR(1536, FLOAT32, DENSE)` and the HNSW migration `067_add_rag_vector_search.sql`. Existing PostgreSQL/pgvector results are local staging evidence only. Final Oracle migration application, golden-label confirmation, threshold, cost, and production-write approval remain pending.

## 6. Exit Criteria for `PROJECT_VERIFIED_COMPLETE`

```text
[Historical]
[ ] 1982~2000 공식/허가된 원천 데이터 반입
[ ] synthetic / placeholder 데이터 0건
[ ] provenance 없는 역사 데이터 0건
[ ] 각 시즌 game completeness 검증
[ ] batting/pitching completeness 검증
[ ] duplicates 검증
[ ] quarantine 검토 완료
[ ] audit_historical_lake 1982~2000 통과

[OCI]
[x] Oracle ADB connection PASS
[x] KBO_APP migration ledger current through 066
[x] KBO_APP schema audit `schema_drift=false`
[x] INSERT -> MERGE PASS
[x] Oracle SELECT verification PASS
[x] UPDATE propagation PASS
[x] checkpoint PASS
[x] restart/recovery PASS
[x] idempotency PASS
[x] duplicate 0
[x] cleanup PASS

[Regression]
[x] 전체 pytest PASS
[x] Ruff 0 errors
```
