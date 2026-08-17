# KBO Data Platform - Operational Runbook

Last updated: 2026-08-16
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

## 5. Current Verification Blockers (2026-08-16)

- Oracle wallet files, TCP port 1522, and `KBO_APP` Thin-driver connectivity pass. The full Oracle migration chain, including safety-gated files, is applied on the disposable `ADMIN` verification schema, and the isolated live MERGE/UPDATE/idempotency/cleanup verification is `VERIFIED_COMPLETE`.
- The disposable `ADMIN` verification schema passes fresh ORM baseline creation, all safety-gated Oracle migrations, reapplication, `--check`, schema audit (`schema_drift=false`), and the repository rollback/upsert smoke profile.
- CI uses the dedicated `OCI_DB_URL` secret for Oracle acceptance checks. `DATABASE_URL` remains the application primary URL and must not be used by the destructive reset tool.
- The production `KBO_APP` read-only audit currently reports `schema_drift=false` with one pending verification migration (`065_reconcile_model_indexes.sql`); no production migration write has been approved or executed.
- The isolated checkpoint recovery verification is also `VERIFIED_COMPLETE`: initial sync 3 rows, resumed sync 1 row, repeated sync 1 row, final target 4 rows, duplicates 0, and cleanup successful.
- The current Oracle historical audit reports complete game, inning, batting, and pitching row coverage for 1982-2000 with zero duplicates and zero quarantine rows, but the provenance audit classifies the loaded records as deterministic synthetic fixtures; source-verified batting/pitching coverage is 0 for every season.
- The current 8,236 historical metadata payloads do not contain verified manifest provenance fields (`source_url`, `authorization_ref`, and `sha256`). The strengthened audit therefore reports all 1982-2000 seasons as `PARTIAL` until an approved manifest is registered.
- No verified 1982-2000 archive manifest or usage authorization is currently registered. Historical archive writes remain blocked until provenance and permission evidence are recorded.
- Workspace inventory found no `data/archives` directory, archive payload, or `*.manifest.json` file. Checksum validation and `ingest_historical_archive --dry-run` cannot proceed until those inputs are supplied.
- RAG primary corpus inventory is structurally clean (`17` sources, `206,366` generated chunks, zero duplicate/invalid identities and missing metadata), but only `162` canonical `rag_chunks` rows currently exist; `206,273` chunks would be new. Local pgvector migrations `001`-`005` now apply successfully on the isolated Docker service, and the consistency audit reports `primary=162`, `vector=0`, `MISSING_IN_VECTOR=162`. A deterministic staging dry-run for two Markdown chunks passed with `write_enabled=False`; do not run the full index build until the configured embedding budget is approved.

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
