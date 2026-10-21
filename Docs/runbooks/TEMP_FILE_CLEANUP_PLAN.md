# 임시파일 및 1회성 테스트 파일 정리 작업계획

작성일: 2026-10-21
대상 저장소: `/Users/mac/project/KBO_playwright`
목적: 저장소 루트와 `data/`, `scripts/`, `reports/`, `scratch/` 등에 누적된 임시·1회성·생성 산출물과 디버그 파일을 안전하게 식별·분류하고, 검증 가능한 절차로 정리한다. **실제 삭제는 본 계획의 각 단계를 사용자가 승인한 뒤에만 수행한다.**

## 0. 전제 및 안전 원칙

1. **추적(tracked) 파일은 건드리지 않는다.** `git ls-files`에 등록된 파일은 본 계획의 삭제 대상에서 제외한다. 삭제 대상은 모두 `git check-ignore`로 무시되거나 untracked인 파일이다.
2. **진행 중인 작업 트리는 보호한다.** 현재 `git status`에 나타난 16개 수정 파일과 2개 신규 테스트(`tests/crawlers/test_requested_source_url.py`, `tests/services/test_source_url_at_close.py`)는 진행 중인 기능 작업의 일부로 간주하고 정리 대상에서 제외한다.
3. **삭제 전 백업/아카이브.** 복구 불가능한 데이터(DB 백업, crawl_evidence, r2_rekey 등)는 삭제 대신 `data/archive/cleanup_YYYYMMDD/`로 이동하거나 외부 저장소로 이관한다.
4. **단계별 검증.** 각 단계 후 `git status`, `ruff check`, 관련 `pytest` 타깃을 실행해 회귀가 없는지 확인한다.
5. **시크릿 우선 처리.** `.env.bak_20261013_221338` 등 시크릿이 포함된 백업 파일은 가장 먼저 삭제한다.
6. **`data/backups/`(5.4GB)는 본 계획에서 자동 삭제하지 않는다.** 외부 저장소 이관 또는 보존 정책 결정 후 처리한다.

## 1. 현황 요약 (실측)

| 범주 | 위치 | 규모 | git 상태 |
|---|---|---|---|
| Python 캐시 | `**/__pycache__`, `*.pyc` | 105 dirs / 2,109 files | ignored |
| pytest 캐시 | `.pytest_cache` | 1 | ignored |
| 커버리지 산출물 | `.coverage` | 1 | ignored |
| 테스트 런타임 DB | `data/test_runtime_*.db` | 11 files / 6.1MB | ignored (`/data/*`) |
| 루트 임시 스크립트 | 루트 `*.py` (probe/diag/bench/verify/test_*) | 8 files | ignored |
| scratch 작업 산출물 | `scratch/` | 2 dirs / 7 files | ignored |
| 생성 리포트 | `reports/` | 1.1MB / 17 JSON + 디렉터리 | ignored (`/reports/`) |
| 런타임 로그 | `logs/*.log` | 7 files / 24MB | ignored (`*.log`) |
| 시크릿 백업 | `.env.bak_20261013_221338` | 1 | ignored |
| investigations 1회성 프로브 | `scripts/investigations/probe_*.py` | 3 files | ignored (일부) |
| agent-harness 증거 | `artifacts/agent-harness/` | 63MB | ignored |
| 대용량 DB 백업 | `data/backups/` | 5.4GB / 8 files | (확인 필요) |
| crawl 증거 | `data/crawl_evidence/` | 226MB | 운영 필수(보존) |
| RAG 재키 산출물 | `data/r2_rekey/` | 81MB | review 후 결정 |
| 기타 data 하위 | `data/archive`, `archives`, `audit`, `schedules`, ... | 수백 MB | review 후 결정 |

## 2. 분류 기준

- **A. 즉시 삭제 안전 (Regenerable, 고유 가치 없음)**: 캐시, 테스트 런타임 DB, scratch, reports, logs, .coverage, .env.bak.
- **B. 검토 후 삭제/보존 (1회성 스크립트·프로브)**: 루트 임시 스크립트, investigations의 "Temporary diagnostic probe" 라벨 파일.
- **C. 아카이브/이관 후 정리 (대용량·감사 증거)**: artifacts/agent-harness 구버전, data/backups, data/archive, data/crawl_evidence 오래된 페이로드, data/r2_rekey.
- **D. 보존 (운영 필수)**: `data/seed`, `data/player_id_overrides*.csv`, `data/kbo_dev.db`, `data/crawl_evidence`(현행), `vendor/skills/`.

## 3. 단계별 작업계획

### Phase 0 — 사전 점검 (실행 전)

- [ ] `git status` 스냅샷 저장 (현재 in-progress 변경 보호).
- [ ] `git stash list` 및 working tree 변경 파일 목록을 `Docs/runbooks/cleanup_snapshot_YYYYMMDD.txt`로 백업.
- [ ] 삭제 대상이 tracked가 아닌지 `git ls-files <path>`로 재확인.
- [ ] 시크릿 포함 파일(`.env.bak_*`)은 Phase 1에서 최우선 처리.

### Phase 1 — 즉시 삭제 안전 (A) [승인 후 실행]

대상:
1. `.env.bak_20261013_221338` (시크릿) — 즉시 `rm`.
2. `__pycache__/` 전역 + `*.pyc`/`*.pyo`/`*.pyd`.
3. `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `.coverage`, `.coverage.*`, `htmlcov/`.
4. `data/test_runtime_*.db` (+ `-wal`/`-shm`).
5. `scratch/` 전체.
6. `reports/` 전체 (gitignored, 재생성 가능).
7. `logs/*.log` (단, 최근 7일치는 `logs/archive/`로 이동 후 삭제; 또는 전체 삭제 후 스케줄러가 재생성).

예상 확보 용량: ~30MB + scratch/reports.

검증:
- `git status --short`가 Phase 0과 동일한 modified/untracked 목록만 보여야 함.
- `python3 -m pytest tests/ -q -m "not integration and not slow and not oci" -n 2` 통과 (캐시 삭제 후 cold run).

### Phase 2 — 1회성 스크립트/프로브 정리 (B) [검토 후]

루트 (8 files):
- `bench_common.py`, `diag_conn.py`, `probe_256.py`, `probe_refs_detail.py`, `probe_refs_pg.py`, `test_db_conn.py`, `test_nemotron.py`, `verify_dim256.py`

investigations (3 files, "Temporary diagnostic probe" 라벨):
- `scripts/investigations/probe_62700_pa.py`
- `scripts/investigations/probe_team_code_drift.py`
- `scripts/investigations/probe_scheduler_type_contract.py`

처리 방안 (택 1):
- (a) 삭제. 단, `probe_scheduler_type_contract.py`는 `src/scheduler/locks.py`의 `_with_db_fail_fast_guard`를 직접 import하므로 스케줄러 계약 변경 시 회귀 탐지에 쓰일 수 있음 → 삭제 전 `tests/scheduler/`에 동등 테스트가 있는지 확인.
- (b) `scripts/maintenance/` 또는 `scripts/investigations/` 하위에 `archive/` 폴더로 이동해 보존.

재사용 가치 있는 investigations 파일 (보존 권장):
- `check_season_answer_set.py`, `inspect_tool.py`, `probe_season_conflicts.py`, `solve_season_drops.py` — 역사 시즌 작업에 사용. 위치는 유지.

`.gitignore` 보강:
- 루트 패턴 `probe_*.py`, `diag_*.py`, `bench_*.py`, `verify_dim*.py`, `test_db_conn.py`, `test_nemotron.py` 명시 (현재는 광범위 ignore로 커버되지만 명시 권장).
- `scripts/investigations/probe_*.py` 중 "Temporary" 라벨 파일은 삭제 또는 `scripts/investigations/_archive/`로 이동.

검증:
- `ruff check src/ tests/ scripts/` = 0 errors (investigations는 scope 제외지만 scripts/ 루트는 포함).
- `git status`에 삭제된 파일이 tracked 변경으로 나타나지 않아야 함 (ignored 파일이므로 status 변화 없음).

### Phase 3 — 대용량·감사 산출물 정리 (C) [승인 + 백업 후]

1. `artifacts/agent-harness/` (63MB): 30일 이상 된 run 디렉터리만 삭제, 최근 7일치는 보존. 또는 전체를 `data/archive/agent_harness_YYYYMMDD.tar.gz`로 압축.
2. `data/backups/` (5.4GB): 외부 저장소(D: 드라이브 백업 정책)로 이관 후 로컬에서는 최신 1~2개만 유지. **자동 삭제 금지** — 운영 DB 복구 경로.
3. `data/archive/` (51MB), `data/archives/` (36MB): 90일 이상 된 스냅샷/롤백 SQL을 `data/archive/old/`로 이동 또는 tar.gz 압축.
4. `data/crawl_evidence/` (226MB): `CRAWL_EVIDENCE_DIR` 계약상 replay에 필요. 30일 이상 된 `payloads/`/`raw/`만 정리하되, `snapshot_drift_check`가 참조하는 기준선은 보존. 삭제 전 `kbo snapshot validate`로 기준선 확인.
5. `data/r2_rekey/` (81MB): RAG 재키 작업 산출물. 작업 완료 확인 후 `data/archive/`로 이동.
6. `data/schedules/` (55MB): 캐시 성격. 재생성 가능하므로 삭제 가능하나 cold rebuild 비용 확인.

예상 확보 용량: 2~5GB (backups 이관 시).

검증:
- `kbo snapshot validate --limit 100` (drift 0).
- `python3 -m src.cli.kbo incidents list` 등 운영 CLI 정상.
- `ruff check`, pytest targeted.

### Phase 4 — .gitignore 보강 및 정리 자동화

- [ ] `.gitignore`에 명시 패턴 추가:
  - `probe_*.py`, `diag_*.py`, `bench_*.py`, `verify_dim*.py` (루트)
  - `.env.bak_*`
  - `data/test_runtime_*.db*`
  - `scratch/` (이미 존재)
  - `reports/` (이미 존재)
- [ ] `scripts/investigations/_archive/` 디렉터리 생성 + README (보존 정책).
- [ ] (선택) `scripts/maintenance/cleanup_temp_files.py` 작성: 위 Phase 1~3을 `--dry-run`/`--apply` 플래그로 자동화. AGENTS.md의 "dry-run 우선" 원칙 준수. mutation 가드 `KBO_ALLOW_CLEANUP_MUTATION=1` 도입 검토.
- [ ] pre-commit에 `find` 기반 대용량/임시 파일 차단 훅 추가 검토 (`.gitignore` 누락 방지).

### Phase 5 — 최종 검증

- [ ] `git status --short`로 tracked 변경이 Phase 0과 동일한지 확인.
- [ ] `ruff check src/ tests/ scripts/` = 0 errors.
- [ ] `ruff format --check .` clean.
- [ ] `./venv/bin/python -m pytest tests/ -q -m "not integration and not slow and not oci" -n 2` 통과.
- [ ] `python3 scripts/diagnose_scheduler_locks.py` (lock 정상).
- [ ] (운영) `kbo config --env production --strict`, `freshness_gate` 정상.

## 4. 리스크 및 주의사항

1. **crawl_evidence 삭제 시 snapshot replay 불가.** `CRAWL_EVIDENCE_DIR` 계약 위반. 반드시 `kbo snapshot validate` 후 정리.
2. **data/backups 삭제는 운영 복구 경로를 끊는다.** 외부 이관 확인 전 삭제 금지.
3. **in-progress 변경 보호.** Phase 1~3 실행 중 `git stash`/`git checkout` 금지. 삭제는 `rm`만 사용, git 명령으로 working tree 변경을 되돌리지 않는다.
4. **.env.bak 삭제는 시크릿 유출 방지.** 단, 삭제 전 해당 백업이 유일한 시크릿 사본인지 확인 (`.env`가 현재 유효한지 검증).
5. **investigations 파일은 ruff scope 제외.** 삭제해도 lint 영향 없음. 단 역사 시즌 작업 중이면 보존.
6. **vendor/skills는 절대 삭제 금지** (라이선스 검토 통과 자산).

## 5. 권장 실행 순서

1. Phase 0 → Phase 1 (A, 즉시 안전) → 검증.
2. Phase 1 통과 후 Phase 2 (B) 검토·실행 → 검증.
3. Phase 3 (C)는 운영 시간대(새벽)에 백업 이관과 함께 실행.
4. Phase 4 (.gitignore/자동화)는 Phase 1~3 완료 후.
5. Phase 5 최종 검증.

## 6. 산출물

- 본 계획: `Docs/runbooks/TEMP_FILE_CLEANUP_PLAN.md`
- (실행 후) 정리 결과 보고: `Docs/runbooks/TEMP_FILE_CLEANUP_RESULT_YYYYMMDD.md` (삭제 목록, 확보 용량, 검증 결과).

---

### 부록 A: 즉시 삭제 후보 파일 전체 목록 (Phase 1)

```
.env.bak_20261013_221338
.coverage
scratch/  (전체)
reports/  (전체)
logs/*.log  (또는 logs/archive/로 이동)
data/test_runtime_19194_gw0.db
data/test_runtime_19194_gw1.db
data/test_runtime_19375.db
data/test_runtime_21209_gw0.db
data/test_runtime_21209_gw1.db
data/test_runtime_22789_gw0.db
data/test_runtime_22789_gw1.db
data/test_runtime_7515_gw1.db
data/test_runtime_8295_gw0.db
data/test_runtime_8295_gw1.db
data/test_runtime_92230_gw0.db
data/test_runtime_92230_gw1.db
**/__pycache__/  (전체)
*.pyc / *.pyo / *.pyd
```

### 부록 B: 검토 후 삭제 후보 (Phase 2)

```
# 루트
bench_common.py
diag_conn.py
probe_256.py
probe_refs_detail.py
probe_refs_pg.py
test_db_conn.py
test_nemotron.py
verify_dim256.py

# investigations (Temporary diagnostic probe)
scripts/investigations/probe_62700_pa.py
scripts/investigations/probe_team_code_drift.py
scripts/investigations/probe_scheduler_type_contract.py
```

### 부록 C: 보존 대상 (삭제 금지)

```
data/seed/
data/player_id_overrides.csv
data/player_id_row_overrides.csv
data/pa_manual_overrides_2020.csv
data/kbo_dev.db
data/kbo_auth_state.json
data/crawl_evidence/  (현행, replay 기준선)
vendor/skills/
scripts/investigations/check_season_answer_set.py
scripts/investigations/inspect_tool.py
scripts/investigations/probe_season_conflicts.py
scripts/investigations/solve_season_drops.py
```
