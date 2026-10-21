# 임시파일 정리 실행 결과 (2026-10-21)

## Phase 1 — 즉시 삭제 안전 (완료)

- `.env.bak_20261013_221338` (시크릿 백업) 삭제
- `__pycache__/` 전역 + `*.pyc`/`*.pyo`/`*.pyd` 삭제 (venv 포함, 재생성됨)
- `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `.coverage` 삭제
- `data/test_runtime_*.db*` 11개 삭제 (6.1MB)
- `scratch/` 전체 삭제 (아카이브: `data/archive/cleanup_phase1/reports_scratch_*.tar.gz`)
- `reports/` 전체 삭제 (아카이브 포함)
- `logs/*.log` 삭제 (아카이브: `data/archive/cleanup_phase1/logs_*.tar.gz`)

## Phase 2 — 1회성 스크립트/프로브 (완료)

루트 (8개, ignored): `bench_common.py`, `diag_conn.py`, `probe_256.py`, `probe_refs_detail.py`, `probe_refs_pg.py`, `test_db_conn.py`, `test_nemotron.py`, `verify_dim256.py` → 아카이브 `cleanup_phase2/root_temp_scripts_*.tar.gz`

investigations (3개, **tracked** → 삭제로 표시):
- `scripts/investigations/probe_62700_pa.py`
- `scripts/investigations/probe_team_code_drift.py`
- `scripts/investigations/probe_scheduler_type_contract.py`
→ 아카이브 `cleanup_phase2/investigations_probes_*.tar.gz`

보존: `check_season_answer_set.py`, `inspect_tool.py`, `probe_season_conflicts.py`, `solve_season_drops.py`

## Phase 3 (부분) — 대용량 산출물 (완료)

- `artifacts/agent-harness/` (63MB, 23 runs) → 아카이브 `cleanup_phase3/artifacts_agent_harness_*.tar.gz` 후 삭제
- `data/backups/` (5.4GB), `data/crawl_evidence/` (226MB), `data/r2_rekey/` (81MB), `data/archive/` (51MB) → **보존** (운영 복구 경로 / replay 기준선)

## Phase 4 — .gitignore 보강 (완료)

- 루트 패턴 `probe_*.py`, `diag_*.py`, `bench_*.py`, `verify_dim*.py`, `test_db_conn.py`, `test_nemotron.py`
- `.env.bak_*`
- `data/test_runtime_*.db*`

## 코드 정리 (검증 통과용)

- `src/reporting/source_inventory.py`: 3곳 `except Exception:` → `except SQLAlchemyError:` (BLE001 해소, import 추가)
- `src/crawlers/game_detail_crawler.py`, `tests/crawlers/test_game_detail_naver_transport.py`: ruff format 적용

## 검증 결과

- `ruff check src/ tests/ scripts/` = **All checks passed!**
- `ruff format --check src/ tests/ scripts/` = **2176 files already formatted**
- `tests/reporting/test_source_inventory.py` = **79 passed**
- `tests/crawlers/test_game_detail_naver_transport.py` = **21 passed**
- `git status`: in-progress 변경 목록 동일, 추적 파일 3개 삭제(`D`) 표시

## 남은 항목 (사용자 결정 필요)

- `data/backups/` (5.4GB): 외부 저장소 이관 후 정리
- `data/crawl_evidence/` (226MB): `kbo snapshot validate` 후 30일 초과 페이로드 정리
- `data/r2_rekey/` (81MB): RAG 재키 작업 완료 확인 후 archive 이동
- `data/archive/`, `data/archives/` (87MB): 90일 초과 스냅샷 정리
- `scripts/maintenance/cleanup_temp_files.py` 자동화 스크립트 작성 (선택)
