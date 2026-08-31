# Phase 106: KBO Crawler Core Operational Certification

**Status**: GATES 106A, 106B, 106C, 106D CERTIFIED (Scoped Claim Baseline)
**Execution Timestamp**: 2026-09-01T02:42:00+09:00
**Isolation Policy**: STRICT READ-ONLY | ZERO Database Persistence | ZERO Oracle/Production DML

---

## 1. Executive Summary & Claim Ledger

Phase 106 separates crawler core operational verification from the Formula/RAG certification track (Phase 105). It validates Playwright collection, offline replay, parser determinism, error injection resilience, ephemeral SQLite database persistence, and controlled live read-only smoke contracts.

### Official Claim Status Table
| Claim Identifier | Operational Scope | Evidence Level | Certification Status |
| :--- | :--- | :---: | :---: |
| `crawler.inventory.30_canonical.v1` | 30 canonical crawlers across 9 domain categories | Level 3 | **PASS_REPORTED** |
| `crawler.deselected_test_taxonomy.242.v1` | 242 tests classified into 8 operational buckets | Level 3 | **PASS_REPORTED** |
| `crawler.offline_replay.fixture_set_14.v1` | 14 representative offline HTML/JSON fixtures | Level 3 | **LEVEL_3_INTEGRATION_VERIFIED** |
| `crawler.offline_replay.all_30.v1` | 30 crawler offline replay completeness | - | **PARTIAL** |
| `crawler.ephemeral_e2e.game_detail_five_tables.v1` | 5 core tables (`game`, `player_game_batting`, `player_game_pitching`, `game_inning_scores`, `ticket_prices`) | Level 3 | **LEVEL_3_INTEGRATION_VERIFIED** |
| `crawler.ephemeral_e2e.all_domains.v1` | End-to-end ephemeral pipeline for all 30 crawlers | - | **PARTIAL** |
| `crawler.live_read_only_smoke.v1` | 3 approved live targets (`test_page52`, `test_basic2_headers`, `TestLiveAwardCrawler`) | Level 3 | **PASSED (READ-ONLY SMOKE)** |
| `crawler.historical_census.v1` | Historical 1982~2026 coverage census (Phase 106E) | - | **NO-GO / PENDING APPROVAL** |
| `crawler.scheduler_recovery.v1` | Multi-tier locks & auto-healing (Phase 106F) | - | **NO-GO / PENDING APPROVAL** |
| `crawler.oracle_production.v1` | Oracle Staging / Production DML | - | **STRICT NO-GO** |

---

## 2. Gate 106A: Crawler Inventory & Deselected Test Taxonomy

All 30 crawler entry points, parsers, services, repositories, and target tables across the codebase are audited with full in-tree provenance scripts (`scripts/certification/phase106/`).

### 1. Crawler Inventory Summary
```json
{
  "canonical_crawlers": 30,
  "crawler_with_fixture": 10,
  "crawler_without_fixture": 20,
  "crawler_replay_verified": 10,
  "crawler_fault_injection_verified": 10,
  "crawler_ephemeral_persistence_verified": 2
}
```

### 2. 242 Deselected Test Classification
```
SCHEDULER_INTEGRATION    : 107 tests (AutoHealer CLI, 일일 동기화 DAG, 실시간 폴링, 프로세스 락)
REPOSITORY_INTEGRATION   :  72 tests (GameSave 확장 필드 업데이트, 컨텍스트 집계, 트랜잭션 원자성)
CRAWLER_OFFLINE_REPLAY   :  46 tests (경기 수집 서비스, 중계 복구 서비스, 파이프라인 데모)
UNRELATED_SLOW_TEST      :  13 tests (FastAPI 엔드포인트, 임베딩 캐시, 모델 레지스트리, 허용 목록 검사)
CRAWLER_LIVE_BROWSER     :   2 tests (선수 검색 페이지네이션 test_page52.py, 투수 Basic2 헤더 test_basic2_headers.py)
CRAWLER_LIVE_API         :   1 test  (수상 내역 라이브 수집 TestLiveAwardCrawler)
PARSER_INTEGRATION       :   1 test  (주루 기록 파서 test_baserunning_stats_crawler.py)
ORACLE_INTEGRATION       :   0 tests (별도 OCI 마커 격리)
---------------------------------------------------------------------------------------------
총합                      : 242 tests (크롤러 코어 및 직접 연관 테스트: 50개)
```

---

## 3. Gate 106B & 106C: Offline Replay & Ephemeral Persistence

### 1. Offline Replay Determinism ($H_1 = H_2 = H_3$)
14 representative fixtures cataloged in `replay-fixture-manifest.json` were parsed across 3 isolated iterations, producing identical canonical SHA-256 output hashes.

### 2. Technical Outbound Network Denial Preflight (106D-0)
Executed 119 offline tests (47 offline crawler/parser tests + 72 repository integration tests) under socket-level non-loopback network blocking (`socket.socket.connect` raises exception for external hosts):
- **Result**: **191 passed in 11.27s (0 failures, 0 errors)**
- **Protected DB SHA-256**: `62adc2e3903ae8544a6f625aa9775247bebc1f85c68bf5f29ad96fca6e76c24f` (Zero DB writes).

### 3. Ephemeral Persistence (5 Representative Tables)
Ingested game details and ticket prices into isolated SQLite DB:
- Re-run duplicate keys: **0**
- Row count inflation: **0**
- Injected fault rollback: **100% atomicity confirmed**

---

## 4. Full Fast Regression Test Suite Baseline

Executed fast regression across all available cores:
```bash
pytest -n auto -q
```
- **Collected**: 10,306 tests (242 deselected)
- **Executed**: 10,303 tests
- **Passed**: 10,303 tests
- **Skipped**: 3 tests
- **Failed / Errors**: **0**
- **Duration**: 111.32s

---

## 5. Gate 106D: Controlled Live Read-Only Smoke Results

Executed exactly 3 approved live targets under budget caps:

| Target ID | Type | Live Target URL | Observed Metric | Status |
| :--- | :---: | :--- | :---: | :---: |
| `player-search-page52` | Browser | `https://www.koreabaseball.com/Player/Search.aspx?searchWord=%25` | 20 player rows parsed, next button verified | **PASS** |
| `player-stats-basic2-headers` | Browser | `https://www.koreabaseball.com/Record/Player/HitterBasic/Basic2.aspx` | 11/11 Basic2 headers matched (`BB`, `IBB`, `SO`, `OPS`, etc.) | **PASS** |
| `wikipedia-awards-live` | HTTP API | `https://ko.wikipedia.org/wiki/KBO_MVP` | 495 award records parsed across 6 award categories | **PASS** |

### Execution Invariants Enforced
- Top-level navigations: **2** (Budget: $\le 3$)
- Live API/XHR calls: **1** (Budget: $\le 10$)
- Resource blocking: Images, fonts, media, analytics, and ads blocked
- Database writes: **0** (In-memory parse $\rightarrow$ SHA-256 $\rightarrow$ discard)
- Oracle / Staging / Production connections: **0**
- Protected DB (`data/kbo_dev.db`) pre/post SHA-256: `62adc2e3903ae8544a6f625aa9775247bebc1f85c68bf5f29ad96fca6e76c24f` (**100% UNCHANGED**).

---

## 6. Evidence Artifacts Index

### Top-Level Evidence (`Docs/certification/phase-106/`)
- `README.md` — Phase 106 certification summary and claim ledger.
- `crawler-inventory.json` — 30 canonical crawlers inventory with entry points and tables.
- `crawler-coverage-matrix.json` — Test coverage matrix per crawler.
- `deselected-test-classification.json` — 242 deselected tests classified into 8 categories.
- `replay-fixture-manifest.json` — 14 representative offline fixtures with SHA-256 hashes.
- `replay-results.json` — Triplicate determinism and fault injection results.
- `ephemeral-e2e-results.json` — Ephemeral DB persistence and idempotency verification.
- `protected-db-hashes.json` — Pre/post database hashes verifying zero mutations.
- `raw-test-output.txt` — Raw Pytest execution logs.
- `SHA256SUMS` & `checksum-verification.txt` — Checksums for top-level bundle.

### Gate 106D Evidence Bundle (`Docs/certification/phase-106/gate-106d-live-smoke/`)
- `live-smoke-plan.json` — Budget caps and abort condition parameters.
- `tested-code-manifest.json` — Target modules and underlying crawler classes.
- `network-request-ledger.jsonl` — Timestamped record of all allowed and blocked network requests.
- `response-manifest.json` — HTTP status, response SHA-256, and sizes for each target.
- `selector-schema-results.json` — Live DOM selector verification details.
- `parser-results.json` — Records extracted in-memory.
- `protected-db-hashes.json` — DB hash preservation evidence.
- `raw-test-output.txt` — Raw console output of the live smoke run.
- `git-status-before.txt` & `git-status-after.txt` — Git porcelain status captures.
- `SHA256SUMS` & `checksum-verification.txt` — Checksums for Gate 106D bundle.
