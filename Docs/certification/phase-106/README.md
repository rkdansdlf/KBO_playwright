# Phase 106: KBO Crawler Core Operational Certification

**Status**: GATES 106A, 106B, 106C, 106D, 106E CERTIFIED (Scoped Claim Baseline)
**Execution Timestamp**: 2026-09-01T03:35:00+09:00
**Isolation Policy**: STRICT READ-ONLY | ZERO Database Persistence | ZERO Oracle/Production DML
**Protected DB SHA-256**: `f7a7c122ce9656de47957ebfca662d736418fc4ca7e8f0d2255690a1f64bbe30` (100% Unchanged)

---

## 1. Executive Summary & Official Claim Ledger

Phase 106 decouples crawler core operational verification from the Formula/RAG certification track (Phase 105). It validates Playwright collection, offline replay determinism, error injection resilience, ephemeral SQLite database persistence, controlled live read-only smoke contracts, and exhaustive historical data coverage census across all 45 seasons (1982~2026).

### Official Claim Status Table
| Claim Identifier | Operational Scope | Evidence Level | Certification Status |
| :--- | :--- | :---: | :---: |
| `crawler.inventory.30_canonical.v1` | 30 canonical crawlers across 9 domain categories | Level 3 | **PASS_REPORTED** |
| `crawler.deselected_test_taxonomy.242.v1` | 242 tests classified into 8 operational buckets | Level 3 | **PASS_REPORTED** |
| `crawler.default_fast_regression.v1` | Fast profile regression (10,303 passed, 3 skipped, 242 deselected) | Level 3 | **PASS_REPORTED** |
| `crawler.offline_preflight_reconciliation.191.v1` | Exact 191 node reconciliation ($119 + 16 + 56 = 191$) | Level 3 | **LEVEL_3_INTEGRATION_VERIFIED** |
| `crawler.offline_replay.fixture_set_14.v1` | 14 representative offline HTML/JSON fixtures | Level 3 | **LEVEL_3_INTEGRATION_VERIFIED** |
| `crawler.ephemeral_e2e.game_detail_five_tables.v1` | 5 core tables (`game`, `player_game_batting`, `player_game_pitching`, `game_inning_scores`, `ticket_prices`) | Level 3 | **LEVEL_3_INTEGRATION_VERIFIED** |
| `crawler.live_read_only_smoke.kbo_browser_two_targets.v1` | 2 KBO browser targets (`player-search-pagination-contract`, `player-stats-basic2-headers`) | Level 3 | **PASSED (READ-ONLY SMOKE)** |
| `crawler.live_read_only_smoke.secondary_http_one_target.v1` | 1 secondary source (`wikipedia-awards-live`, 495 records, HTTP HTML via httpx) | Level 3 | **PASSED (READ-ONLY SMOKE)** |
| `crawler.live_read_only_smoke.all_30_crawlers.v1` | Full 30 crawlers live certification | - | **NOT_TESTED** |
| `crawler.historical_census.1982_2026.v1` | Read-only coverage census across all 45 seasons (Gate 106E) | Level 3 | **LEVEL_3_INTEGRATION_VERIFIED** |
| `crawler.scheduler_recovery.v1` | Multi-tier locks & auto-healing (Phase 106F) | - | **NO-GO / PENDING APPROVAL** |
| `crawler.oracle_production.v1` | Oracle Staging / Production DML | - | **STRICT NO-GO** |

---

## 2. Test Population & Exact Node Reconciliation

### 1. Repository Test Population Breakdown
$$\text{Total Collected } 10,548 = \text{Selected } 10,306\ (10,303\text{ passed} + 3\text{ skipped}) + \text{Deselected } 242$$

### 2. Gate 106D-0 Offline Preflight Node Equation ($N=191$)
$$\text{Total Executed (191)} = \text{Deselected Offline (46)} + \text{Deselected Parser (1)} + \text{Deselected Repo (72)} + \text{New Cert Tests (16)} + \text{Co-located Fast Tests (56)}$$
- `CRAWLER_OFFLINE_REPLAY`: 46 nodes
- `PARSER_INTEGRATION`: 1 node
- `REPOSITORY_INTEGRATION`: 72 nodes
- `PHASE_106_CERTIFICATION`: 16 nodes (11 replay determinism + 5 ephemeral persistence)
- `CO_LOCATED_FAST_UNIT_TEST`: 56 nodes (fast unit tests inside the same 15 test modules)
- **Unclassified Nodes**: **0** (Saved in `gate-106d-offline-nodes-reconciliation.json` and `gate-106d-offline-selected-tests.txt`).

---

## 3. Gate 106D: Controlled Live Read-Only Smoke Results

Executed exactly 3 approved targets under strict budget caps and resource blocking:

| Target ID | Protocol | Live Target URL | Observed Metric | Status |
| :--- | :---: | :--- | :---: | :---: |
| `player-search-pagination-contract` | Playwright Browser DOM | `https://www.koreabaseball.com/Player/Search.aspx?searchWord=%25` | 20 player rows parsed, next button DOM element verified | **PASS** |
| `player-stats-basic2-headers` | Playwright Browser DOM | `https://www.koreabaseball.com/Record/Player/HitterBasic/Basic2.aspx` | 11/11 Basic2 headers matched (`BB`, `IBB`, `SO`, `OPS`, etc.) | **PASS** |
| `wikipedia-awards-live` | HTTP HTML via httpx (Secondary Authority) | `https://ko.wikipedia.org/wiki/KBO_MVP` | 495 award records parsed across 6 categories | **PASS** |

### Network & Security Summary
- Top-level navigations: **2** (Budget: $\le 3$)
- Total outbound requests: **87** (Allowed: **52**, Blocked by policy: **35**)
- Observed hosts: `www.koreabaseball.com`, `6ptotvmi5753.edge.naverncp.com`, `ko.wikipedia.org`, `www.googletagmanager.com`
- Unexpected hosts: **0**
- Browser page errors on KBO origin: **0**
- Chrome WebUI warnings: Classified as `BROWSER_INTERNAL_WEBUI_WARNING` (gate impact: NONE)
- Database persistence: **0** (All live responses parsed in memory and discarded)
- Protected DB (`data/kbo_dev.db`) pre/post SHA-256: `f7a7c122ce9656de47957ebfca662d736418fc4ca7e8f0d2255690a1f64bbe30` (**100% Unchanged**).

---

## 4. Gate 106E: Historical Coverage Census (1982~2026)

Conducted exhaustive read-only census across all 45 KBO seasons in `data/kbo_dev.db`:

### 1. Overall Database Statistics
- **Total Database Tables**: 90 tables
- **Total Historical Games**: 27,004 games (20,364 COMPLETED + 296 DRAW + 6,321 CANCELLED + 23 SCHEDULED)
- **Closed Seasons (1982~2025)**: 44 seasons (20,534 finalized games with $\ge 95\%$ boxscore coverage)
- **In-Progress Season (2026)**: 1 season (139 games recorded)

### 2. Cross-Table Referential Integrity
```text
player_game_batting orphans : 0
player_game_pitching orphans: 0
game_inning_scores orphans  : 0
game_play_by_play orphans   : 0
duplicate_game_ids          : 0
duplicate_inning_scores     : 0
duplicate_batting_stats     : 0
```
- **Integrity Status**: **100% PASS (Zero Orphans, Zero Natural Key Duplicates)**.

---

## 5. Master Evidence Directory Structure

```
Docs/certification/phase-106/
├── README.md
├── SHA256SUMS
├── checksum-verification.txt
├── crawler-inventory.json
├── crawler-coverage-matrix.json
├── deselected-test-classification.json
├── gate-106d-offline-nodes-reconciliation.json
├── gate-106d-offline-selected-tests.txt
├── replay-fixture-manifest.json
├── replay-results.json
├── ephemeral-e2e-results.json
├── protected-db-hashes.json
├── raw-test-output.txt
├── gate-106d-live-smoke/
│   ├── README.md
│   ├── SHA256SUMS
│   ├── checksum-verification.txt
│   ├── live-smoke-plan.json
│   ├── tested-code-manifest.json
│   ├── network-request-ledger.jsonl
│   ├── browser-console-ledger.jsonl
│   ├── pageerror-ledger.jsonl
│   ├── browser-warning-classification.json
│   ├── response-manifest.json
│   ├── selector-schema-results.json
│   ├── parser-results.json
│   ├── protected-db-hashes.json
│   └── raw-test-output.txt
└── gate-106e-historical-census/
    ├── README.md
    ├── SHA256SUMS
    ├── checksum-verification.txt
    ├── source-applicability-matrix.json
    ├── season-coverage-census.json
    ├── table-row-counts.json
    ├── missing-reason-breakdown.json
    ├── duplicate-natural-keys.json
    ├── orphan-integrity-results.json
    ├── in-progress-season-status.json
    ├── protected-db-hashes.json
    └── raw-query-output.txt
```
