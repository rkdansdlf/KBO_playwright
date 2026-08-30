# Phase 106: KBO Crawler Core Operational Certification

**Status**: COMPLETE (Gates 106A, 106B, 106C Certified)
**Execution Timestamp**: 2026-08-31T02:46:00+09:00
**Isolation Policy**: ZERO Live Network Requests | ZERO Production/Staging DML | ZERO Operational DB Mutations

---

## Executive Summary

Phase 106 separates crawler core operational verification from the Formula/RAG certification track (Phase 105). While Phase 105 completed formula and natural key contracts, Phase 106 establishes end-to-end operational verification across the Playwright collection, offline replay, parser determinism, error injection resilience, and ephemeral database persistence pipelines.

```
+---------------------------------------------------------------------------------------------------+
|                                  PHASE 106 CERTIFICATION TRACK                                   |
+------------------------------------+------------------------------------+-------------------------+
| Gate 106A: Inventory & Taxonomy    | Gate 106B: Offline Snapshot Replay | Gate 106C: Ephemeral E2E |
| - 30 Canonical Crawlers Mapped     | - Triplicate Determinism (H1=H2=H3)| - Ephemeral SQLite DB   |
| - 100% of 242 Deselected Tests     | - 14 Snapshot Fixtures Cataloged   | - Idempotency Re-run 0  |
| - Zero Unclassified Entry Points   | - Fail-Closed Fault Injection Pack | - Atomic Rollback PASS  |
| [PASSED]                           | [PASSED]                           | [PASSED]                |
+------------------------------------+------------------------------------+-------------------------+
```

---

## Gate 106A: Crawler Inventory & Coverage Matrix

All crawler entry points, parsers, services, repositories, and target tables across the codebase were audited.

### 1. Crawler Category Summary
| Category | Crawlers Count | Representative Crawlers | Primary Ingestion Method |
| :--- | :---: | :--- | :--- |
| **SCHEDULE** | 2 | `ScheduleCrawler`, `PreviewCrawler` | Playwright DOM / JSON |
| **GAME_DETAIL** | 4 | `GameDetailCrawler`, `LegacyGameDetailCrawler`, `PBPCrawler`, `NaverRelayCrawler` | Playwright DOM / Static HTML / XHR JSON |
| **ROSTER** | 5 | `DailyRosterCrawler`, `RosterTransactionCrawler`, `PlayerSearchCrawler`, `PlayerProfileCrawler`, `RetiredPlayerListingCrawler` | Playwright DOM |
| **STATS** | 7 | `PlayerBattingCrawler`, `PlayerPitchingCrawler`, `TeamBattingStatsCrawler`, `TeamPitchingStatsCrawler`, `BaserunningStatsCrawler`, `FieldingStatsCrawler`, `ExternalStatsCrawler` | Playwright DOM / HTML / Static Adapter |
| **AWARDS** | 1 | `AwardCrawler` | Static HTML / MediaWiki API |
| **FACILITIES** | 7 | `TeamEventCrawler`, `TicketCrawler`, `FoodCrawler`, `ParkingCrawler`, `SeatCrawler`, `CongestionCrawler`, `TransitTimeCrawler` | Static HTML / REST API JSON |
| **MEDIA** | 1 | `FanCultureCrawler` | YouTube Data API JSON |
| **FUTURES** | 2 | `FuturesScheduleCrawler`, `FuturesProfileCrawler` | Playwright DOM |
| **GENERAL** | 1 | `OperationNoticeNaverCrawler` | REST API JSON / DOM |
| **Total** | **30** | *Full details in `crawler-inventory.json`* | - |

### 2. Deselected Test Taxonomy (242 Tests)
The 242 tests excluded from default fast regression (`pytest.ini` `-m "not slow and not integration"`) have been 100% classified into 8 standard operational buckets:

| Category | Count | Primary Scope & Rationale |
| :--- | :---: | :--- |
| `SCHEDULER_INTEGRATION` | 107 | AutoHealer CLI orchestration (51), Daily update DAG (7), dynamic live polling (32), scheduler shutdown/locks (17) |
| `REPOSITORY_INTEGRATION` | 72 | GameSave extended field updates (57), context aggregators (10), ranking aggregators (2), atomic multi-table persistence (3) |
| `CRAWLER_OFFLINE_REPLAY` | 46 | Game collection batch service (26), relay recovery service (11), pipeline demo fixtures (3), detail stability (2), external stats (2), auth pool (2) |
| `UNRELATED_SLOW_TEST` | 13 | FastAPI endpoints & auth (9), static collection path allowlist (1), embedding cache (1), model registry bootstrap (1), historical analysis (1) |
| `CRAWLER_LIVE_BROWSER` | 2 | Live player search pagination (`test_page52.py`), live Basic2 pitching headers (`test_basic2_headers.py`) |
| `CRAWLER_LIVE_API` | 1 | Live external awards crawl (`TestLiveAwardCrawler::test_live_crawl_counts`) |
| `PARSER_INTEGRATION` | 1 | Baserunning dash handling unit test (`test_baserunning_stats_crawler.py`) |
| `ORACLE_INTEGRATION` | 0 | Dedicated OCI marker used separately |
| **Total** | **242** | **100% Accounted For (Crawler-related: 50 tests)** |

---

## Gate 106B: Offline Snapshot Replay Certification

14 representative offline HTML and JSON fixtures were cataloged and replayed through target parsers.

### 1. Triplicate Replay Determinism ($H_1 = H_2 = H_3$)
Every snapshot fixture was parsed in 3 isolated iterations. Canonical JSON serialization was hashed using SHA-256:

| Fixture ID | Format | Parser Engine | Repetitions | Determinism Status | Canonical SHA-256 |
| :--- | :---: | :--- | :---: | :---: | :--- |
| `game_detail_20251001NCLG0` | HTML | `GameDetailParser` | 3 | **IDENTICAL** | `91e1d0336ae56d54cf8bf10fb4e0f10cb9faee6e1a49f69742a0352ef41ebc9d` |
| `team_batting_2023` | HTML | `parse_team_batting_html` | 3 | **IDENTICAL** | `686256f1f440a3dd9f71c4c810ec548da39bca4ba99fae5e6e3ce45d8aa6d3f2` |
| `team_pitching_2023` | HTML | `parse_team_pitching_html` | 3 | **IDENTICAL** | `6f710534208a0d014022c4d623ea397395a1c97a5b3ee28e6c710db446bf4848` |
| `events_notice_hanwha` | HTML | `parse_team_events` | 3 | **IDENTICAL** | `4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945` |
| `ticket_prices_lg` | HTML | `parse_ticket_page` | 3 | **IDENTICAL** | `03b632fa5a7ff8b0ec3531b262846fc454c68832a81878842915867fc9b57b77` |
| `naver_live_relay_inning_1` | JSON | `NaverRelayJSONParser` | 3 | **IDENTICAL** | `f6ff8655c6ce8e3dc52da91321481b490f23ba5b5505e83ec985b8fae92be76c` |

### 2. Fault Injection Resilience Pack
Parser behavior was tested against malformed and corrupted payloads:
- **Empty HTML without tables**: Fails closed (`ValueError: No tables found`) instead of fabricating empty records.
- **Truncated HTML**: Gracefully captures available structured DOM nodes without crashing.
- **Corrupted Headers**: Discards invalid columns safely without schema corruption.
- **Unsupported Source Keys**: Rejects cleanly and returns empty lists without unhandled exceptions.

---

## Gate 106C: Ephemeral End-to-End Pipeline & Persistence

Complete pipeline testing was conducted using isolated, disposable SQLite databases (`sqlite:///:memory:` and temporary disk fixtures).

### 1. Ingestion & Idempotency Guarantee
- **First Run**: Raw snapshot parsed and successfully persisted into `game`, `player_game_batting`, `player_game_pitching`, `game_inning_scores`, and `ticket_prices`.
- **Second Run**: Exact duplicate payload re-executed through the same repository functions.
- **Idempotency Verification**:
  - Duplicate Natural Keys: **0**
  - Row Count Delta: **0**
  - Unexpected Mutations: **0**

### 2. Transaction Atomicity & Rollback Verification
- Injected simulated crash mid-transaction.
- Verified that all partial writes are rolled back, leaving zero orphaned rows.

### 3. Protected Database Zero-Mutation Guarantee
Pre-run and post-run SHA-256 hashes of the operational database (`data/kbo_dev.db`) were verified:

| File | Size (Bytes) | SHA-256 Hash | Integrity Status |
| :--- | :---: | :--- | :---: |
| `data/kbo_dev.db` | 273,506,304 | `62adc2e3903ae8544a6f625aa9775247bebc1f85c68bf5f29ad96fca6e76c24f` | **UNTOUCHED (READ-ONLY)** |
| `data/backups/kbo_dev_20260830_020000.db` | 273,346,560 | `1a7f0580c85c276329486c99c5658b76a086da361250fa9d2b27072e9a2637eb` | **UNTOUCHED (READ-ONLY)** |

---

## Next Steps (Subsequent Phases)

The following phases involve external site interactions and will only be executed upon explicit user approval:
- **Phase 106D: Limited Live Read-Only Smoke** (KBO live DOM selector validation, 1-3 targets max, zero writes).
- **Phase 106E: Historical Coverage Census** (1982~2026 completeness census across schedules, details, PBP, and rosters).
- **Phase 106F: Scheduler & Recovery Certification** (Multi-tier locking, failure escalation, and automated healing).

---

## Evidence Artifacts Index

All evidence files are located in `Docs/certification/phase-106/`:

1. `README.md` — Complete Phase 106 certification summary and test results.
2. `crawler-inventory.json` — Exhaustive metadata for 30 crawler modules, entrypoints, parsers, and tables.
3. `crawler-coverage-matrix.json` — Coverage matrix mapping test availability per crawler.
4. `deselected-test-classification.json` — 100% classification of the 242 deselected regression tests.
5. `replay-fixture-manifest.json` — Catalog of 14 representative offline HTML/JSON fixtures with SHA-256 hashes.
6. `replay-results.json` — Triplicate determinism execution results and fault injection summary.
7. `ephemeral-e2e-results.json` — End-to-end pipeline ingestion, idempotency, and rollback test results.
8. `protected-db-hashes.json` — Pre/post SHA-256 hashes confirming zero writes to protected databases.
9. `raw-test-output.txt` — Full Pytest test execution output log.
10. `SHA256SUMS` — Cryptographic checksums of all evidence bundle artifacts.
11. `checksum-verification.txt` — Verification output of `shasum -c SHA256SUMS`.
