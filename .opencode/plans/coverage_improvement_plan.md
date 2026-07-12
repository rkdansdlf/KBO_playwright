# Coverage Improvement Plan: 70% → 85%+

**Current State**: 70.70% overall (36,073 stmts), 50 modules < 70%

---

## Tier 1: Quick Wins — Small Modules (<100 stmts, <70% coverage)
**Target**: 1-2 days | **Estimated ROI**: +3-4% overall coverage

| Module | Coverage | Stmts | Strategy |
|--------|----------|-------|----------|
| `src/services/player_status_confirmer.py` | 21.6% | 51 | Pure unit tests with mocked DB/pool |
| `src/crawlers/daily_roster_crawler.py` | 32.7% | 98 | Mock Playwright, test parser logic |
| `src/utils/compliance.py` | 33.0% | 94 | HTTPX mock for compliance checks |
| `src/utils/seoul_api_client.py` | 33.9% | 59 | Mock Seoul API responses |
| `src/utils/map_api_client.py` | 35.7% | 98 | Mock Kakao/Naver/TMAP APIs |
| `src/crawlers/base_naver_crawler.py` | 38.6% | 44 | Test URL construction, headers |
| `src/utils/youtube_api_client.py` | 40.2% | 82 | Mock YouTube API |
| `src/crawlers/team_event_crawler.py` | 43.3% | 90 | Mock HTTP + parser test |
| `src/crawlers/congestion_crawler.py` | 46.0% | 50 | Mock HTTPX response |
| `src/cli/seed_relay_validation_metrics.py` | 46.3% | 67 | CLI smoke test with capsys |
| `src/crawlers/player_list_crawler.py` | 52.2% | 46 | URL/payload verification |
| `src/crawlers/seat_crawler.py` | 58.8% | 85 | Extract pure parser, mock PW |
| `src/crawlers/food_crawler.py` | 60.2% | 88 | Extract pure parser, mock PW |
| `src/crawlers/transit_time_crawler.py` | 60.4% | 48 | Mock transit API |
| `src/crawlers/parking_crawler.py` | 60.7% | 89 | Extract pure parser, mock PW |
| `src/cli/crawl_staff_register.py` | 62.1% | 58 | CLI smoke |
| `src/utils/team_stats_helpers.py` | 63.6% | 88 | Pure function tests |
| `src/cli/crawl_text_relay.py` | 64.4% | 45 | CLI smoke |
| `src/cli/seed_p1_data.py` | 65.4% | 26 | CLI smoke with capsys |
| `src/cli/collect_profiles.py` | 67.2% | 58 | Mock repository |
| `src/crawlers/broadcast_crawler.py` | 68.6% | 70 | Pure URL extraction test |

**Approach**: For crawlers with Playwright — extract pure parsing/URL logic into testable functions, mock `httpx`/`playwright`. For CLI — use `capsys` smoke tests with `--dry-run` or mocked repos.

---

## Tier 2: High-Impact Large Modules (≥500 stmts, <70%)
**Target**: 3-5 days | **Estimated ROI**: +5-7% overall coverage

| Module | Coverage | Stmts | Strategy |
|--------|----------|-------|----------|
| `src/crawlers/player_batting_all_series_crawler.py` | 30.2% | 606 | Extract `parse_basic2_page`, `parse_bb_page`, `parse_brute_page` pure functions; mock HTTPX + HTML fixtures |
| `src/crawlers/player_pitching_all_series_crawler.py` | 43.9% | 581 | Same — extract pure parsers per tab |
| `src/crawlers/game_detail_crawler.py` | 49.5% | 835 | **C901 already refactored** — now add tests for extracted helpers (`_build_hitter_payload`, `_build_pitcher_payload`, `_resolve_player_ids`, `_parse_metadata`). Mock `page.evaluate` |

**Approach**: These already have C901 refactoring done (Phase 10). The extracted pure functions just need test coverage with HTML fixtures.

---

## Tier 3: Medium Modules (100-500 stmts, <70%)
**Target**: 5-7 days | **Estimated ROI**: +4-6% overall coverage

| Priority | Module | Coverage | Stmts | Strategy |
|----------|--------|----------|-------|----------|
| 1 | `src/crawlers/fielding_stats_crawler.py` | 18.6% | 231 | Extract parser, mock PW |
| 2 | `src/crawlers/futures/profile.py` | 22.4% | 156 | Extract pure profile parsing |
| 3 | `src/crawlers/team_info_crawler.py` | 23.4% | 124 | Extract modal parser |
| 4 | `src/crawlers/team_history_crawler.py` | 24.4% | 135 | Extract history table parser |
| 5 | `src/crawlers/player_movement_crawler.py` | 25.2% | 115 | Extract movement table parser |
| 6 | `src/utils/playwright_pool.py` | 25.5% | 137 | Mock pool, test acquire/release |
| 7 | `src/crawlers/game_mvp_crawler.py` | 32.4% | 111 | Mock PW + parser |
| 8 | `src/crawlers/retire/detail.py` | 32.7% | 107 | Extract retired player parser |
| 9 | `src/crawlers/futures/futures_pitching.py` | 39.8% | 103 | Extract pure parser |
| 10 | `src/cli/smart_polling_gate.py` | 40.4% | 141 | Mock Naver API, test gate logic |
| 11 | `src/cli/live_crawler.py` | 40.5% | 447 | Mock orchestrator, test phase logic |
| 12 | `src/crawlers/player_search_crawler.py` | 42.2% | 408 | Extract pagination/search pure logic |
| 13 | `src/crawlers/ticket_crawler.py` | 46.7% | 152 | Extract price parser, mock PW |
| 14 | `src/cli/verify_chunk_quality.py` | 47.0% | 149 | Mock DB, test quality checks |
| 15 | `src/crawlers/team_batting_stats_crawler.py` | 47.6% | 124 | Extract parser |
| 16 | `src/crawlers/pbp_crawler.py` | 50.0% | 200 | Extract PBP parser |
| 17 | `src/cli/auto_healer.py` | 50.9% | 226 | Mock recovery logic |
| 18 | `src/crawlers/kbo_event_crawler.py` | 52.2% | 113 | Mock HTTP + parse |
| 19 | `src/cli/crawler_live_smoke.py` | 52.8% | 144 | Mock live crawler |
| 20 | `src/crawlers/futures/futures_batting.py` | 52.8% | 123 | Extract parser |
| 21 | `src/crawlers/schedule_crawler.py` | 57.7% | 222 | Extract schedule parser |
| 22 | `src/crawlers/staff_register_crawler.py` | 57.8% | 116 | Extract parser |
| 23 | `src/crawlers/team_pitching_stats_crawler.py` | 57.8% | 128 | Extract parser |
| 24 | `src/cli/backfill_starting_pitchers_from_stats.py` | 58.0% | 138 | Mock DB |
| 25 | `src/crawlers/operation_notice_doosan_crawler.py` | 61.9% | 118 | Extract parser |
| 26 | `src/crawlers/text_relay_crawler.py` | 62.3% | 273 | Extract relay parser |
| 27 | `src/cli/monthly_unified_audit.py` | 62.7% | 126 | Mock audit logic |
| 28 | `src/cli/run_advanced_daily.py` | 62.7% | 110 | Mock pipeline |
| 29 | `src/crawlers/operation_notice_lg_crawler.py` | 64.5% | 107 | Extract parser |
| 30 | `src/crawlers/roster_transaction_crawler.py` | 65.2% | 178 | Extract parser |
| 31 | `src/crawlers/retire/listing.py` | 66.7% | 168 | Extract parser |
| 32 | `src/cli/daily_preview_batch.py` | 68.9% | 103 | Mock preview logic |

---

## Tier 4: Zero-Coverage Modules
**Decision needed**: Exclude from coverage or add tests?

| Module | Stmts | Recommendation |
|--------|-------|----------------|
| `src/api/app.py` | 96 | **Exclude** — FastAPI app, not core crawler logic. Add to `pyproject.toml` `omit` |
| `src/cli/api_server.py` | 12 | **Exclude** — CLI entry for API server |
| `src/cli/retry_daily_failures.py` | 124 | Add CLI smoke test (easy) |
| `src/cli/run_all_crawlers.py` | 268 | Add CLI smoke test |
| `src/cli/run_daily_update.py` | 896 | **Exclude** — orchestrator, integration-tested via CI |
| `src/crawlers/static_text_crawler.py` | 80 | Add parser tests if used, else exclude |
| `src/utils/kbo_auth.py` | 68 | Mock Playwright login flow |
| `src/utils/metrics.py` | 15 | **Exclude** — Prometheus metrics, not logic |
| `src/utils/sentry.py` | 16 | **Exclude** — Sentry init boilerplate |

---

## Tier 5: Code Quality Cleanup (2-3 days)
**Not coverage but improves maintainability & reduces false negatives**

### 5.1 Unused `noqa` Cleanup
```bash
ruff check --output-format=concise src/ | grep "unused noqa"
```
Remove stale `# noqa` comments that no longer suppress anything.

### 5.2 Per-File-Ignore Reduction
**Target for removal (if fixed)**:
- `ASYNC109` (async timeout param) — `src/crawlers/player_search_crawler.py`, `src/crawlers/relay_crawler.py`, `src/crawlers/schedule_crawler.py` → use `timeout=...` kwarg correctly
- `PLC0415` (import outside toplevel) — 202 violations globally ignored → audit intentional lazy imports vs fixable
- `TRY300`/`TRY301` — `src/cli/smart_polling_gate.py`, `src/cli/live_crawler.py` → add `else` blocks
- `ARG001`/`ARG002` — unused args in callbacks → prefix with `_` or `# noqa: ARG001` at function level
- `SLF001` — private member access → use public API or `# noqa: SLF001` at line level
- `S608` — shell injection → use `asyncio.create_subprocess_exec` with args list

---

## Tier 6: Flaky Test Stabilization
**Run**: `pytest -x --tb=short -q` 3x in a row → identify intermittent failures
**Likely causes**: DB fixture sharing, async cleanup, Playwright browser state
**Fix**: Add proper fixture isolation (`scope="function"`), async teardown, `asyncio.run` guards

---

## Implementation Order

### Phase 1 (Days 1-2): Tier 1 Quick Wins
- [ ] `player_status_confirmer.py` — pure unit tests
- [ ] `daily_roster_crawler.py` — mock PW, test parser
- [ ] `compliance.py`, `seoul_api_client.py`, `map_api_client.py` — HTTPX mocks
- [ ] `base_naver_crawler.py`, `youtube_api_client.py` — URL/header tests
- [ ] `team_event_crawler.py`, `congestion_crawler.py` — mock + parse
- [ ] CLI smoke tests: `seed_relay_validation_metrics.py`, `crawl_staff_register.py`, `seed_p1_data.py`, `crawl_text_relay.py`, `collect_profiles.py`
- [ ] Parser extraction + tests: `seat_crawler.py`, `food_crawler.py`, `parking_crawler.py`, `broadcast_crawler.py`, `player_list_crawler.py`, `transit_time_crawler.py`
- [ ] Pure function tests: `team_stats_helpers.py`, `result_code_mapper.py`

### Phase 2 (Days 3-5): Tier 2 Large Modules
- [ ] `player_batting_all_series_crawler.py` — extract & test 3 parsers (basic2, bb, brute)
- [ ] `player_pitching_all_series_crawler.py` — extract & test parsers
- [ ] `game_detail_crawler.py` — test extracted C901 helpers

### Phase 3 (Days 6-10): Tier 3 Medium Modules
- [ ] Priority order per table above
- [ ] Focus on crawler parser extraction pattern

### Phase 4 (Days 11-12): Tier 4 Decisions + Tier 5 Cleanup
- [ ] Update `pyproject.toml` coverage excludes for api/cli entry points
- [ ] Run `ruff check --select=unused-noqa` and clean
- [ ] Audit per-file-ignores, convert to line-level where possible
- [ ] Run flaky test detection (3x pytest)

### Phase 5 (Day 13): Verification
```bash
ruff check src/ tests/ scripts/
ruff format --check .
pytest --cov=src --cov-report=term-missing --cov-fail-under=80 -m "not integration"
```

---

## Coverage Exclusions to Add (`pyproject.toml`)

```toml
[tool.coverage.run]
omit = [
    "src/api/*",
    "src/cli/api_server.py",
    "src/cli/run_daily_update.py",
    "src/utils/metrics.py",
    "src/utils/sentry.py",
    "src/crawlers/static_text_crawler.py",  # if unused
]
```

---

## Success Metrics

| Metric | Current | Target |
|--------|---------|--------|
| Overall coverage | ~70% | ≥ 80% |
| Modules < 70% | 50 | < 15 |
| Critical crawlers covered | 3/8 | 8/8 |
| CLI entry points tested | ~20% | 80% |
| Unused noqa | ~50 | 0 |
| Flaky tests | ~20 | 0 |
