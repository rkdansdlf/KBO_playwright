# Repository Guidelines

## Project Structure & Module Organization
This repository is a Playwright-based KBO data crawler with a two-track pipeline:
- `src/`: Core application code (crawlers, parsers, models, repositories, CLI).
- `scripts/`: Maintenance and batch utilities (crawling, oci, maintenance).
- `tests/`: Pytest tests and debug scripts (`test_*.py` run by default).
- `Docs/`: Runbooks, URL references, limitations, and schemas.
- `migrations/`: Schema migrations (including OCI).
- `data/`, `logs/`: Local SQLite DB and runtime logs.

## Agent Skill Defaults
Agents should apply the repository's crawler-oriented skill set automatically; the user should not need to invoke these skills one by one.
- Default crawler change workflow: use `$playwright` for browser-backed selector, DOM, screenshot, and extraction checks; use `$systematic-debugging` for crawler failures, selector drift, parser errors, timing issues, DB writes, and unexpected data quality changes; use `$test-driven-development` for feature work, bug fixes, refactors, parser changes, repository changes, and behavior changes; use `$verification-before-completion` before claiming a fix is complete.
- Do not force every skill onto every task. For documentation-only or configuration-only changes, use only the relevant subset and still verify the edited files.
- For GitHub Actions failures, CI regressions, or workflow log investigation, use `$gh-fix-ci`.
- For PR, issue, branch, or repository triage, use `$github`; route to the more specific GitHub skill once the task is clear.
- For security review of a PR, commit, branch diff, or working-tree patch, use `$security-diff-scan`; for repository-wide or scoped-path security audits, use `$security-scan`.
- For large crawler, scheduler, backfill, OCI sync, or GitHub Actions changes spanning multiple modules, use `$writing-plans` before editing.
- For quality gate, freshness, gap report, or data quality summaries that need analytical presentation, use the Data Analytics reporting or visualization skills when useful.
- Verification should normally include the narrowest relevant `pytest` target plus `ruff check src/ tests/`; for CLI behavior, prefer a dry-run or read-only command before any save/sync operation.

## Build, Test, and Development Commands
- `python3 -m venv venv && source venv/bin/activate`: Create and activate virtual environment.
- `pip3 install -r requirements.txt`: Install Python dependencies.
- `playwright install chromium`: Install Playwright browser binaries.
- `python3 scripts/scheduler.py`: Run the automated scheduler.
- `python3 -m src.cli.crawl_schedule --year 2025 --month 3`: Crawl schedule data.
- `python3 -m src.cli.collect_games --year 2024 --month 10`: Collect game details.
- `python3 -m src.cli.crawl_futures --season 2025 --concurrency 3`: Crawl Futures stats.
- `python3 -m src.cli.crawl_futures --season 2025 --concurrency 3 --changed-since "2026-06-03"`: Incremental Futures crawl (skip recently updated players).
- `python3 -m src.cli.sync_oci --truncate`: Sync local SQLite to OCI.
- `python3 -m src.cli.crawl_p0_data --save`: Collect all P0 data (events + roster + ticket).
- `python3 -m src.cli.crawl_team_events --save`: Collect team events/news only.
- `python3 -m src.cli.crawl_roster_transactions --save`: Collect daily roster transactions.
- `python3 -m src.cli.crawl_ticket_info --save`: Collect ticket prices/open rules.
- `python3 -m src.cli.seed_data_sources`: Seed initial DataSource entries.
- `python3 -m src.cli.seed_p1_data`: Seed P1 data (seat + parking + food).
- `python3 scripts/seed_seat_sections.py`: Seed stadium seat sections.
- `python3 scripts/seed_parking.py`: Seed parking lot data.
- `python3 scripts/seed_stadium_food.py`: Seed food vendor/menu data.
- `python3 -m src.cli.recalc_team_stats --season 2025` or `--dry-run`: Recalculate team stats from player stats.
- `python3 -m src.cli.recalc_player_stats --season 2025` or `--dry-run`: Recalculate player season stats from game-level data (fixes quality gate mismatches).
- `python3 -m scripts.maintenance.audit_pa_formula --all-years`: Audit PA formula violations (PA = AB+BB+HBP+SH+SF) across all years.
- `python3 -m scripts.maintenance.audit_pa_formula --fix-year 2020`: Apply conservative SH fix to satisfy PA formula for a season with missing SH/SF data.
- `python3 -m scripts.maintenance.backfill_sh_sf_from_pbp --year 2020`: Backfill SH/SF from PBP sacrifice descriptions.
- `python3 -m src.cli.quality_gate_check --year 2025`: Run statistical quality gate (batting/pitching/pa_formula).
- `python3 -m scripts.maintenance.backfill_player_ids`: Backfill NULL player_ids in game stats tables (uses PlayerIdResolver with name+team+season matching).
- `python3 -m scripts.maintenance.backfill_player_ids --year 2026`: Single-year backfill.
- `python3 -m scripts.maintenance.backfill_player_ids --dry-run`: Preview only (no writes).
- `python3 -m scripts.maintenance.resolve_null_player_ids_conservative`: Conservative resolver using group evidence + overrides CSV.
- `python3 -m scripts.maintenance.resolve_null_player_ids_conservative --year 2026 --apply`: Apply conservative resolution for 2026.
- `python3 -m src.cli.monthly_pa_audit`: Run PA formula audit & fix for previous year.
- `python3 -m src.cli.monthly_unified_audit`: Run unified audit (PA formula + team stats) for previous year.
- `python3 -m src.cli.monthly_unified_audit --year 2025 --dry-run`: Preview unified audit (no changes).
- `python3 -m src.cli.monthly_unified_audit --year 2025 --team-only`: Run only team stats audit.
- `python3 -m src.cli.monthly_unified_audit --year 2025 --pa-only`: Run only PA formula audit.
- `python3 -m src.crawlers.fan_culture_crawler --save`: Crawl KBO cheer songs from YouTube channels.
- `python3 scripts/seed_fan_culture.py`: Seed team rivalry data.
- `python3 -m src.cli.backfill_advanced_stats --year YYYY`: Backfill advanced batting/pitching stats.
- `python3 -m src.cli.daily_preview_batch --date YYYYMMDD`: Run pregame batch for a target date.
- `python3 -m src.cli.freshness_gate [--days N]`: Check data freshness against expected thresholds.
- `python3 -m src.cli.gap_report [--category ...]`: Run gap analysis for missing/aged data.
- `python3 -m src.cli.refresh_source_snapshots --all --max-hours 24`: Refresh DataSource raw snapshots and last-success timestamps.
- `python3 -m src.cli.generate_quality_report --year YYYY`: Generate data quality statistics report.
- `python3 -m src.cli.quality_dashboard --limit 14 --json`: Summarize generated quality reports into a compact dashboard payload.
- `python3 -m src.cli.crawl_kbo_official_events --save`: Refresh KBO official event/promotion source snapshots.
- `python3 -m src.cli.crawler_selector_gate --config Docs/references/crawler_selector_gate.json --json`: Validate crawler selector contracts against fixture/live targets.
- `python3 -m src.cli.diagnose_crawler_failure --json logs/<logfile>.log`: Classify crawler failure logs and suggest targeted recovery commands.
- `python3 -m src.cli.data_quality_regression_pack --json`: Run compact DB invariants for PA formula, impossible stats, and NULL player IDs.
- `python3 -m src.cli.hydrate_runtime_from_oci [--year YYYY] [--date YYYYMMDD]`: Hydrate local runtime cache from OCI.
- `python3 -m src.cli.live_crawler [--mode ...]`: Run live data crawler during game hours.
- `python3 -m src.cli.recalc_player_game_stats --year YYYY`: Recalculate player game-level batting/pitching stats.
- `python3 -m src.cli.recalc_season_stats --year YYYY`: Recalculate season-level player/team stats.
- `python3 -m src.cli.run_daily_update`: Execute the full daily update pipeline (finalize + standings + defense + rankings).
- `python3 -m src.cli.run_periodic_extras`: Run periodic data sync tasks.
- `python3 -m src.cli.run_weekly_maintenance`: Run weekly maintenance tasks (futures profiles, enrichment).
- `python3 -m src.cli.sync_oci --season-stats`: Sync season-level player/team stats to OCI.
- `python3 -m src.cli.sync_oci --player-game-stats`: Sync player game-level stats to OCI.
- `python3 -m src.cli.smart_polling_gate --json`: Lightweight gate to check if today's KBO games are finished (used in CI polling).
- `python3 -m src.cli.data_integrity_checker --date YYYYMMDD`: Post-crawl data integrity validation (game existence, terminal status, stats, NULL player IDs).
- `python3 -m src.cli.load_text_relay --input-dir data/`: Load text relay CSV files into the database.
- `python3 scripts.verification.verify_player_game_stats --year YYYY`: Verify player game stat consistency.
- `pytest`: Run the test suite.

### Additional CLI Inventory

These modules are operational or diagnostic entrypoints that are less frequently used than the primary commands above. Prefer `python3 -m src.cli.<module> --help` before running them, and use dry-run/read-only flags when available.

| Category | CLI modules |
| --- | --- |
| Data collection | `collect_profiles`, `collect_rosters`, `crawl_congestion`, `crawl_operation_notices`, `crawl_parking`, `crawl_retire`, `crawl_seat_sections`, `crawl_stadium_food`, `crawl_staff_register`, `crawl_text_relay`, `crawl_transit_time` |
| Pipeline / jobs | `run_all_crawlers`, `run_advanced_daily`, `daily_highlight_batch`, `daily_review_batch`, `daily_story_batch`, `crawl_phase1_extra`, `run_pipeline_demo` |
| Repair / backfill | `auto_healer`, `backfill_advanced_stats`, `backfill_pregame_previews`, `backfill_starting_pitchers_from_stats`, `fix_player_names`, `rebuild_relay_events`, `reconcile_postgame`, `regenerate_game_stories`, `regenerate_review_summaries`, `repair_game_stats`, `retry_daily_failures` |
| Calculations | `calculate_matchups`, `calculate_rankings`, `calculate_sabermetrics`, `calculate_standings`, `monthly_team_audit` |
| Monitoring / reports | `check_data_status`, `crawler_live_smoke`, `crawler_selector_gate`, `dashboard_report`, `data_quality_report`, `db_healthcheck`, `health_check`, `monitor_data_freshness`, `morning_pbp_report`, `quality_dashboard`, `smart_polling_gate`, `data_integrity_checker` |
| Analysis / sync utilities | `analyze_data`, `diagnose_coach_pitching`, `discover_historical_players`, `fetch_kbo_pbp`, `ingest_mock_game_html`, `ingest_schedule_html`, `seed_relay_validation_metrics`, `sync_pregame_previews`, `verify_chunk_quality`, `verify_sync_consistency`, `load_text_relay` |

## Code Quality & Linting
- `ruff check src/ tests/ scripts/` = **0 errors** (enforced by pre-commit).
- `ruff format --check` must pass.
- `pyproject.toml` excludes `scripts/investigations/` from ruff scope (debug/investigation files).
- `G004` (logging-f-string) is globally ignored — f-strings in logging are intentional after print→logger conversion.
- All `src/` modules have return-type annotations; use `X | None` (not `Optional[X]`) and `list[X]` (not `List[X]`).
- `from __future__ import annotations` required before modern type syntax in Python 3.12.

## Coding Style & Naming Conventions
- Python, 4-space indentation, type hints encouraged but not required.
- Module names are `snake_case.py`; classes are `CamelCase`.
- Crawler classes live in `src/crawlers/`, parsers in `src/parsers/`, data access in `src/repositories/`.
- Prefer structured payloads (dicts with explicit keys) between crawlers and repositories.

## Testing Guidelines
- Test framework: `pytest`.
- Test files follow `test_*.py` naming in `tests/`.
- Debug utilities (e.g., `debug_*.py`) are not part of the default test run.
- Example: `pytest tests/test_player_profile_parser.py`.

## Commit & Pull Request Guidelines
- Commit history uses short, imperative messages (e.g., “Add ...”, “Implement ...”).
- PRs should include: a clear summary, affected modules, test commands run, and any selector/URL changes.

## Configuration & Secrets
- Use `.env` for `DATABASE_URL`, `OCI_DB_URL`, request throttling (e.g., `KBO_REQUEST_DELAY_MIN`), and external API keys (`YOUTUBE_API_KEY`, `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET`).
- Crawler stability depends on consistent delays; avoid reducing throttling without review.

## Concurrency & Scheduling
- Automated tasks (`scripts/scheduler.py`) use a **3-stage locking mechanism** to prevent concurrent execution conflicts:
  - **`LIVE_LOCK`**: High-frequency real-time jobs (live refresh, pregame refresh).
  - **`DAILY_LOCK`**: Core daily data pipeline (daily game crawl, postgame finalize).
  - **`MAINTENANCE_LOCK`**: Long-running maintenance jobs (futures profile crawl, OCI sync, season stat recalc, report generation).
- Always use the appropriate lock when adding new scheduled jobs or long-running maintenance tasks.
- All data save logic uses **UPSERT** for idempotency; failed jobs can be safely re-run.

## GitHub Actions Automation

The CI/CD pipeline uses 11 workflows and 3 composite actions under `.github/`:

### Composite Actions
- `.github/actions/python-env/`: Shared setup — checkout, setup-python (3.12), pip install, Playwright (cached via actions/cache, ~5s on hit), init-db + seed (optional), OCI hydration (optional). Used via `uses: ./.github/actions/python-env` with `playwright`, `init-db`, `hydrate` boolean inputs.
- `.github/actions/kbo-job-setup/`: Reusable checkout + python-env + optional date resolution. Wraps python-env with `playwright`, `init-db`, `hydrate`, `hydrate-year`, `hydrate-date`, `resolve-date`, `target-date` inputs. Outputs `KST_DATE`, `KST_YEAR`. Used to eliminate boilerplate in multi-job workflows.
- `.github/actions/notify/`: Status notification to Telegram and/or Slack. Inputs: `status` (success/failure/cancelled), `workflow` (name override), `channels` (telegram/slack/both).

### Consolidated Daily Pipeline (`daily_kbo_sync.yml`)
- **Schedule**: 18:00 UTC (03:00 KST next day), `workflow_dispatch` available
- **Concurrency**: `cancel-in-progress: true` — manual dispatch cancels pending scheduled runs (safe: all writes use UPSERT)
- **Jobs** (4 sequential):
  1. `finalize` — run_daily_update + standings + defense + rankings + freshness gate
  2. `post-process` — PBP healer + batch parse snapshots
  3. `quality` — quality report + trend tracker + gap report (Tier 3) + data freshness monitor + recalc player-game stats
  4. `advanced-sync` — advanced daily sync + reference integrity gate + quality gate + completeness audit + freshness gate (extended)
- **Environments**: `OCI_DB_URL`, `KBO_USER_ID`, `KBO_USER_PWD`, `TELEGRAM_BOT_TOKEN`, per-category `TELEGRAM_CHAT_ID_*` for gap report routing
- **Note**: 271→251 lines after extracting `kbo-job-setup` composite action

### Backfill Workflows (Consolidated, Tier 2 on GH Actions)
| Matrix ID | Cron (KST) | Purpose |
|-----------|-----------|---------|
| `missed_crawls` | Sun 04:30 | Multi-phase auto-backfill (detail+PBP+preview+profiles) |
| `player_game_stats` | Sun 04:00 | Recalc player game-level batting/pitching stats |
| `sh_sf` | Sun 05:45 | Derive SH/SF from PBP events |
| `advanced_stats` | Sun 06:00 | Recalc advanced batting/pitching season stats |
| `player_ids` | Thu 05:30 | Resolve NULL player_ids in game stats |
| `roster` | Month 2nd 04:00 | Roster movements + daily rosters |

All six backfill types are defined in a single `backfill.yml` using a job matrix. Trigger manually with `--backfill_id <id>` or `all`.

### Other Workflows
- `daily_preview.yml`: Pregame batch and live crawl (day-of-game cron windows)
- `pitcher_backfill.yml`: Live pitcher stat backfill during game hours
- `weekly_maintenance.yml`: Sunday 05:00 KST — futures profiles, player enrichment
- `periodic_extras.yml`: Monthly 1st — periodic data sync
- `full_recalculation.yml`: Manual dispatch — season stat recalculation + OCI sync
- `kbo_automation.yml`: Manual dispatch — 8 phases: pregame, live, finalize, freshness, quality-report, gap-report, backfill, recalc-stats
- `test_suite.yml`: CI on push/PR — ruff lint + pytest matrix (3.12)
- `docker_build.yml`: Docker image build and push
- `security_audit.yml`: Vulnerability scanning

### Required Secrets
- `OCI_DB_URL`, `KBO_USER_ID`, `KBO_USER_PWD`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
- Per-category gap alert: `TELEGRAM_CHAT_ID_RELAY`, `TELEGRAM_CHAT_ID_STANDINGS`, `TELEGRAM_CHAT_ID_PROFILE`, `TELEGRAM_CHAT_ID_FRESHNESS`
- External APIs: `YOUTUBE_API_KEY`, `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET`
- Optionally: `SLACK_WEBHOOK_URL` (for notify action with `channels: slack`)

## Anchored Summary

Last updated: 2026-06-27

### Current Sprint (2026-06-23)

### Phase A-C: FBT select + 테스트 최적화 + SIM 정리
- FBT001/FBT002를 pyproject.toml select에 추가, `quality_dashboard.py:_as_bool` keyword-only 전환
- 테스트 최적화: basic2 crawler 101s→0.28s (360x), OCI sync retry 4s×3→0.05s (240x)
- SIM 21건 정리: src/ SIM102/105/108/113/117 = 0 violations
- 전체 pytest: 93s→75s (19% 단축)

### Phase 1: TCH typing import 정리
- TC001/TC002/TC003 select 추가, 256건 `--unsafe-fixes` auto-fix
- 15개 model 파일의 `date/datetime`이 TYPE_CHECKING으로 잘못 이동된 것 복구 (SQLAlchemy Mapped runtime 필요)
- model/tests/scripts per-file-ignore 설정

### Phase 2: C4 + PT
- C4 comprehension 최적화 11건 auto-fix
- PT (pytest style) select 추가 (tests/scripts per-file-ignore)

### Phase 3: 테스트 최적화
- team_stats_fallback 35s→0.49s (70x, `get_team_mapping_for_year` monkeypatch)

### Phase 4: CI timeout 단축
- lint: 10→5min, test: 30→10min

### Phase 5: 마무리
- pre-commit install 완료
- PLR2004 select+ignore 완전 제거 (중복 설정 정리)
- coverage fail_under=50 (이미 pyproject.toml에 설정됨)
- 최종 pytest: 4327 passed in 45.59s (기존 93s 대비 51% 단축)

### Ruff Rules Expansion Status

Ruff expansion phases completed across the current cleanup campaign. The work enabled stricter lint coverage while preserving existing CLI contracts, crawler interfaces, and keyword-call compatibility where tests or public helpers depended on parameter names.

| Area | Rules / Scope | Result |
| --- | --- | --- |
| Return flow | `RET` | Fixed 59 violations (`RET503`, `RET504`) |
| Try/raise hygiene | `TRY004`, `TRY201`, `TRY301` | Fixed 10 violations |
| Raise message hygiene | `TRY003` | Fixed 64 `src/` violations; tests/scripts keep fixture-oriented ignores |
| Logging in exceptions | `TRY400`, `TRY401` | Fixed 100 violations |
| Try/else structure | `TRY300` | Fixed 59 `src/` violations across 24 files |
| Path handling | `PTH` | Fixed 167 violations; migrated `os.path` usage to `pathlib` |
| Annotations | `ANN002`, `ANN003`, `ANN202`, `ANN205` | Fixed 18 violations; added future annotations to 18 `__init__.py` files |
| Function argument annotations | `ANN001` | Fixed 288 `src/` violations; annotated common DB/session and crawler helper parameters |
| Dynamic typing | `ANN401` | Fixed 171 `src/` violations; replaced direct `Any` annotations with concrete types or `object` where appropriate |
| Blind exceptions | `BLE001` | Removed all `src/` violations; narrowed scripts where safe |
| Exception chaining | `B904` | Enabled exception chaining checks; fixed remaining script violation with `raise ... from ...` |
| Unused arguments | `ARG` | Fixed 42 `src/` violations; kept keyword compatibility with targeted `noqa` where required |
| Print statements | `T20` | Fixed 10 `src/` violations with targeted `T201` allowances for CLI stdout / parser smoke-test output |
| CLI tests/docs | Core CLI smoke tests and inventory | Added `run_daily_update` CLI tests and documented additional CLI inventory |
| Parser/repository tests | Edge-case coverage | Added ticket duplicate-price and source snapshot failure-message tests |
| Pre-commit | Hook set | Upgraded pre-commit hooks and added AST, merge-conflict, private-key, and debug-statement checks |
| Container/deps | Dockerfile / packaging | Added `.dockerignore`, non-root Docker user, DB healthcheck, and aligned dependency lower bounds |
| Magic value constants | `PLR2004` | Fixed 113 `src/` violations; created `src/constants.py` with KBO domain constants (`MAX_INNINGS`, `KBO_FOUNDING_YEAR`, `DATE_STR_LEN`, etc.); replaced HTTP 200 → `HTTPStatus.OK` across 17 files; 165 low-value violations suppressed via `pyproject.toml ignore` |
| Lazy import hygiene | `PLC0415` | Moved 25 stdlib lazy imports (`json`, `os`, `re`, `argparse`, `datetime`) to top-level across 18 files; circular-dependent `src.` imports retained with intentional lazy pattern |
| Redundant exception / simplification | `RSE`, `PIE`, `RUF017/022`, `YTT` | Fixed 6 violations (PIE790 3x, PIE810 2x, RUF022 1x, RSE102 1x); 0 violations after `--fix` |
| Simplify / naming / access | `SIM105/117`, `N`, `SLF001`, `FLY002` | Enabled SIM105, SIM117 (src/ 0 violations); N pep8-naming (src/ 0); SLF001 private-member-access (14 noqa'd); FLY002 (tests/ ignore) |

### Current Verification Baseline

- `ruff check src/ tests/ scripts/` = 0 errors (default select, 100 pre-existing violations in tests/scripts: S608 29x, PERF401 27x, T201 10x, G003 10x, N806 9x, ARG 5x, others 10x — to be cleaned in next session)
- `ruff format --check .` = 898 files already formatted
- `python3 scripts/lint_bare_except.py` = 0 bare `except Exception` in 425 files
- `python -m pytest --tb=short -q --cov=src --cov-report=term --cov-report=term-missing:skip-covered` = 3794 passed (3 pre-existing failures: team_stat_aggregator x2, team_stats_fallback x1 — all DB table missing); 1 xfailed; 22s; coverage baseline 67%
- `ruff check --select N src/` = 0 violations (pep8-naming, enabled 2026-06-24)
- `ruff check --select SLF001 src/` = 0 violations (enabled 2026-06-24, 14 intentional internal-access noqa'd)
- `# noqa: BLE001` in `src/` = 0
- `# noqa: BLE001` in `scripts/` = 6 intentional CLI / operational catch-all guards

### Smart Polling System (2026-06-25)

- **2-layer architecture**: Layer 1 (`smart_polling_gate.py`) uses lightweight HTTPX to query Naver Sports API and determine if today's games are finished. Layer 2 (`run_daily_update`) runs heavy Playwright crawling only when gate passes.
- **Cron schedules**: Tue-Fri 21:30-23:30 KST, Sat 20:00-22:30 KST, Sun 17:00-21:00 KST (every 30 min during expected game end times). Monday excluded (no games).
- **Workflow**: `kbo_smart_polling.yml` — 4 jobs: polling_gate → daily_update → integrity_check → notify.
- **Error handling**: Gate exit 0 = proceed, exit 1 = skip (games in progress), exit 2+ = failure (triggers alert). Yesterday fallback handles games running past midnight.
- **Post-crawl validation**: `data_integrity_checker.py` verifies game existence, terminal status, scores, child stats (batting/pitching), and NULL player IDs.

### Source Snapshot / Event Status

- `kbo_official_events` now crawls official KBO `BusinessAndEvent` pages instead of the main page; dry-run returns 11 candidates and `--save` wrote 11 `team_events` plus 7 raw snapshots.
- `refresh_source_snapshots --source-key doosan_bears_events` and `--source-key doosan_bears_ticket` save successfully through Playwright fallback after httpx TLS verification failure.
- Local `data_sources` registry has 38 active sources and 0 `last_success_at IS NULL` entries after the source refresh campaign.
- Weekly maintenance runs `DATABASE_URL="$OCI_DB_URL" python3 -m src.cli.refresh_source_snapshots --all --max-hours 168` as a non-blocking step before the main weekly sync.
- `quality_gate_check --year 2020..2025` succeeds after splitting aggregate placeholders (`TOTAL`, `합계`, etc.) from all-star raw team codes (`EA`/`WE`) when canonical team codes are present.
- `PlayerMovementCrawler.crawl_years(..., save_snapshots=True)` now records `kbo_player_movement` raw snapshots during daily roster/player movement updates.

### C901 (Complexity) Progress

| Phase | Functions | C901 Δ | Status |
|-------|-----------|--------|--------|
| 1 | 12 files (crawlers/parsers) | 103→91 | ✅ Complete |
| 2 | 14 CLI files | 91→76 | ✅ Complete |
| 3 | 10 files | 76→64 | ✅ Complete |
| 4 | 8 files | 64→52 | ✅ Complete |
| 5 | 4 funcs | 52→48 | ✅ Complete |
| 6 | 4 funcs | 48→44 | ✅ Complete |
| 7a/7b (redo) | 8 funcs (foreign_player, manager_change, baserunning_stats, fa, relay, futures_batting, futures_pitching, player_search main) | 41→31 | ✅ Complete |
| 7c | PL fixable auto-fix (11 violations) | — | ✅ Complete |
| 8a | 4 live CLI funcs (crawl_futures 2, live_crawler 2) | 41→31 | ✅ Complete |
| 8b | 2 funcs (sync_games.sync_game_details) | 31→31 | ✅ Complete |
| 9A | Playwright C901 light refactoring (baserunning + profile) | 27→25 | ✅ Complete |
| 9B | PLR2004 magic-value-comparison | — | ✅ Complete |
| 9C | PLC0415 lazy import hygiene | — | ✅ Complete |
| 10 | Deep PW-aware refactors (game_detail 5, player_search 1, basic2 2, pbp 2, preview 1, fa 1) | 25→0 | ✅ Complete |

### Ruff Check Status

- `ruff check src/ tests/ scripts/` = 0 errors (default select, C901 not enforced).
- `ruff check --select C901 src/` = 0 violations (103→0, 100% eliminated).
- `ruff check --select PLR0915 src/` = 0 violations (too-many-statements, threshold 50).
- `ruff check --select FURB167,RUF013,DTZ005,S608,TC001,TC002,TC003 src/` = 0 violations; these rules are now enabled for `src/`.
- `ruff check --select RSE,PIE,RUF017,RUF022,YTT,FLY002 src/` = 0 violations; enabled 2026-06-24.
- `ruff check --select PLR0913 src/` = 29 intentional remaining (public/crawler entrypoints with keyword compatibility).

### Phase 10 Completed (2026-06-23) — Deep Playwright-aware C901 refactoring

- **game_detail_crawler.py** 5→0: Extracted metadata/timeout/hitter/pitcher/payload-building helpers from `_crawl_game_detail_and_stats`.
- **player_search_crawler.py** 1→0: Split `_paginate_current_tab` into `_add_current_page_rows`, `_visit_remaining_numeric_pages`, `_visit_next_pager_block`, `_click_pager_target`.
- **simple_basic2_crawler.py** 2→0: Extracted `_prepare_basic2_bb_page`, `_parse_bb_row`, `_bb_stat_payload`, `_bb_extra_stats` from `crawl_basic2_with_headers` and `_parse_basic2_header_data_legacy`.
- **pbp_crawler.py** 2→0: Extracted `_prepare_live_text_page`, `_wait_for_pbp_container`, `_initial_legacy_state`, `_build_legacy_event` from `crawl_live_game_pbp` and `_build_fallback_pbp_data`.
- **preview_crawler.py** 1→0: Extracted `_fetch_preview_game_list`, `_build_preview_payload`, `_enrich_preview_lineups` from `crawl_preview_list`.
- **fa_crawler.py** 1→0: Extracted `_fa_header_mapping`, `_parse_fa_row`, `_parse_fa_rows` from `crawl_fa_data`.
- Key insight: `page.evaluate()`/`page.locator()` calls remain in the original method, while data-assembly, ID-resolution, and payload-building logic is extracted into helper functions.
- All refactors verified: ruff 0 errors, pytest 4323 passed, freshness gate PASS.

### Quick Fixes (2026-06-23)

- **`generate_quality_report.py`**: Removed spurious `months=6` kwarg from `get_team_stats_trend()` call (TeamSeason is season-level aggregate, function doesn't accept it). CLI no longer crashes. Added regression test.
- **Freshness gate `missing_review_wpa`**: Diagnosed 13 missing games on 2026-06-16~18. Applied `regenerate_review_summaries --backup-out --apply` for 3 dates → freshness gate now passes.
- **Foreign player parser fix**: Added `외국인 (투수/타자/선수) ... 계약/영입` name extraction pattern to `_extract_foreign_player_name`. Fixes existing test regression.
- **Quality gate aggregate filter**: Split `INVALID_TEAM_CODES` into `AGGREGATE_TEAM_CODES` (TOTAL/합계/-/empty) and raw all-star codes (EA/WE). All 2020-2025 seasons pass.
- **Player ID overrides**: Added 2 SSG Haechi entries to `data/player_id_overrides.csv`.

### Phase 11 Complete (2026-06-24) — New lint rules, N naming, test optimization

- **ANN201/204/206, RSE, PIE, FLY002**: 6 rules enabled with 0 src/ violations.
- **N naming (pep8-naming)**: 23 violations fixed (22×N806 + 1×N811), select enabled for src/, per-file-ignored for tests/scripts/.
- **Test optimization**: 3 slow tests monkeypatched — fan_culture 5s→~0.1s, OCI pregame 3s→~0.1s, player_status_confirmer 3s→1.5s. Total pytest: 46.51s→34.06s (26.7% 단축).

### Current Verification Baseline (2026-06-28)

- `ruff check src/ tests/ scripts/` = 0 errors (188 rules enabled, 0 warnings).
- `ruff format --check .` = clean.
- `python3 -m pytest` = **5838 passed**, 0 failures, 2 skipped, 1 xfailed; ~29s.
- `ruff check --select C901 src/` = 0 violations (100% eliminated).
- `--cov=src --cov-report=term` = **72%** (fail_under=65).
- `# noqa: BLE001` in `src/` = 0.
- `pre-commit` hooks installed locally.

### Phase 30-33 Complete (2026-06-27) — Ruff rule expansion, security, FURB/PLC/PLR/RUF

| Phase | Work | Result |
|-------|------|--------|
| 30 | Enable security + EM rules (S101, S102, S104, S110, S112, S113, EM102, EM103) | ✅ 8 rules; S101 per-file-ignored tests/scripts |
| 31 | Enable zero-violation FURB rules | ✅ 26 rules (FURB103,105,113,116,118,122,129,131,136,140,142,145,148,152,154,156,163,164,166,168,169,180,181,187,189,192) |
| 32 | Enable zero-violation PLC/PLR rules | ✅ 8 rules (PLC0414,PLC2401,PLC2403,PLC3002,PLR1701,PLR1704,PLR1722,PLR6201,PLR6301) |
| 33 | Enable zero-violation RUF rules | ✅ 18 rules (RUF007,008,009,016,018,020,023,026,028,030,032,033,034,037,041,043,048,049) |

### Phase 39-41 Complete (2026-06-28) — COM, D400/D415, D213 docstring style

| Phase | Work | Result |
|-------|------|--------|
| 39 | D400/D415 enable (missing trailing period + terminal punctuation) | ✅ src/ 0 violations; tests/scripts per-file-ignore |
| 40 | COM812 (missing trailing comma) | ✅ select + tests/scripts per-file-ignore |
| 41 | D212→D213 (multi-line docstring summary 2nd line) | ✅ 917 auto-fix in src/; tests/scripts per-file-ignore |

### Phase 42-43 Complete (2026-06-28) — PLC0415 ignore, S safety rules

| Phase | Work | Result |
|-------|------|--------|
| 42 | PLC0415 (import-outside-top-level) → global ignore | ✅ 202 violations deferred (intentional lazy imports for circular deps) |
| 43 | S101/S102/S104/S108/S110/S112/S113 enable | ✅ 7 rules; S108 per-file-ignore tests/scripts |

### Priority 1-3 Complete (2026-06-27) — Test fixes, RUF002/RUF003, PYI

| Priority | Work | Result |
|----------|------|--------|
| 1 | Fix pre-existing test failures | ✅ 5 failures → 0 (rename 4 duplicate modules + add GAME_STATUS_DELAYED import) |
| 2 | Enable RUF002/RUF003 | ✅ 3 en-dash fixes in src/, 2 rules added to select |
| 3 | Enable PYI category | ✅ ~40 stub rules added, per-file-ignored in tests/scripts |
| 34 | Coverage expansion → skipped | Already at 72% (gate 65%) |
| 35 | pyproject.toml cleanup → skipped | Per-file-ignores structure already grouped |

### Ruff Rules Expansion Status (2026-06-27)

Total enabled rules: **167** (E, W, F, I, UP, RET, ANN, TC, TRY, B, SIM, G, BLE, RUF, EM, PYI, PERF, PT, PTH, ARG, T20, FURB, DTZ, S, N, FBT, RSE, PIE, YTT, SLF, PLR, ISC, PGH, PLW, ASYNC, TID, ERA, C4, T10, PLC, B).

### Test Suite Status

- 5 pre-existing failures (not introduced by this session):
  - `test_manager_change_crawler_phase8.py` (3 tests — extract_team_id edge cases)
  - `test_game_status_phase8.py` (1 test — derive_stable_game_status with delayed_new_status)
  - Collection errors: `test_failure_diagnosis.py`, `test_game_data_validator.py`, `test_quality_gate.py` (duplicate __pycache__ module names)

### Phase 20-28 Complete (2026-06-26) — Ruff rule expansion, test cleanup, pre-commit

| Phase | Work | Result |
|-------|------|--------|
| 20 | Enable zero-violation rules (DTZ001, DTZ003, FLY002, PGH, SIM117, SIM118, PLC0207) | ✅ 7 rules added, PLC0207 1 fix |
| 21 | ERA001 cleanup (15 commented-out code blocks) | ✅ 7 src/ files cleaned |
| 22 | TID252 relative imports → absolute | ✅ 19 repository files converted |
| 23 | RUF001 ambiguous unicode per-file-ignore | ✅ 28 src/ files + tests/scripts |
| 24 | Coverage fail_under 50→65 | ✅ Baseline 68% |
| 25 | Fix pre-existing working tree issues | ✅ All-green 5145→5265 passed |
| 26 | Remove RUF001/RUF002/RUF003 from global ignore | ✅ RUF001 now actively enforced |
| 27 | Enable C4/T10/PLC0208/PLW0245/PLW1509/B026 | ✅ 6 rules + 42 auto-fixes |
| 28 | Install pre-commit hooks locally | ✅ All hooks pass |

### Ruff Rules Expansion Status (2026-06-26)

Total enabled rules: 90+ (including E, W, F, I, UP, RET, ANN, TC, TRY, B, SIM, G, BLE, RUF, EM, PYI, PERF, PT, PTH, ARG, T20, FURB, DTZ, S, N, FBT, RSE, PIE, YTT, SLF, PLR, ISC, PGH, PLW, ASYNC, TID, ERA, C4, T10, PLC, PLW, B).

### Recent Work (2026-06-26) — CI/CD stabilization, data quality, PLR0913 batch

- **S501 보안**: `ticket_crawler.py`의 3곳 `verify=False` 제거 (MITM 취약점 해결).
- **S101**: `game_detail_crawler.py`, `team_info_crawler.py`의 4곳 `assert` → `raise ValueError`.
- **CI/CD 안정화**: `daily_kbo_sync.yml`의 `cancel-in-progress: true` → `false` (--fix 중단 방지).
- **스케줄러 이중화**: `compute_standings`, `compute_rankings`, `aggregate_team_defense`, `heal_unverified_pbp` 로컬 등록 (GH Actions 백업).
- **PLR0913 추가 리팩토링**: `RunStats`, `DedupConfig`, `RebuildOptions`, `RegenerationConfig`, `WpaState`, `TeamAggregationQuery` dataclass 생성. 7개 함수 리팩토링.
- **SH/SF 파생 개선**: `outs_before < 2` SF 필터 추가, `player_game_batting` 테이블 갱신, 이름 충돌 로직.
- **Docker**: `Dockerfile.playwright` 생성, `docker-compose.text-relay.yml` 생성, `text_relay_docker.yml` GH Actions 워크플로우 생성.
- **테스트 추가**: +70개 신규 테스트 (4,842 → 4,849).
- **GitHub Actions**: `test_suite.yml`에 coverage 아티팩트 업로드 및 60% 게이트 추가.

### Phase 17 Complete (2026-06-25) — EM102/EM103/DTZ timezone hygiene

- **PLR0913 문서 정리**: `Docs/references/PLR0913_REFACTOR_PLAN.md` → COMPLETE 상태로 업데이트 (PLR0913 enabled + 0 violations).
- **EM102 enable**: `data_integrity_checker.py`, `relay_crawler.py` 2건 f-string-in-exception 수정.
- **EM103 enable**: 0 violations 확인 후 select 추가.
- **DTZ007/DTZ011/DTZ001 enable**: 76 violations 수정 (50 files).
  - `datetime.strptime(X, Y)` → `datetime.strptime(X, Y).replace(tzinfo=KST)` (64건)
  - `date.today()` → `datetime.now(KST).date()` (8건)
  - `datetime(Y, M, D)` → `datetime(Y, M, D, tzinfo=KST)` (4건)
  - `team_event_parser.py:_parse_fetched_at`에서 naive → aware datetime으로 통일.
- **isort 설정**: `known-first-party = ["src"]` 추가.
- **Import 정리**: 50개 파일에 KST import 추가 (기존 `from src.constants import`에 merge).
- **테스트 업데이트**: 25개 테스트의 datetime aware assertion 수정, mock chain 업데이트.
- **per-file-ignore**: tests/scripts에 DTZ007/DTZ011/DTZ001 추가.
- **pytest**: 4,811 passed (10 pre-existing failures: standings 7 + stadium 1 + type_helpers 1 + season_aggregator 1).
- **ruff**: EM102, EM103, DTZ001, DTZ007, DTZ011 src/ select 추가 (tests/scripts는 per-file-ignore).

### Data Directory Cleanup (2026-06-25)

- Removed 15 stale 0-byte sync logs (`sync_oci_2010_v4.log` through `sync_oci_2024_v4.log`).
- Removed corrupted DB backup (`kbo_dev_corrupted.db.bak` + WAL/SHM).
- Archived 32 old DB backups (kept newest 2), 100 null_player_id CSVs, 279 quality_gate CSVs, review summaries.
- Reduced `data/` from 76GB to 36GB.
- Added `data/archive/` to `.gitignore`.

### Phase 36 Complete (2026-06-25) — D rules docstring campaign

- **18 zero-violation D rules enabled**: D106, D209, D210, D300, D301, D402, D404, D405, D406, D407, D408, D409, D410, D411, D412, D413, D414, D416, D418, D419.
- **D200 auto-fix**: 128 violations → 0 (ruff --fix --unsafe-folds, one-line docstrings collapsed).
- **D413 auto-fix**: 38 violations → 0 (blank line after last section).
- **D205 skipped**: fix unavailable for Korean docstrings (ruff "sometimes available" limitation).
- **D101/D102/D103/D105/D107/D401 deferred**: require manual docstring content creation or don't work with Korean text.
- **22 preview-only rules removed from select**: FURB103/105/113/116/118/122/129/131/136/140/142/145/148/152/154/156/163/164/166/168/169/180/181/187/189/192, PLC0414/2401/2403/3002, PLR1701/1722/6201/6301 — eliminated 22 warning lines per ruff invocation.
- **pytest**: 5416 passed, 0 failed.
- **ruff**: 0 errors, 0 warnings.

### Notes For Future Agents

- Preserve CLI stdout `print()` calls that intentionally emit JSON or rendered command output; use targeted `# noqa: T201` rather than logging.
- Do not rename unused public/helper parameters when tests or callers pass them by keyword; prefer targeted `# noqa: ARG001` or `# noqa: ARG002`.
- `tests/**` and `scripts/**` intentionally ignore selected annotation, `TRY003`, `ARG`, and `T20` rules because fixture signatures, monkeypatch callbacks, debug scripts, and CLI utilities commonly require broad or unused arguments, inline fixture exceptions, and stdout output.
- `from src.db.engine import SessionLocal` and model imports in relay_crawler._load_game_metadata_from_db, player_search_crawler helpers are intentionally lazily imported to avoid circular deps — do not `PLC0415`-fix them.
- C901 refactoring of Playwright-dependent functions is feasible: keep `page.evaluate()`/`page.locator()` in the original method, extract only data-assembly/ID-resolution/payload-building logic into helpers.

### CI & 테스트 안정화 (2026-06-25)

- **`player_pitching_all_series_crawler.py` AttributeError 수정**: `_collect_pitcher_basic2_additional`가 `Basic2AdditionalContext`를 `parse_basic2_page`에 직접 전달하여 `AttributeError: has no attribute 'season'` 발생. 루프마다 `Basic2PageContext` 래퍼를 생성(`year→season`, `series_info["league"]→league`, `display_name→sort_key`, `limit→max_players`)하여 전달하도록 수정. Daily Pipeline CI `finalize` 잡 복구.
- **`full_audit.py` OCI 병목 해소**: `table_columns()`에 `_COLUMNS_CACHE` 전역 딕셔너리 캐시를 추가하여 `information_schema.columns` 반복 조회를 1회로 줄임. PostgreSQL `AND table_schema = CURRENT_SCHEMA()` 필터로 공유 환경 크로스-스키마 오탐 방지.
- **`test_scheduler.py` fixture 격리 복구**: `_inject_mock_module` (autouse)가 `sys.modules["scripts.scheduler"]`를 stub으로 교체 후 미복구하여 이후 테스트(`test_scheduler_alerting.py`)가 `compute_standings_job NameError` 발생. `yield` 이후 원본 모듈을 복구하는 teardown 로직 추가.
- **테스트 결과**: 3 failed → **0 failed**, 4835 → **5265 passed** (전체 스위트 정상화).

### Phase 37 Complete (2026-06-28) — D102/D103/D105 docstring campaign

- **D102/D103/D105 enabled**: Added to ruff select alongside existing D100/D104/D105
- **Script**: `scripts/generate_docstrings.py` — AST-based docstring generator with:
  - Function name → verb/noun extraction for English summaries
  - Parameter descriptions from type hints and naming patterns
  - Return type descriptions
  - Magic method templates (`__repr__`, `__str__`, etc.)
  - Multi-line signature detection via regex
  - `...` body skip (Protocol abstract methods)
  - Per-insertion syntax validation (skips invalid insertions)
- **Coverage**: ~1100 public functions/methods across models, utils, parsers,
  crawlers, repositories, services, sync, aggregators, monitoring, analyzers,
  sources, validators, db
- **Protocol methods**: 6 abstract methods with `...` body excluded (3 files
  per-file-ignored: game_detail_crawler.py, game_collection_service.py,
  postgame_reconciliation_service.py)
- **D401 (imperative mood)**: Evaluated and skipped — codebase uses Google/NumPy
  3rd person style ("Calculates"), which is industry standard. D401 would require
  changing 75 existing docstrings + all generated docstrings without quality gain.
- **D417 (argument descriptions)**: 75 violations remain — requires manual review
  of existing docstrings to add Args sections. Deferred.
- **ruff**: 0 errors, 0 warnings (180 rules enabled)
- **pytest**: 5698 passed, 0 failures

### Phase 38 Complete (2026-06-28) — Zero-violation rules + COM812 + D205

- **17 new zero-violation rules enabled**: B904, COM818, N802-N803, N807, N815-N816, N818, RSE102, RUF012, SIM101, PLC0206, RUF028, RUF033-RUF034, RUF037, RUF041, RUF043, RUF048-RUF049
- **COM812 fixed**: 146 trailing comma violations → 0
- **D205 fixed**: 149 → 51 (98 auto-fixed, 51 multi-line summary edge cases moved to global ignore)
- **199 rules enabled** (180 → 199)
- **pytest**: 5763 passed, 0 failures
