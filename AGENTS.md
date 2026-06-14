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
- `python3 -m src.cli.crawl_game_details --date 20241015` or `python3 -m src.cli.collect_games --year 2024 --month 10`: Collect game details.
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
- `python3 -m src.cli.freshness_gate [--max-hours N]`: Check data freshness against expected thresholds.
- `python3 -m src.cli.gap_report [--category ...]`: Run gap analysis for missing/aged data.
- `python3 -m src.cli.generate_quality_report --year YYYY`: Generate data quality statistics report.
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
- `python3 -m scripts.verification.verify_player_game_stats --year YYYY`: Verify player game stat consistency.
- `pytest`: Run the test suite.

## Code Quality & Linting
- `ruff check src/ tests/` = **0 errors** (enforced by pre-commit).
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
