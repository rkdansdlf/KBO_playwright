# Repository Guidelines

## Project Structure & Module Organization
This repository is a Playwright-based KBO data crawler with a two-track pipeline:
- `src/`: Core application code (crawlers, parsers, models, repositories, CLI).
- `scripts/`: Maintenance and batch utilities (crawling, oci, maintenance).
- `tests/`: Pytest tests and debug scripts (`test_*.py` run by default).
- `Docs/`: Runbooks, URL references, limitations, and schemas.
- `migrations/`: Schema migrations (including OCI).
- `data/`, `logs/`: Local SQLite DB and runtime logs.

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
- `pytest`: Run the test suite.

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
- Use `.env` for `DATABASE_URL`, `OCI_DB_URL`, and request throttling (e.g., `KBO_REQUEST_DELAY_MIN`).
- Crawler stability depends on consistent delays; avoid reducing throttling without review.

## Concurrency & Scheduling
- Automated tasks (`scripts/scheduler.py`) use a **3-stage locking mechanism** to prevent concurrent execution conflicts:
  - **`LIVE_LOCK`**: High-frequency real-time jobs (live refresh, pregame refresh).
  - **`DAILY_LOCK`**: Core daily data pipeline (daily game crawl, postgame finalize).
  - **`MAINTENANCE_LOCK`**: Long-running maintenance jobs (futures profile crawl, OCI sync, season stat recalc, report generation).
- Always use the appropriate lock when adding new scheduled jobs or long-running maintenance tasks.
- All data save logic uses **UPSERT** for idempotency; failed jobs can be safely re-run.

## GitHub Actions Automation

The CI/CD pipeline uses 14 workflows and 3 composite actions under `.github/`:

### Composite Actions
- `.github/actions/python-env/`: Shared setup for all workflows — checkout, setup-python (3.12), pip install, Playwright (optional), init-db + seed (optional), OCI hydration (optional). Used via `uses: ./.github/actions/python-env` with `playwright`, `init-db`, `hydrate` boolean inputs.
- `.github/actions/notify/`: Status notification to Telegram and/or Slack. Inputs: `status` (success/failure/cancelled), `workflow` (name override), `channels` (telegram/slack/both).
- `.github/actions/notify-telegram/`: Legacy Telegram-only notifier (kept for backward compatibility).

### Consolidated Daily Pipeline (`daily_kbo_sync.yml`)
- **Schedule**: 18:00 UTC (03:00 KST next day), `workflow_dispatch` available
- **Jobs** (5 sequential):
  1. `finalize` — run_daily_update + standings + defense + rankings + freshness gate
  2. `post-process` — PBP healer + batch parse snapshots
  3. `quality` — quality report + trend tracker + gap report (Tier 3) + data freshness monitor
  4. `advanced-sync` — advanced daily sync + reference integrity gate + quality gate + completeness audit
- **Environments**: `OCI_DB_URL`, `KBO_USER_ID`, `KBO_USER_PWD`, `TELEGRAM_BOT_TOKEN`, per-category `TELEGRAM_CHAT_ID_*` for gap report routing

### Backfill Workflows (Tier 2 on GH Actions)
| File | Cron (KST) | Purpose |
|------|-----------|---------|
| `backfill_missed_crawls.yml` | Sun 04:00 | Multi-phase auto-backfill (detail+PBP+preview+profiles) |
| `backfill_sh_sf.yml` | Sun 05:45 | Derive SH/SF from PBP events |
| `backfill_advanced_stats.yml` | Sun 06:00 | Recalc advanced batting/pitching season stats |
| `backfill_player_ids.yml` | Wed 05:30 | Resolve NULL player_ids in game stats |
| `backfill_roster.yml` | Month 1st 04:00 | Roster movements + daily rosters |

### Other Workflows
- `daily_preview.yml` / `live_refresh.yml` / `pitcher_backfill.yml`: Real-time pregame and live data (day-of-game cron windows)
- `weekly_maintenance.yml`: Sunday 05:00 KST — futures profiles, player enrichment
- `periodic_extras.yml`: Monthly 1st — periodic data sync
- `full_recalculation.yml`: Manual dispatch — season stat recalculation + OCI sync
- `kbo_automation.yml`: Manual dispatch — 8 phases: pregame, live, finalize, freshness, quality-report, gap-report, backfill, recalc-stats
- `test_suite.yml`: CI on push/PR — ruff lint + pytest matrix (3.11, 3.12)

### Required Secrets
- `OCI_DB_URL`, `KBO_USER_ID`, `KBO_USER_PWD`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
- Per-category gap alert: `TELEGRAM_CHAT_ID_RELAY`, `TELEGRAM_CHAT_ID_STANDINGS`, `TELEGRAM_CHAT_ID_PROFILE`, `TELEGRAM_CHAT_ID_FRESHNESS`
- Optionally: `SLACK_WEBHOOK_URL` (for notify action with `channels: slack`)
