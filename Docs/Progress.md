# Project Progress Overview

## Current Status
- **Schedules**: 2025 preseason, regular season, and initial postseason fixtures ingested from saved HTML; `game_schedules` holds 770 entries total.
- **Game Details**: Box score parser verified (`20251001NCLG0` fixture) with 16 batting and 16 pitching rows stored in `player_game_batting` / `player_game_pitching`.
- **Validation**: Game data validator active; failures block persistence and update crawl status with diagnostics.
- **Futures Pipeline**: CLI wired to Futures profile endpoints; awaiting Playwright-enabled environment to fetch live tables.
- **Mock Ingest**: End-to-end demo script (`run_pipeline_demo`) supports offline HTML ingestion and reporting.

## Recent Improvements
- Added offline schedule/game parsers and ingest CLIs for reproducible loads.
- Implemented structured player-season models with provenance (`source='PROFILE'` for Futures, `source='GAMECENTER'` for game logs).
- Hardened error handling in repositories (validation, crawl status updates, reporting helpers).
- Created `.gitignore` entries for venvs, caches, local data, and fixtures.

## Pending Tasks
- Capture and ingest remaining 2025 postseason schedule once matchups are confirmed.
- Run Futures crawler on a Playwright-enabled host; verify records for players such as `51868` (Futures hitting stats present online).
- Expand mock fixtures for additional games to strengthen regression coverage.
- Integrate weekly scheduler (e.g., cron or APScheduler) to trigger Futures sync per Docs/CRAWLING_LIMITATIONS guidance.

## Next Steps
1. **Futures Sync Verification**: Execute `run_pipeline_demo --run-futures` on a machine where Chromium launches successfully; confirm `player_season_batting/pitching` rows with `league='FUTURES'` appear.
2. **Postseason Data**: Once KBO releases final bracket pages, save HTML fixtures and ingest with `--season-type postseason` to complete schedule coverage.
3. **Automation**: Script nightly schedule refresh + weekly Futures job (respecting rate limits in Docs/CRAWLING_LIMITATIONS.md).
4. **Reporting**: Build lightweight checks (SQL or CLI) to compare schedule counts vs. expected totals before downstream jobs run.

---
_Last updated: 2025-10-14_
