# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

KBO_playwright is a Python-based web scraping project that collects Korean Baseball Organization (KBO) data using Playwright. The project implements a **2-track data collection pipeline** that separates regular season (game-based) and Futures League (profile-based) data collection strategies.

## Core Architecture

### Two-Track Pipeline Design

1. **Track A: Regular Season (Game-Based)**
   - Collects game-by-game detailed statistics from GameCenter
   - Data sources: Schedule, BoxScore, Lineup Analysis (XHR), Play-by-Play (PBP)
   - Nightly rollup process aggregates game data into season statistics
   - High-frequency updates (daily at 03:00 KST)

2. **Track B: Futures League (Profile-Based)**
   - Collects season-cumulative statistics from player profile pages
   - Data source: Player profile "Futures" tab
   - Low-frequency updates (weekly on Sunday at 05:00 KST)
   - No game-by-game detail available

### Data Processing Flow

All data collection follows a strict 4-stage process:
1. **Collect**: Fetch HTML/JSON using Playwright
2. **Parse**: Extract data using CSS selectors or regex
3. **Validate**: Type and range checking (e.g., batting average 0-1)
4. **Save**: UPSERT to database for idempotency

## Development Commands

### Environment Setup
```bash
# Install Python dependencies
pip install -r requirements.txt

# Install Playwright browser (required after pip install)
playwright install chromium
```

### Running Crawlers
```bash
# Regular season game crawler (manual execution)
python src/cli/crawl_games.py --date 20251013

# Futures league profile crawler (manual execution)
python src/cli/crawl_futures.py

# Retired/inactive player crawler (monthly maintenance)
python src/cli/crawl_retire.py --years 1982-2025 --concurrency 3
```

### Scheduler Operations
```bash
# Run automated scheduler (APScheduler)
python scheduler.py

# Docker deployment
docker-compose up -d scheduler
```

## Key Technical Considerations

### Web Scraping Etiquette
- **Rate Limiting**: Minimum 1-2 second delay between requests (`time.sleep(1-2)`)
- **Execution Window**: Schedule for 02:00-05:00 KST (low traffic hours)
- **Exponential Backoff**: Implement for 429 errors and transient failures
- **User-Agent**: Use identifiable string (e.g., "PlaywrightKBO Crawler/1.0")
- **Respect robots.txt**: Check before crawling new paths

### Playwright Best Practices
- Use `page.wait_for_selector()` for dynamic content loading
- Set `page.wait_for_load_state('networkidle')` for AJAX-heavy pages
- Wrap operations in `try...except` blocks to prevent cascade failures
- Set `max_retries=3` for transient network errors

### Data Identification
- Regular season data: `league='KBO'`, `source='game'`
- Futures league data: `league='FUTURES'`, `source='profile'`
- All saves use UPSERT for idempotency (safe to re-run)

### Key Selectors (Fragile - Verify on Site Changes)

**Regular Season:**
- Game links: `a[href*="gameId="]`
- Away hitters: `.tblAwayHitter1`, `.tblAwayHitter2`
- Home hitters: `.tblHomeHitter1`, `.tblHomeHitter2`
- Pitcher tables: `div.away-pitcher-record table`, `div.home-pitcher-record table`
- PBP data: `div.relay-bx`, `.txt-box`

**Futures League:**
- Futures tab: `//a[contains(text(), '퓨처스')]` (XPath)
- Stats table: `div#cphContents_cphContents_cphContents_udpPlayerFutures > table.tbl.tt`
- Year rows: `tbody > tr`

**Important**: KBO website can change selectors without notice. See [Docs/URL_REFERENCE.md](Docs/URL_REFERENCE.md) for canonical reference.

### Critical URLs

- Schedule: `https://www.koreabaseball.com/Schedule/Schedule.aspx`
- GameCenter: `https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={id}&section={REVIEW|RELAY}`
- Lineup API: `https://www.koreabaseball.com/ws/Schedule.asmx/GetLineUpAnalysis` (POST)
- Player Profile: `https://www.koreabaseball.com/Record/Player/HitterDetail/Basic.aspx?playerId={id}`

## Data Limitations & Fallback Rules

### Game Data Issues
- **Post-game delay**: Wait minimum 1 hour after game end before scraping (PBP/analysis may not be ready)
- **Suspended games**: Final stats may be split across multiple entries, not aggregated
- **Exhibition games**: Lineup analysis API may not have data

### Futures League Issues
- **Missing tables**: New players may have no Futures tab - treat as no record (skip gracefully)
- **Non-standard values**: Handle `-.---` or similar strings by converting to `0` or `NULL`
- **Update lag**: Profile data not real-time, may lag by days (treat as snapshot)

### Universal Limitations
- **Sabermetrics**: KBO site doesn't provide WAR, wOBA, FIP, etc. - must calculate from raw stats
- **Historical data**: Pre-2000s data may be incomplete or inconsistent

## Scheduler Configuration

### Automated Jobs (APScheduler)

1. **Regular Season Rollup** (`crawl_games_regular`)
   - Frequency: Daily at 03:00 KST
   - Purpose: Collect previous day's games + aggregate season stats

2. **Futures Sync** (`crawl_futures_profile`)
   - Frequency: Weekly (Sunday at 05:00 KST)
   - Purpose: Sync all player Futures league cumulative stats

### Failure Handling
- **Retry Policy**: 3 attempts with exponential backoff (1s → 2s → 4s)
- **Failure Queue**: Failed jobs logged to queue (Redis/DB) for manual review
- **Locking**: Use file-based `.lock` or DB transaction locks during aggregation to prevent race conditions

## Important Documentation

Critical documentation files in [Docs/](Docs/):
- [projectOverviewGuid.md](Docs/projectOverviewGuid.md) - Detailed operational runbook with daily/weekly/monthly procedures
- [URL_REFERENCE.md](Docs/URL_REFERENCE.md) - Canonical URL patterns and selectors (single source of truth)
- [SCHEDULER_README.md](Docs/SCHEDULER_README.md) - APScheduler strategy, job separation, concurrency, and locking
- [CRAWLING_LIMITATIONS.md](Docs/CRAWLING_LIMITATIONS.md) - Known data quality issues and workarounds
- [schema/KBOseasonGamePipeLine.md](Docs/schema/KBOseasonGamePipeLine.md) - **CRITICAL: Complete production-ready architecture**:
  - Full MySQL database schema (franchises, team_identities, ballparks, game_schedules, series, games, etc.)
  - 3-stage incremental migration SQL files (0010, 0011, 0012)
  - Complete Airflow/Prefect DAG implementation with TaskFlow API
  - SQLAlchemy 2.x ORM + Core hybrid pattern
  - SQLite ↔ MySQL engine switching logic with dialect-specific UPSERT
  - Alembic migration setup with batch mode for SQLite
  - Orchestration layer design (Preseason/Regular/Postseason pipelines)
  - Event-driven workflow with crawl_status state machine
  - Data validation layer and pipeline optimization strategies

## Troubleshooting Common Issues

### 429 / Rate Limit Errors
- Increase delay between requests to 2-3 seconds
- Expand exponential backoff window
- Shift execution to lower-traffic hours

### Missing Data After Game
- Verify collection ran at least 1 hour post-game
- Re-run collection job (idempotent UPSERT is safe)
- Check next rollup cycle for re-verification

### Selector Not Found Errors
- Check [URL_REFERENCE.md](Docs/URL_REFERENCE.md) for selector changes
- Verify KBO site structure hasn't changed
- Run structure validation script before production deployment
