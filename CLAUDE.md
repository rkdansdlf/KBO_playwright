# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

KBO_playwright is a Python-based web scraping project that collects Korean Baseball Organization (KBO) data using Playwright. The project implements a **2-track data collection pipeline** that separates regular season (game-based) and Futures League (profile-based) data collection strategies. It supports both SQLite for local development and PostgreSQL/MySQL for production, with Docker and Supabase integration.

## Core Architecture

### Two-Track Pipeline Design

1.  **Track A: Regular Season (Game-Based)**
    *   Collects game-by-game detailed statistics from GameCenter.
    *   Data sources: Schedule, BoxScore, Lineup Analysis (XHR), Play-by-Play (PBP).
    *   Nightly rollup process aggregates game data into season statistics.
    *   High-frequency updates (daily at 03:00 KST).

2.  **Track B: Futures League (Profile-Based)**
    *   Collects season-cumulative statistics from player profile pages.
    *   Data source: Player profile "Futures" tab.
    *   Low-frequency updates (weekly on Sunday at 05:00 KST).
    *   No game-by-game detail available.

### Dual Repository Pattern (SQLite + Supabase)

```
Crawl → SQLite (validation) → Supabase (production)
```

- **SQLite**: Local development, fast writes, easy debugging, data validation
- **Supabase (PostgreSQL)**: Production storage, persistent, API access, realtime
- **Sync**: `src/sync/supabase_sync.py` handles validated data transfer
- **Setup**: See [Docs/SUPABASE_SETUP.md](Docs/SUPABASE_SETUP.md)

### Data Processing Flow

All data collection follows a strict 4-stage process:
1.  **Collect**: Fetch HTML/JSON using Playwright.
2.  **Parse**: Extract data using dedicated parsers (e.g., `futures_stats_parser`).
3.  **Validate**: Data quality checks in SQLite (see `verify_sqlite_data.py`).
4.  **Save**: Use repository patterns (`save_futures_batting`) to UPSERT data idempotently into the database (SQLite/PostgreSQL/MySQL).
5.  **Sync**: Transfer validated data to Supabase for production use.

## Development Commands

### Environment Setup
```bash
# Install Python dependencies
pip install -r requirements.txt

# Install Playwright browser (required after pip install)
playwright install chromium

# Set up environment variables
cp .env.example .env
# --> Edit .env to configure DATABASE_URL
```

### Database Operations
```bash
# Initialize SQLite database (creates tables from models)
./venv/bin/python3 init_db.py

# Seed initial team data (franchises, ballparks, etc.)
./venv/bin/python3 seed_teams.py

# Verify SQLite data integrity
./venv/bin/python3 verify_sqlite_data.py

# Sync validated data to Supabase
export SUPABASE_DB_URL='postgresql://postgres.xxx:[PASSWORD]@xxx.pooler.supabase.com:5432/postgres'
./venv/bin/python3 src/sync/supabase_sync.py
```

### Running Crawlers
```bash
# Regular season game crawler (manual execution)
python src/cli/crawl_game_details.py --date 20251013

# Futures league profile crawler (manual execution)
python src/cli/crawl_futures.py

# Retired/inactive player crawler (monthly maintenance)
python src/cli/crawl_retire.py --years 1982-2025 --concurrency 3
```

### Scheduler & Docker
```bash
# Run automated scheduler locally (APScheduler)
python scheduler.py

# Build and run with Docker
docker-compose build
docker-compose up -d scheduler

# View logs
docker-compose logs -f scheduler
```

## Key Technical Considerations

### Database Management
- **Engine**: `src/db/engine.py` provides a factory `create_engine_for_url` that configures SQLite, PostgreSQL, or MySQL based on the `DATABASE_URL` in `.env`.
- **Idempotency**: Repositories like `save_futures_batting.py` use dialect-specific `INSERT ... ON CONFLICT` (SQLite) or `ON DUPLICATE KEY UPDATE` (MySQL) for safe, repeatable data saving.
- **Data Sync**: `src/cli/sync_supabase.py` provides a robust mechanism to transfer data from a source (e.g., local SQLite) to a target (e.g., Supabase Postgres), handling all registered models in the correct dependency order.

### Web Scraping Etiquette
- **Rate Limiting**: Minimum 1-2 second delay between requests.
- **Execution Window**: Schedule for 02:00-05:00 KST (low traffic hours).
- **User-Agent**: Use an identifiable string.

### Playwright Best Practices
- Use `page.wait_for_selector()` for dynamic content.
- Use `page.wait_for_load_state('networkidle')` for AJAX-heavy pages.
- Wrap operations in `try...except` blocks.

### Data Identification
- Regular season data: `league='KBO'`, `source='game'`
- Futures league data: `league='FUTURES'`, `source='profile'`
- All saves use UPSERT for idempotency.

### Key Selectors (Fragile - Verify on Site Changes)

**Regular Season:**
- Game links: `a[href*="gameId="]`
- Away hitters: `.tblAwayHitter1`, `.tblAwayHitter2`
- Home hitters: `.tblHomeHitter1`, `.tblHomeHitter2`
- Pitcher tables: `div.away-pitcher-record table`, `div.home-pitcher-record table`

**Futures League:**
- Futures tab: `//a[contains(text(), '퓨처스')]` (XPath)
- Stats table: `div#cphContents_cphContents_cphContents_udpPlayerFutures > table.tbl.tt`

**Important**: KBO website can change selectors. See [Docs/URL_REFERENCE.md](Docs/URL_REFERENCE.md).

### Critical URLs

- Schedule: `https://www.koreabaseball.com/Schedule/Schedule.aspx`
- GameCenter: `https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={id}&section=REVIEW`
- Player Profile: `https://www.koreabaseball.com/Record/Player/HitterDetail/Basic.aspx?playerId={id}`

## Scheduler Configuration (APScheduler)

1.  **Regular Season Rollup** (`crawl_games_regular`)
    *   Frequency: Daily at 03:00 KST
    *   Purpose: Collect previous day's games.

2.  **Futures Sync** (`crawl_futures_profile`)
    *   Frequency: Weekly (Sunday at 05:00 KST)
    *   Purpose: Sync all player Futures league cumulative stats.

## Important Documentation

- **[Docs/projectOverviewGuid.md](Docs/projectOverviewGuid.md)** - Detailed operational runbook.
- **[Docs/URL_REFERENCE.md](Docs/URL_REFERENCE.md)** - Canonical URL patterns and selectors.
- **[Docs/SCHEDULER_README.md](Docs/SCHEDULER_README.md)** - APScheduler strategy and job configuration.
- **[Docs/CRAWLING_LIMITATIONS.md](Docs/CRAWLING_LIMITATIONS.md)** - Known data quality issues.

