# KBO Playwright Crawler

Korean Baseball Organization (KBO) data collection system using Playwright, with support for Docker, Supabase, and multiple database backends.

## Features

- **2-Track Pipeline**: Separate crawling for Regular Season (game-based) and Futures League (profile-based).
- **Flexible Database Support**: Works with SQLite, PostgreSQL, and MySQL.
- **Dockerized Environment**: Easy setup and deployment with Docker and Docker Compose.
- **Supabase Sync**: CLI tool to synchronize local data with a remote Supabase Postgres database.
- **Automated Scheduling**: APScheduler for daily/weekly data collection.
- **Idempotent Storage**: Safe to re-run crawlers without data duplication thanks to UPSERT logic.

## Data Coverage & Limitations

Based on thorough research of the KBO official website:

- **2001 - Present**: Full data coverage. Both seasonal statistics and detailed game-level data (lineups, box scores, play-by-play) are available and integrated into the modern Game Center.
- **1982 - 2000**: Partial coverage.
    - **Seasonal Stats**: All historical seasonal statistics for hitters and pitchers are available and have been collected.
    - **Game Details**: **NOT AVAILABLE**. The KBO website does not provide interactive box scores or detailed game logs for games prior to 2001. This data is absent from all modern navigation paths and URL patterns.
- **Futures League**: Data coverage varies by season and player profile availability.

## Quick Start

### 1. Environment Setup

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Install Playwright browser binaries
playwright install chromium

# 3. Set up environment variables
cp .env.example .env

# 4. (Optional) Edit .env to configure your database
# Default is SQLite: DATABASE_URL=sqlite:///./data/kbo_dev.db
```

### 2. Initialize Database

Create the database schema from the defined models.

```bash
python -m src.db.engine
```

### 3. Run a Test Crawl

Execute the initial data collection script to test the full pipeline.

```bash
# This script runs the main crawling steps in order
python init_data_collection.py
```

This will:
- Crawl player lists, profiles, and game schedules.
- Save the results to the configured database.

## Development

### Running Individual Crawlers

You can run specific parts of the crawling pipeline using the CLI scripts.

```bash
# Operational daily entrypoint (recommended)
python -m src.cli.run_daily_update --date 20251015

# Crawl and persist monthly schedule
python -m src.cli.crawl_schedule --year 2025 --months 10

# Crawl Futures League stats
python -m src.cli.crawl_futures

# Crawl retired player data
python -m src.cli.crawl_retire --years 2023
```

### Database Operations

```bash
# Check database connection
python -m src.cli.db_healthcheck

# Seed tables (teams + seasons). CSV rows with schema headers are skipped
# automatically; if the CSV lacks data, a built-in 22-team fallback list
# is merged so team FKs stay valid without manual SQL.
python seed_data.py

# NOTE: teams is treated as a semi-static reference table. Supabase still has
# legacy tables (e.g., team_history) referencing it, so `sync_supabase.py`
# always UPSERTs teams and never truncates that table.

# Sync local SQLite data to a remote Postgres/Supabase DB
# Ensure TARGET_DATABASE_URL is set in your .env file
python -m src.cli.sync_supabase --truncate
```

### Docker Deployment

The project is fully containerized for easy deployment.

```bash
# Build and run the scheduler service in the background
docker-compose build
docker-compose up -d scheduler

# View the scheduler logs
docker-compose logs -f scheduler

# Stop the services
docker-compose down
```

## Project Structure

```
KBO_playwright/
├── src/
│   ├── cli/               # Command-line interface scripts
│   ├── crawlers/          # Playwright crawler implementations
│   ├── db/                # Database engine and session management
│   ├── models/            # SQLAlchemy ORM models
│   ├── parsers/           # HTML/JSON parsing logic
│   ├── repositories/      # Data storage logic (UPSERTs)
│   ├── sync/              # Data synchronization utilities
│   └── utils/             # Helper utilities
├── scripts/
│   ├── crawling/          # Historical data crawling scripts
│   ├── supabase/          # Supabase sync and maintenance scripts
│   ├── maintenance/       # Database validation and repair scripts
│   └── scheduler.py       # APScheduler job definitions (production)
├── data/                  # Default directory for SQLite DB
├── Docs/                  # Detailed project documentation
├── migrations/            # Database migration scripts
│   └── supabase/          # Supabase-specific migrations
├── docker-compose.yml     # Docker service definitions
├── Dockerfile             # Docker container setup
├── requirements.txt       # Python dependencies
├── init_db.py             # Database initialization
├── seed_data.py           # Initial data seeding
├── init_data_collection.py # Initial data collection workflow
└── CLAUDE.md              # AI assistant guidance
```

## Scripts

The `scripts/` directory contains utility scripts for maintenance and operations:

### Crawling Scripts (`scripts/crawling/`)
- `crawl_all_historical.py` - Crawl historical game data
- `recrawl_legacy_years.py` - Re-crawl specific historical seasons
- `collect_detailed_data.py` - Collect detailed player profiles and game data

### Supabase Scripts (`scripts/supabase/`)
- `sync_player_basic_first.py` - Initial sync of player basic data
- `check_supabase_data.py` - Verify Supabase data integrity
- `fix_supabase_constraints.py` - Fix foreign key constraints
- `test_supabase_sync.py` - Test Supabase synchronization

### Maintenance Scripts (`scripts/maintenance/`)
- `fix_player_names.py` - Re-crawl and fix player names
- `verify_sqlite_data.py` - Verify local SQLite data quality
- `reset_sqlite.py` - Reset local database
- `check_missing_teams.py` - Find missing team data
- `check_missing_supabase_teams.py` - Compare SQLite vs Supabase `teams` table IDs

## Documentation

See the [Docs/](Docs/) folder for comprehensive documentation:

- **[CLAUDE.md](CLAUDE.md)**: Quick reference for development with AI assistants.
- **[Docs/projectOverviewGuid.md](Docs/projectOverviewGuid.md)**: Operational runbook and procedures.
- **[Docs/URL_REFERENCE.md](Docs/URL_REFERENCE.md)**: Canonical URL patterns and CSS selectors.
- **[Docs/CRAWLING_LIMITATIONS.md](Docs/CRAWLING_LIMITATIONS.md)**: Known data issues and workarounds.

## License

This project is for educational and research purposes. Please respect KBO's terms of service.
