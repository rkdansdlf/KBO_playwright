# KBO Playwright Crawler

Korean Baseball Organization (KBO) data collection system using Playwright, with support for Docker, Supabase, and multiple database backends.

## Features

- **2-Track Pipeline**: Separate crawling for Regular Season (game-based) and Futures League (profile-based).
- **Flexible Database Support**: Works with SQLite, PostgreSQL, and MySQL.
- **Dockerized Environment**: Easy setup and deployment with Docker and Docker Compose.
- **Supabase Sync**: CLI tool to synchronize local data with a remote Supabase Postgres database.
- **Automated Scheduling**: APScheduler for daily/weekly data collection.
- **Idempotent Storage**: Safe to re-run crawlers without data duplication thanks to UPSERT logic.

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
# Crawl game details for a specific date
python -m src.cli.crawl_game_details --date 20241015

# Crawl Futures League stats
python -m src.cli.crawl_futures

# Crawl retired player data
python -m src.cli.crawl_retire --years 2023
```

### Database Operations

```bash
# Check database connection
python -m src.cli.db_healthcheck

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
│   └── repositories/      # Data storage logic (UPSERTs)
├── data/                  # Default directory for SQLite DB
├── Docs/                  # Detailed project documentation
├── docker-compose.yml     # Docker service definitions
├── Dockerfile             # Docker container setup
├── requirements.txt       # Python dependencies
├── scheduler.py           # APScheduler job definitions
└── CLAUDE.md              # AI assistant guidance
```

## Documentation

See the [Docs/](Docs/) folder for comprehensive documentation:

- **[CLAUDE.md](CLAUDE.md)**: Quick reference for development with AI assistants.
- **[Docs/projectOverviewGuid.md](Docs/projectOverviewGuid.md)**: Operational runbook and procedures.
- **[Docs/URL_REFERENCE.md](Docs/URL_REFERENCE.md)**: Canonical URL patterns and CSS selectors.
- **[Docs/CRAWLING_LIMITATIONS.md](Docs/CRAWLING_LIMITATIONS.md)**: Known data issues and workarounds.

## License

This project is for educational and research purposes. Please respect KBO's terms of service.
