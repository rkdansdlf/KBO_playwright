# KBO Playwright Crawler

Korean Baseball Organization (KBO) data collection system using Playwright, with support for Docker, OCI PostgreSQL, and multiple database backends.

## Features

- **2-Track Pipeline**: Separate crawling for Regular Season (game-based) and Futures League (profile-based).
- **Flexible Database Support**: Works with SQLite, PostgreSQL, and MySQL.
- **Dockerized Environment**: Easy setup and deployment with Docker and Docker Compose.
- **OCI Sync**: CLI tool to publish validated local SQLite data to OCI PostgreSQL.
- **Automated Scheduling**: APScheduler for daily/weekly data collection.
- **Idempotent Storage**: Safe to re-run crawlers without data duplication thanks to UPSERT logic.
- **Adaptive Rate-Limiting**: Centralized throttling with random jitter to prevent IP blocks.
- **Anti-Bot Protections**: Automatic User-Agent rotation and stealth script injection.
- **Compliance Focused**: Built-in support for `robots.txt` rules and automated compliance checks.

## Data Coverage & Limitations

Based on thorough research of the KBO official website:

- **2001 - Present**: Full data coverage. Both seasonal statistics and detailed game-level data (lineups, box scores, play-by-play) are available and integrated into the modern Game Center.
- **1982 - 2000**: Partial coverage.
    - **Seasonal Stats**: All historical seasonal statistics for hitters and pitchers are available and have been collected.
    - **Game Details**: **NOT AVAILABLE**. The KBO website does not provide interactive box scores or detailed game logs for games prior to 2001. This data is absent from all modern navigation paths and URL patterns.
- **Futures League**: Data coverage varies by season and player profile availability.

## Quick Start

### 1. Setup

```bash
# Automatic setup (venv + deps + playwright + db)
bash scripts/setup.sh

# Or manual:
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
cp env.example .env
# Edit .env as needed, then:
python3 -c "from src.db.engine import init_db; init_db()"
```

### 2. Run the Scheduler

```bash
source venv/bin/activate
python3 -m scripts.scheduler
```

### 3. Test a Single Command

```bash
python3 -m src.cli.crawl_schedule --year 2025 --month 10
```

> **자세한 명령어와 워크플로우는 [`AGENTS.md`](AGENTS.md) 참조.**

## Development

### Running Individual Crawlers

모든 CLI 명령어는 [`AGENTS.md`](AGENTS.md)에 정리되어 있습니다. 주요 명령어:

```bash
# Operational daily entrypoint (recommended)
python3 -m src.cli.run_daily_update --date 20251015

# Completed-game relay/PBP recovery
python3 scripts/fetch_kbo_pbp.py --date 20251015

# Manual monthly detail collection
python3 -m src.cli.collect_games --year 2025 --month 10

# Crawl and persist monthly schedule
python3 -m src.cli.crawl_schedule --year 2025 --months 10

# Futures League stats
python3 -m src.cli.crawl_futures

# Retired player data
python3 -m src.cli.crawl_retire --years 2023
```

### Database Operations

```bash
# Check database health
python3 -m src.cli.health_check

# Seed tables (teams, seasons, data sources)
python3 scripts/legacy/maintenance/seed_data.py

# Sync validated local SQLite data to OCI PostgreSQL
python3 -m src.cli.sync_oci --game-details --unsynced-only
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

## Rate Limiting & Anti-Block

To ensure long-running crawls remain stable and respect the target server's resources, the system implements several anti-block mechanisms:

- **Centralized Throttling**: All crawlers use a shared `AsyncThrottle` service that enforces a minimum delay between requests.
- **Random Jitter**: A random delay (default ±0.3s) is added to each request to simulate human-like behavior.
- **User-Agent Rotation**: Every browser session automatically picks a random, modern User-Agent from a curated pool.
- **Stealth Injection**: Evasion scripts are injected to mask automation flags (e.g., `navigator.webdriver`).

### Configuration

You can tune these settings in your `.env` file:

| Variable | Default | Description |
|---|---|---|
| `KBO_REQUEST_DELAY` | `1.5` | Base delay between requests in seconds. |
| `KBO_REQUEST_JITTER` | `0.3` | Random variance added to the base delay. |
| `KBO_UA_ROTATION` | `true` | Whether to enable automatic User-Agent rotation. |

## Robots.txt & Compliance

The system is designed to be a "good citizen" crawler:

- **Automated Checks**: The `compliance` utility fetches and parses `https://www.koreabaseball.com/robots.txt` to ensure target paths are allowed.
- **Fail-Safe**: Crawlers will automatically abort navigation if a `Disallow` rule is detected for the target URL.
- **Snapshots**: Compliance snapshots are stored under `Docs/robots/` for auditing.

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
│   ├── maintenance/       # Database validation and repair scripts
│   ├── verification/      # Data quality verification
│   └── scheduler.py       # APScheduler job definitions (production)
├── .github/
│   ├── workflows/         # GitHub Actions CI/CD (14 workflows)
│   └── actions/           # Composite actions (python-env, notify)
├── data/                  # Default directory for SQLite DB
├── Docs/                  # Detailed project documentation
├── migrations/            # Database migration scripts (SQLite + OCI)
├── tests/                 # Pytest test suite (1100+ tests)
├── docker-compose.yml     # Docker service definitions
├── Dockerfile             # Docker container setup
├── env.example            # Environment variable template
├── requirements.txt       # Python dependencies
└── AGENTS.md              # Full command reference & CI/CD guide
```

## Scripts

The `scripts/` directory contains utility scripts for maintenance and operations:

### Crawling Scripts (`scripts/crawling/`)
- `crawl_all_historical.py` - Crawl historical game data
- `recrawl_legacy_years.py` - Re-crawl specific historical seasons
- `collect_detailed_data.py` - Deprecated wrapper; use `python -m src.cli.collect_games` or `python -m src.cli.run_daily_update`

### Maintenance Scripts (`scripts/legacy/maintenance/`)
- `fix_player_names.py` - Re-crawl and fix player names
- `verify_sqlite_data.py` - Verify local SQLite data quality
- `reset_sqlite.py` - Reset local database
- `check_missing_teams.py` - Find missing team data
- `fill_oci_null_player_ids_from_local.py` - Repair OCI NULL `player_id` values from validated local rows

> **Note:** Legacy maintenance scripts moved to `scripts/legacy/maintenance/`. Active CLI commands prefer `python3 -m src.cli.*`.

## Documentation

See the [Docs/](Docs/) folder and [`AGENTS.md`](AGENTS.md) for comprehensive documentation:

- **[AGENTS.md](AGENTS.md)**: Full command reference, CI/CD workflows, required secrets.
- **[Docs/](Docs/)**: Project overview, runbooks, URL patterns, limitations.
- **[Docs/references/COMMAND_REFERENCE.md](Docs/references/COMMAND_REFERENCE.md)**: Detailed CLI command guide.
- **[Docs/references/SCHEDULER_README.md](Docs/references/SCHEDULER_README.md)**: APScheduler job definitions.
- **[Docs/zero_issue_runbook_oci.md](Docs/zero_issue_runbook_oci.md)**: OCI sync & quality gates.

## License

This project is for educational and research purposes. Please respect KBO's terms of service.
