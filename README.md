# KBO Playwright Crawler

Korean Baseball Organization (KBO) data collection system using Playwright.

## Features

- **2-Track Pipeline**: Separate crawling strategies for Regular Season (game-based) and Futures League (profile-based)
- **Automated Scheduling**: APScheduler for daily/weekly data collection
- **Idempotent UPSERT**: Safe to re-run without data duplication
- **Production-Ready Architecture**: Full database schema and Airflow DAG implementation

## Quick Start

### 1. Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Install Playwright browser
playwright install chromium
```

### 2. Run POC Test

Test the basic crawling functionality:

```bash
# Activate virtual environment (if using venv)
source venv/bin/activate

# Run POC test
python poc_test.py
```

This will:
- Crawl the schedule for October 2024
- Extract game IDs
- Crawl detailed data for one game
- Save results to `data/` directory as JSON files

### 3. Project Structure

```
KBO_playwright/
├── src/
│   ├── crawlers/          # Crawler implementations
│   │   ├── schedule_crawler.py
│   │   └── game_detail_crawler.py
│   └── utils/             # Utility functions
├── data/                  # Output data directory
├── logs/                  # Log files
├── Docs/                  # Detailed documentation
│   ├── projectOverviewGuid.md
│   ├── URL_REFERENCE.md
│   ├── SCHEDULER_README.md
│   └── schema/
│       └── KBOseasonGamePipeLine.md  # Complete architecture
├── requirements.txt
├── poc_test.py           # POC test script
└── CLAUDE.md             # Claude Code guidance

```

## Documentation

See [Docs/](Docs/) folder for comprehensive documentation:

- **[CLAUDE.md](CLAUDE.md)** - Quick reference for development
- **[Docs/projectOverviewGuid.md](Docs/projectOverviewGuid.md)** - Operational runbook
- **[Docs/schema/KBOseasonGamePipeLine.md](Docs/schema/KBOseasonGamePipeLine.md)** - Complete production architecture
- **[Docs/URL_REFERENCE.md](Docs/URL_REFERENCE.md)** - URL patterns and CSS selectors
- **[Docs/CRAWLING_LIMITATIONS.md](Docs/CRAWLING_LIMITATIONS.md)** - Known issues and workarounds

## Key Concepts

### Two-Track Pipeline

1. **Track A: Regular Season (Game-Based)**
   - Daily crawling at 03:00 KST
   - BoxScore, Lineup, Play-by-Play data
   - Nightly aggregation to season stats

2. **Track B: Futures League (Profile-Based)**
   - Weekly crawling (Sunday 05:00 KST)
   - Season-cumulative stats from player profiles
   - Lower frequency due to data stability

### Data Flow

```
Collect → Parse → Validate → Save (UPSERT)
```

All operations follow this 4-stage process for data quality and idempotency.

## Web Scraping Etiquette

- **Rate Limiting**: 1-2 second delay between requests
- **Execution Window**: 02:00-05:00 KST (low traffic)
- **Exponential Backoff**: Automatic retry with increasing delays
- **Respect robots.txt**: Check before crawling new paths

## Development

### Running Individual Crawlers

```bash
# Schedule crawler
python -m src.crawlers.schedule_crawler

# Game detail crawler
python -m src.crawlers.game_detail_crawler
```

### Configuration

Create `.env` (see `.env.example`) for configuration:

```env
DATABASE_URL=sqlite:///./data/kbo_dev.db
# or MySQL (requires mysql-connector-python)
# DATABASE_URL=mysql+mysqlconnector://user:pass@host:3306/kbo
REQUEST_DELAY=1.5
MAX_RETRIES=3
```

Verify DB connectivity:

```bash
python -m src.cli.db_healthcheck
```

## Next Steps

After POC validation:

1. **Database Layer**: Implement SQLAlchemy models and repositories
2. **Data Validation**: Add Pydantic schemas and validation rules
3. **Scheduler**: Set up APScheduler for automated runs
4. **Production Pipeline**: Deploy Airflow DAGs (see Docs/schema/)

## License

This project is for educational and research purposes. Please respect KBO's terms of service and robots.txt.
