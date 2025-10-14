# KBO Crawler Scheduler Usage

## Overview

The scheduler automates KBO data collection using APScheduler with two independent jobs:

1. **Daily Games Crawl** (03:00 KST): Collects previous day's game results
2. **Weekly Futures Sync** (Sunday 05:00 KST): Syncs Futures League stats for all active players

## Quick Start

### Option 1: Local Python (Development)

```bash
# Install dependencies (if not already done)
pip install -r requirements.txt

# Run scheduler
python scheduler.py
```

### Option 2: Docker (Production)

```bash
# Build and start scheduler
docker-compose up -d scheduler

# View logs
docker-compose logs -f scheduler

# Stop scheduler
docker-compose down
```

## Manual Crawl Commands

### Docker

```bash
# Crawl schedule
docker-compose --profile manual run --rm crawl_schedule

# Crawl Futures stats
docker-compose --profile manual run --rm crawl_futures

# Check data status
docker-compose --profile manual run --rm check_data
```

### Local Python

```bash
# Crawl 2025 regular season schedule
python -m src.cli.crawl_schedule --season-year 2025 --season-type regular

# Crawl Futures stats for active players
python -m src.cli.crawl_futures --season 2025 --concurrency 2 --delay 2.0

# Check database status
python -m src.cli.check_data_status
```

## Schedule Configuration

Jobs are defined in `scheduler.py`:

- **Daily Games**: `CronTrigger(hour=3, minute=0)` - Every day at 03:00 KST
- **Futures Sync**: `CronTrigger(day_of_week='sun', hour=5, minute=0)` - Every Sunday at 05:00 KST

Both jobs include:
- **Retry Logic**: 3 attempts with exponential backoff
- **Grace Period**: 1-2 hours for missed executions
- **Max Instances**: 1 (prevents concurrent runs)

## Rate Limiting & Best Practices

From `CRAWLING_LIMITATIONS.md`:

1. **Minimum Request Delay**: 1-2 seconds between requests
2. **Execution Window**: 02:00-05:00 KST (low traffic hours)
3. **Futures Concurrency**: Max 2-3 concurrent requests
4. **Post-Game Delay**: Wait 1+ hour after game end before crawling

## File Structure

```
D:\project\KBO_playwright\
├── scheduler.py            # APScheduler main script
├── Dockerfile              # Container definition
├── docker-compose.yml      # Docker services config
├── requirements.txt        # Python dependencies
├── logs/                   # Scheduler logs (created automatically)
│   └── scheduler.log
├── data/                   # SQLite database
│   └── kbo_dev.db
└── src/
    └── cli/                # CLI scripts
        ├── crawl_schedule.py
        ├── crawl_futures.py
        ├── crawl_game_details.py
        └── check_data_status.py
```

## Monitoring

### Check Scheduler Status

```bash
# Docker
docker-compose logs --tail=50 scheduler

# Local (check logs/scheduler.log)
tail -f logs/scheduler.log
```

### Verify Data Collection

```bash
# Docker
docker-compose --profile manual run --rm check_data

# Local
python -m src.cli.check_data_status
```

Expected output:
```
=== Summary ===
  Schedules: 770
  Players: 3+
  Futures batting: 9+
  Game batting: 16+
```

## Troubleshooting

### Scheduler won't start

1. Check Python version: `python --version` (requires 3.10+)
2. Install dependencies: `pip install -r requirements.txt`
3. Check logs: `cat logs/scheduler.log`

### Jobs not running

1. Verify timezone: `TZ=Asia/Seoul` in docker-compose.yml
2. Check cron syntax in `scheduler.py`
3. Review misfire_grace_time settings

### Rate limit errors (429)

1. Increase delay in crawl commands: `--delay 3.0`
2. Reduce concurrency: `--concurrency 1`
3. Check if running during low-traffic hours (02:00-05:00 KST)

### Missing data after crawl

1. Verify game completed at least 1 hour ago
2. Re-run crawl (UPSERT is idempotent)
3. Check crawl_status table for error messages

## Environment Variables

Create `.env` file for database configuration:

```env
DATABASE_URL=sqlite:///./data/kbo_dev.db
# or for MySQL (requires mysql-connector-python):
# DATABASE_URL=mysql+mysqlconnector://user:pass@host:3306/kbo
```

## Production Deployment

1. Update `.env` with production database URL
2. Build Docker image: `docker-compose build`
3. Start scheduler: `docker-compose up -d scheduler`
4. Set up monitoring (Prometheus/Grafana recommended)
5. Configure log rotation in docker-compose.yml

## References

- `Docs/SCHEDULER_README.md`: Detailed architecture
- `Docs/CRAWLING_LIMITATIONS.md`: Rate limits and best practices
- `Docs/Progress.md`: Project status and completed features
