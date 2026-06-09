# Scripts Directory

This directory contains utility scripts for maintenance and operations.

## Usage

All Python commands use `python3 -m src.cli.*` for active operations.
Legacy scripts in `scripts/legacy/maintenance/` (36 active) may still be run directly:

```bash
PYTHONPATH=. python3 scripts/legacy/maintenance/verify_sqlite_data.py
```

## Directory Structure

- **crawling/** - Historical data crawling scripts
- **legacy/maintenance/** - 36 active legacy maintenance scripts
- **archived/legacy/maintenance/** - 155 one-shot / historical scripts (no longer actively used)
- **archived/supabase/** - 7 deprecated Supabase sync scripts
- **legacy/** - Other legacy scripts (quality_gate.py, run_final_pipeline.sh)

## Operational Collection

Use `src.cli` entrypoints for DB-writing schedule/detail collection:
- `python3 -m src.cli.crawl_schedule --year 2025 --month 3`
- `python3 -m src.cli.collect_games --year 2025 --month 10`
- `python3 -m src.cli.run_daily_update --date YYYYMMDD`

## Automation & Scheduling

- `scheduler.py` - APScheduler-based automation for KBO data collection.
  Uses a **3-stage locking mechanism** (`LIVE_LOCK`, `DAILY_LOCK`, `MAINTENANCE_LOCK`).
