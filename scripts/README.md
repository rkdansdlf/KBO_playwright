# Scripts Directory

This directory contains utility scripts for maintenance and operations.

## Usage

All scripts should be run from the project root directory:

```bash
# From project root
cd /path/to/KBO_playwright

# Run scripts with Python module syntax
python -m scripts.maintenance.verify_sqlite_data

# Or use PYTHONPATH
PYTHONPATH=. python scripts/maintenance/verify_sqlite_data.py
```

## Directory Structure

- **crawling/** - Historical data crawling scripts
- **supabase/** - Supabase sync and maintenance 
- **maintenance/** - Database validation and repair

## Available Scripts

### Maintenance
- `verify_sqlite_data.py` - Verify local SQLite data quality
- `fix_player_names.py` - Re-crawl and fix player names
- `reset_sqlite.py` - Reset local database
- `check_missing_teams.py` - Find missing team data
- `smart_deduplicate.py`, `deduplicate_games.py`, `hard_deduplicate.py` - Primary-game calibration wrappers over `src.services.game_deduplication_service`

### Supabase
- `sync_player_basic_first.py` - Initial sync of player basic data
- `check_supabase_data.py` - Verify Supabase data integrity
- `test_supabase_sync.py` - Test synchronization

### Crawling
- `crawl_all_historical.py` - Crawl historical game data
- `recrawl_legacy_years.py` - Re-crawl specific seasons
- `collect_detailed_data.py` - Deprecated wrapper; use `python -m src.cli.collect_games` or `python -m src.cli.run_daily_update`
- `collect_international_games.py` - Dedicated international schedule crawler; DB writes use the shared game snapshot persistence path

### Operational Collection
Use the `src.cli` entrypoints for DB-writing schedule/detail collection:
- `python -m src.cli.crawl_schedule --year 2025 --months 3`
- `python -m src.cli.collect_games --year 2025 --month 3`
- `python -m src.cli.run_daily_update --date YYYYMMDD`

### Manual Debug and Manifest Scripts
These scripts call crawlers directly for parser investigation or write JSON manifests only. They are not standard DB-writing collection paths:
- `scripts/maintenance/init_data_collection.py`
- `scripts/maintenance/debug_missing_game.py`
- `scripts/maintenance/check_kia_code.py`
- `scripts/maintenance/debug_modern_crawler.py`
- `scripts/maintenance/test_cancel_detect.py`
- `scripts/maintenance/test_2019_crawl.py`
- `scripts/maintenance/verify_2018_fix.py`
- `scripts/maintenance/collect_historical_game_ids.py`
- `scripts/maintenance/crawl_historical_schedule.py`
- `scripts/crawl_2009_game_details.py`
- `scripts/maintenance/prototype_2000_crawler.py`
