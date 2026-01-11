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

### Supabase
- `sync_player_basic_first.py` - Initial sync of player basic data
- `check_supabase_data.py` - Verify Supabase data integrity
- `test_supabase_sync.py` - Test synchronization

### Crawling
- `crawl_all_historical.py` - Crawl historical game data
- `recrawl_legacy_years.py` - Re-crawl specific seasons
- `collect_detailed_data.py` - Collect detailed profiles
