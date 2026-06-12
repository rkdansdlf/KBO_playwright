# Scripts Directory

This directory contains utility scripts for maintenance and operations.

## Usage

Use `python3 -m src.cli.*` for active application operations and `python3 -m scripts.maintenance.*` for supported maintenance helpers.

```bash
python3 -m scripts.maintenance.quality_gate --skip-oci
python3 -m scripts.maintenance.discover_wayback_relay_captures --limit 10
```

## Directory Structure

- **crawling/** - Historical data crawling scripts
- **maintenance/** - Supported maintenance helpers used by schedulers/workflows
- **diagnostic/** - Focused diagnostics and fixture capture utilities
- **verification/** - Integrity and consistency verification commands
- **supabase/** - Supabase-specific inspection and maintenance helpers

## Root-Level Scripts

Root-level scripts are retained when they are direct entrypoints, referenced by
automation, or covered as standalone utilities. Use the categories below before
adding or moving a script:

- **Automation**: `scheduler.py`
- **Backfill and batch operations**: `backfill_birthdates.py`, `backfill_player_profiles.py`, `batch_parse_snapshots.py`, `bulk_recalc_team_stats.py`
- **Historical crawling/recovery**: `crawl_2002_2009_stats.py`, `crawl_2009_game_details.py`, `fetch_kbo_pbp.py`
- **Sync and migration utilities**: `fast_sync_stats.py`, `migrate_fielding_oci.py`, `sync_2002_2009.py`, `sync_all_game_details.py`
- **Seed data**: `seed_fan_culture.py`, `seed_parking.py`, `seed_seat_sections.py`, `seed_stadium_food.py`, `seed_stadium_info.py`
- **Diagnostics/investigation**: `dump_defense_html.py`, `historical_analysis.py`, `historical_gap_analysis.py`, `inspect_catcher_table.py`, `inspect_defense_dropdowns.py`, `inspect_defense_tabs.py`, `investigate_2009_game_detail.py`
- **Developer cleanup tools**: `convert_print_to_logger.py`, `fix_bare_except.py`, `lint_bare_except.py`
- **One-off repair/benchmark tools**: `benchmark_oci_sync.py`, `cleanup_corrupted_stats.py`, `fix_lotte_code.py`

## Operational Collection

Use `src.cli` entrypoints for DB-writing schedule/detail collection:
- `python3 -m src.cli.crawl_schedule --year 2025 --month 3`
- `python3 -m src.cli.collect_games --year 2025 --month 10`
- `python3 -m src.cli.run_daily_update --date YYYYMMDD`

## Automation & Scheduling

- `scheduler.py` - APScheduler-based automation for KBO data collection.
  Uses a **3-stage locking mechanism** (`LIVE_LOCK`, `DAILY_LOCK`, `MAINTENANCE_LOCK`).
