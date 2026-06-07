# Scripts Directory

This directory contains utility scripts for maintenance and operations.

## Usage

All scripts should be run from the project root directory:

```bash
# From project root
cd /path/to/KBO_playwright

# Run scripts with Python module syntax
python -m scripts.legacy.maintenance.verify_sqlite_data

# Or use PYTHONPATH
PYTHONPATH=. python scripts/legacy/maintenance/verify_sqlite_data.py
```

## Directory Structure

- **crawling/** - Historical data crawling scripts
- **legacy/maintenance/** - Database validation and repair (legacy scripts)

## Available Scripts

### Maintenance
- `verify_sqlite_data.py` - Verify local SQLite data quality
- `fix_player_names.py` - Re-crawl and fix player names
- `reset_sqlite.py` - Reset local database
- `check_missing_teams.py` - Find missing team data
- `smart_deduplicate.py`, `deduplicate_games.py`, `hard_deduplicate.py` - Primary-game calibration wrappers over `src.services.game_deduplication_service`
- `cleanup_temp_artifacts.py` - Manual cleanup for temporary artifacts under `data/`, `logs/`, `snapshots/`, `scratch/`, and root debug outputs.

### 임시 산출물 정리 가이드 (수동 실행, 기본 보존 7일)

기본 보존 정책은 `data/kbo_dev.db`, `data/kbo_auth_state.json`, `data/*overrides*.csv`, `data/backups/*.db`를 제외한 다음 임시 산출물을 7일 초과 시 삭제합니다.

```bash
# 삭제 대상 예측(실제 삭제 없음)
python scripts/legacy/maintenance/cleanup_temp_artifacts.py --days 7 --dry-run

# 대상만 요약 출력
python scripts/legacy/maintenance/cleanup_temp_artifacts.py --days 7 --whatif

# 실제 삭제(운영자 수동 실행)
python scripts/legacy/maintenance/cleanup_temp_artifacts.py --days 7
```

카테고리 단일 정리:

```bash
python scripts/legacy/maintenance/cleanup_temp_artifacts.py --only scratch --dry-run
python scripts/legacy/maintenance/cleanup_temp_artifacts.py --only daily_update_summary --days 14
```

추가 옵션:

- `--days`: 보존일수(기본 7일) 기준
- `--dry-run`: 삭제 없이 대상만 표시
- `--whatif`: 삭제 대상만 요약 표시
- `--verbose`: 파일 목록까지 상세 출력
- `--protect`: 추가 보호 패턴(`data/...`) 지정 가능(상대 경로 기반 glob)
- `--include-root-json`: 기본 제외 대상이던 루트 `*.json`까지 삭제 후보에 포함
- `.gitignore`에는 `/*.png`, `/*.html`, `/*.json`을 추가해 루트 임시 산출물 재생성을 최소화(스크립트 기본 동작은 `--include-root-json` 미사용 시 제외)

수동 정리 체크리스트:
- 삭제 예측 목록/건수 확인(요약 로그)
- `du -sh data logs` 실행 전/후 비교
- `ls -1 logs/daily_update_summary` 및 `ls -1 logs/quality_reports` 필수 파일 유지 확인
- `test -f data/kbo_dev.db`로 보호 DB 존재 확인
- 필요 시 `python3 -m src.cli.crawl_schedule --year 2025 --months 3` 또는 `python3 -m src.cli.run_daily_update --date ...`로 다음 실행 영향 확인

### OCI
- `check_oci_summary.py` - Summarize OCI database contents
- `debug_oci.py` - Inspect OCI connection and selected rows
- `fast_sync_stats.py`, `sync_2002_2009.py`, `sync_all_game_details.py` - OCI-oriented sync helpers

### Crawling
- `crawl_all_historical.py` - Crawl historical game data
- `recrawl_legacy_years.py` - Re-crawl specific seasons
- `collect_detailed_data.py` - Deprecated wrapper; use `python -m src.cli.collect_games` or `python -m src.cli.run_daily_update`
- `collect_international_games.py` - Dedicated international schedule crawler; DB writes use the shared game snapshot persistence path

### Operational Collection
Use the `src.cli` entrypoints for DB-writing schedule/detail collection, and the relay recovery script for finalized PBP recovery:
- `python -m src.cli.crawl_schedule --year 2025 --months 3`
- `python -m src.cli.collect_games --year 2025 --month 3`
- `python -m src.cli.run_daily_update --date YYYYMMDD`
- `python scripts/fetch_kbo_pbp.py --date YYYYMMDD`

### Automation & Scheduling
- `scheduler.py` - APScheduler-based automation for KBO data collection. Manages live refreshes, pregame checks, daily updates, and maintenance tasks.
  - Uses a **3-stage locking mechanism** (`LIVE_LOCK`, `DAILY_LOCK`, `MAINTENANCE_LOCK`) to prevent concurrency issues between real-time, daily, and background jobs.

### Manual Debug and Manifest Scripts
These scripts call crawlers directly for parser investigation or write JSON manifests only. They are not standard DB-writing collection paths:
- `scripts/legacy/maintenance/init_data_collection.py`
- `scripts/legacy/maintenance/debug_missing_game.py`
- `scripts/legacy/maintenance/check_kia_code.py`
- `scripts/legacy/maintenance/debug_modern_crawler.py`
- `scripts/legacy/maintenance/test_cancel_detect.py`
- `scripts/legacy/maintenance/test_2019_crawl.py`
- `scripts/legacy/maintenance/verify_2018_fix.py`
- `scripts/legacy/maintenance/collect_historical_game_ids.py`
- `scripts/legacy/maintenance/crawl_historical_schedule.py`
- `scripts/crawl_2009_game_details.py`
- `scripts/legacy/maintenance/prototype_2000_crawler.py`
