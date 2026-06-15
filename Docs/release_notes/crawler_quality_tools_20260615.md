# Crawler Quality Tools Release Notes - 2026-06-15

## Summary
- Added crawler selector validation, crawler failure diagnosis, and compact data quality regression tools.
- Improved live crawler detail snapshot handling, preview lineup validation, relay player-side detection, and profile backfill workflows.
- Reduced broad exception handling in key daily update, season-stat crawler, schedule/detail crawler, and audit scripts.

## Release Contents
- **Selector gate:** `src.cli.crawler_selector_gate` validates selector contracts from `Docs/references/crawler_selector_gate.json` and can write JSON/artifacts for drift investigation.
- **Failure diagnosis:** `src.cli.diagnose_crawler_failure` classifies crawler logs and emits text or JSON triage reports.
- **Data quality regression pack:** `src.cli.data_quality_regression_pack` checks compact DB invariants for common crawl/parser regressions.
- **Live and preview crawler hardening:** live detail snapshots now run in a non-cadence-critical background path, preview API payloads reject stale lineup rows, and fielding selectors are centralized.
- **Relay enrichment:** relay player resolution distinguishes defensive substitutions from offensive text when play descriptions indicate a defensive target.
- **Profile backfill workflow:** scheduled profile backfill supports configurable batch/delay behavior and OCI sync of profile tables.
- **Exception hygiene:** reduced `except Exception` in high-volume orchestration and crawler files by replacing them with concrete Playwright, SQLAlchemy, subprocess, HTTP, file, and parser exception sets.

## Verification Commands
```bash
venv/bin/python -m ruff check .
venv/bin/python -m ruff format --check .
venv/bin/python -m pytest -q
```

Focused verification used during release preparation:

```bash
venv/bin/python -m pytest -q tests/cli/test_run_daily_update.py
venv/bin/python -m pytest -q tests/crawlers/test_player_batting_all_series_crawler.py tests/crawlers/test_complete_kbo_player_crawler.py tests/crawlers/test_player_pitching_all_series_crawler.py
venv/bin/python -m pytest -q tests/test_preview_crawler_payloads.py tests/crawlers/test_fielding_stats_crawler.py tests/cli/test_run_advanced_daily.py
venv/bin/python -m pytest -q tests/test_schedule_crawler_stability.py tests/crawlers/test_game_detail_crawler.py tests/test_game_detail_crawler_parsing.py tests/test_game_detail_crawler_stability.py tests/test_game_detail_crawler_roster_fallback.py tests/cli/test_run_all_crawlers.py
```

## Operational Commands
```bash
python3 -m src.cli.crawler_selector_gate --config Docs/references/crawler_selector_gate.json --json
python3 -m src.cli.diagnose_crawler_failure --json logs/<logfile>.log
python3 -m src.cli.data_quality_regression_pack --json
```

## Follow-Up Notes
- `scripts/scheduler.py` still has many broad exception handlers and should be handled separately because it is often edited by scheduler/backfill work.
- Live selector checks should remain advisory until `Docs/references/crawler_selector_gate.json` is stable under CI network conditions.
- See `Docs/troubleshooting/crawler_quality_tools.md` for operational usage and output interpretation.
