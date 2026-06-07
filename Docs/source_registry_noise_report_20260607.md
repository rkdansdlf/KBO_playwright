# Source Registry Noise Report - 2026-06-07

## Scope

This report records the post-fix verification for source-registry success tracking and the P0 team event source repair. No files were staged, committed, or reverted.

## Worktree Groups

| Group | Paths | Verification |
| --- | --- | --- |
| P0 Events | `src/crawlers/team_event_crawler.py`, `src/parsers/team_event_parser.py`, `src/repositories/team_event_repository.py`, `tests/test_team_event_parser.py`, `tests/test_team_event_repository.py` | `ruff check` on touched files; `pytest tests/test_team_event_parser.py tests/test_integration_team_event_parser.py tests/test_team_event_repository.py` |
| Source Registry | `src/repositories/source_registry_repository.py`, `tests/test_phase0_source_registry_repository.py`, `tests/test_source_registry_success_tracking.py` | `pytest tests/test_phase0_source_registry_repository.py tests/test_source_registry_success_tracking.py` |
| Monitor/Health | `src/cli/health_check.py`, `tests/test_scheduler_alerting.py` | `pytest tests/test_scheduler_alerting.py -k health_and_freshness`; `monitor_data_freshness --no-alert`; `health_check` |
| Scheduler/Futures | `scripts/scheduler.py`, `src/cli/crawl_futures.py`, `tests/test_crawl_futures_stability.py`, `tests/test_game_detail_crawler_stability.py` | `pytest tests/test_crawl_futures_stability.py tests/test_game_detail_crawler_stability.py` |
| GitHub Workflows | `.github/actions/python-env/action.yml`, `.github/workflows/*.yml`, `tests/test_github_workflows.py` | Present in worktree; not changed by this event-source pass |
| Sync Tests | `tests/test_sync_do_bulk_copy_upsert.py`, `tests/test_sync_game_summary.py`, `tests/test_sync_table_signature.py` | Untracked; not changed by this event-source pass |

## Event Source Repair

Updated team event fetch sources:

| Source | Endpoint | Result |
| --- | --- | --- |
| `lg_twins_events` | `https://www.lgtwins.com/twins/feed/events?page={page}` | 14 events in 30-day crawl, 44 in 90-day crawl |
| `doosan_bears_events` | `https://www.doosanbears.com/doosan/v1/web/doorun/events?page={page0}&size=8` | 3 events in 30-day crawl, 14 in 90-day crawl |
| `ssg_landers_events` | `https://www.ssglanders.com/media/news?page={page}` | 5 events in 30-day crawl, 12 in 90-day crawl |
| `nc_dinos_events` | `https://www.ncdinos.com/dinos/news.do?newsType=event&pageNo={page}` | 3 events in both 30-day and 90-day crawls |
| `kiwoom_heroes_events` | `https://www.heroesbaseball.co.kr/story/heroesNews/list.do?page={page}` | 7 unique events in both 30-day and 90-day crawls |

Doosan requires a source-scoped TLS verify fallback because its public API currently fails Python certificate verification with an incomplete chain. The fallback is not applied to other hosts.

## Verification Results

Commands run:

```bash
venv/bin/python -m ruff check src/crawlers/team_event_crawler.py src/parsers/team_event_parser.py src/repositories/team_event_repository.py tests/test_team_event_parser.py tests/test_team_event_repository.py
venv/bin/python -m pytest tests/test_team_event_parser.py tests/test_integration_team_event_parser.py tests/test_team_event_repository.py
venv/bin/python -m pytest tests/test_scheduler_alerting.py -k health_and_freshness
venv/bin/python -m pytest tests/test_crawl_futures_stability.py tests/test_game_detail_crawler_stability.py
venv/bin/python -m pytest tests/test_phase0_source_registry_repository.py tests/test_source_registry_success_tracking.py
venv/bin/python -m src.cli.crawl_team_events --save --days 30
venv/bin/python -m src.cli.crawl_team_events --save --days 90
venv/bin/python -m src.cli.monitor_data_freshness --no-alert
venv/bin/python -m src.cli.health_check
```

Observed live crawl results:

| Command | Result |
| --- | --- |
| `crawl_team_events --save --days 30` | 32 events after per-run dedupe; 3 snapshots saved/updated |
| `crawl_team_events --save --days 90` | 89 events; 4 snapshots saved/updated |
| `team_events` table | 103 rows after verification |
| Event source registry | All seeded team event sources except `kbo_official_events` have non-null `last_success_at` |

`monitor_data_freshness --no-alert` no longer reports repaired team event sources as `never crawled`. Remaining issues are policy/backlog items below plus actual table/P0 issues.

## Remaining Never-Crawled Sources

| Category | Sources | Policy |
| --- | --- | --- |
| Seeded, no current crawler mapping | `kbo_official_events`, `kbo_player_movement`, `jamsil_parking_official`, `gujangfood_com` | Keep active for now; implement or explicitly mark optional after product decision |
| Existing crawler path, needs rerun or URL/selector repair | `doosan_bears_ticket`, `lg_twins_ticket`, `nc_dinos_ticket`, `ssg_landers_ticket`, `lg_twins_seat`, `seoul_stadium_seat`, `nc_dinos_food_seat` | Treat as next repair candidates, not source-registry bugs |
| Existing seasonal crawler, not run in this verification | `kbo_team_info`, `kbo_team_history` | Seasonal backlog; run/repair before changing active policy |
| Third-party optional/news | `mlbpark_bullpen`, `naver_sports_news`, `namuwiki_kbo` | Optional or freshness-policy candidates; avoid P0 alert severity unless enabled intentionally |

## Current Monitor Residuals

As of this verification, `monitor_data_freshness --no-alert` reports:

- 16 `never crawled` sources, all listed above.
- Empty tables: `stadium_transit_times`, `stadium_congestion`.
- One P0 game detail issue: `20260607HHLT0 missing_boxscore_detail`.

`health_check` reports the same 16 never-crawled sources and table issues for `stadium_transit_times`, `stadium_congestion`, and `cheer_songs`.
