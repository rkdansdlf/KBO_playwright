| crawler | transport | empty | unit | fallback | snapshot | ledger | DLQ | replay |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `award_crawler` | crawler_http_client | typed | source | none | Y | Y | Y | Y |
| `food_crawler` | crawler_http_client | typed | team | none | Y | Y | Y | Y |
| `game_detail_crawler` | crawler_http_client+playwright | collapsed | game | none | - | Y | Y | Y |
| `kbo_event_crawler` | playwright | typed | document | none | Y | Y | Y | Y |
| `parking_crawler` | crawler_http_client | typed | team | none | Y | Y | Y | Y |
| `player_movement_crawler` | playwright | typed | season | none | Y | Y | Y | Y |
| `relay_crawler` | crawler_http_client+playwright | typed | game | none | - | Y | Y | Y |
| `roster_transaction_crawler` | crawler_http_client+playwright | typed_confirmed | date | browser | Y | Y | Y | Y |
| `schedule_crawler` | crawler_http_client+playwright | typed_confirmed | month | browser | - | Y | Y | Y |
| `team_history_crawler` | playwright | typed | season | none | Y | Y | Y | Y |
| `base_naver_crawler` | raw_httpx | - | - | - | - | - | - | - |
| `baserunning_stats_crawler` | playwright | - | - | - | - | - | - | - |
| `broadcast_crawler` | playwright | - | - | - | - | - | - | - |
| `congestion_crawler` | - | - | - | - | - | - | - | - |
| `daily_roster_crawler` | playwright | - | - | - | - | - | - | - |
| `draft_history_crawler` | playwright | - | - | - | - | - | - | - |
| `dynamic_data_crawler` | - | - | - | - | - | - | - | - |
| `external_stats_crawler` | raw_httpx | - | - | - | - | - | - | - |
| `fan_culture_crawler` | api_client | - | - | - | - | - | - | - |
| `fielding_stats_crawler` | playwright | - | - | - | - | - | - | - |
| `foreign_player_crawler` | playwright | - | - | - | - | - | - | - |
| `futures_schedule_crawler` | playwright | - | - | - | - | - | - | - |
| `game_mvp_crawler` | raw_httpx | - | - | - | - | - | - | - |
| `historical_season_crawler` | - | - | - | - | - | - | - | - |
| `injury_crawler` | playwright | - | - | - | - | - | - | - |
| `legacy_game_detail_crawler` | - | - | - | - | - | - | - | - |
| `manager_change_crawler` | playwright | - | - | - | - | - | - | - |
| `milestone_crawler` | playwright | - | - | - | - | - | - | - |
| `naver_relay_crawler` | playwright | - | - | - | - | - | - | - |
| `operation_notice_doosan_crawler` | playwright | - | - | - | - | - | - | - |
| `operation_notice_lg_crawler` | raw_httpx | - | - | - | - | - | - | - |
| `operation_notice_naver_crawler` | api_client | - | - | - | - | - | - | - |
| `pbp_crawler` | playwright | - | - | - | - | - | - | - |
| `player_batting_all_series_crawler` | playwright | - | - | - | - | - | - | - |
| `player_list_crawler` | - | - | - | - | - | - | - | - |
| `player_pitching_all_series_crawler` | playwright | - | - | - | - | - | - | - |
| `player_profile_crawler` | playwright | - | - | - | - | - | - | - |
| `player_search_crawler` | playwright | - | - | - | - | - | - | - |
| `player_splits_crawler` | playwright | - | - | - | - | - | - | - |
| `press_release_crawler` | playwright | - | - | - | - | - | - | - |
| `preview_crawler` | raw_httpx+playwright | - | - | - | - | - | - | - |
| `realtime_issue_crawler` | - | - | - | - | Y | - | - | - |
| `seat_crawler` | raw_httpx | - | - | - | Y | - | - | - |
| `staff_register_crawler` | playwright | - | - | - | - | - | - | - |
| `static_text_crawler` | playwright | - | - | - | Y | - | - | - |
| `team_batting_stats_crawler` | playwright | - | - | - | - | - | - | - |
| `team_event_crawler` | raw_httpx | - | - | - | Y | - | - | - |
| `team_info_crawler` | playwright | - | - | - | Y | - | - | - |
| `team_pitching_stats_crawler` | playwright | - | - | - | - | - | - | - |
| `text_relay_crawler` | playwright | - | - | - | - | - | - | - |
| `ticket_crawler` | raw_httpx | collapsed | team | alternate_source | Y | - | - | - |
| `transit_time_crawler` | - | - | - | - | - | - | - | - |

**Fully adopted (10)**: `award_crawler`, `food_crawler`, `game_detail_crawler`, `kbo_event_crawler`, `parking_crawler`, `player_movement_crawler`, `relay_crawler`, `roster_transaction_crawler`, `schedule_crawler`, `team_history_crawler`

**Migration order** (fewest satisfied axes first; the gap is named, not implied):
1. `baserunning_stats_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
2. `broadcast_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
3. `fielding_stats_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
4. `foreign_player_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
5. `injury_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
6. `manager_change_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
7. `player_batting_all_series_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
8. `player_pitching_all_series_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
9. `static_text_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
10. `team_batting_stats_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
11. `team_info_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
12. `team_pitching_stats_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
13. `daily_roster_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
14. `draft_history_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
15. `fan_culture_crawler` -- transport: no governed request path, typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
16. `futures_schedule_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
17. `game_mvp_crawler` -- transport: no governed request path, typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
18. `milestone_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
19. `naver_relay_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
20. `operation_notice_doosan_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
21. `operation_notice_naver_crawler` -- transport: no governed request path, typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
22. `player_splits_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
23. `press_release_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
24. `seat_crawler` -- transport: no governed request path, typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
25. `team_event_crawler` -- transport: no governed request path, typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
26. `ticket_crawler` -- transport: no governed request path, typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
27. `base_naver_crawler` -- transport: no governed request path, typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
28. `operation_notice_lg_crawler` -- transport: no governed request path, typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
29. `pbp_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
30. `player_profile_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
31. `player_search_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
32. `preview_crawler` -- transport: no governed request path, typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
33. `staff_register_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
34. `text_relay_crawler` -- typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler
35. `external_stats_crawler` -- transport: no governed request path, typed outcome: no CrawlResult/CrawlOutcome import, run ledger, dead letter queue, replay handler

**Advisories**
- congestion_crawler: has a crawl entrypoint but no transport was detected; check the classifier
- dynamic_data_crawler: has a crawl entrypoint but no transport was detected; check the classifier
- historical_season_crawler: has a crawl entrypoint but no transport was detected; check the classifier
- player_list_crawler: has a crawl entrypoint but no transport was detected; check the classifier
- realtime_issue_crawler: has a crawl entrypoint but no transport was detected; check the classifier
- transit_time_crawler: has a crawl entrypoint but no transport was detected; check the classifier
