| crawler | transport | empty | unit | fallback | snapshot | ledger | DLQ | replay |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `award_crawler` | crawler_http_client | typed | source | none | Y | Y | Y | Y |
| `food_crawler` | crawler_http_client | typed | team | none | Y | Y | Y | Y |
| `game_detail_crawler` | crawler_http_client+playwright | collapsed | game | none | - | Y | Y | Y |
| `parking_crawler` | crawler_http_client | typed | team | none | Y | Y | Y | Y |
| `relay_crawler` | crawler_http_client+playwright | typed | game | none | - | Y | Y | Y |
| `roster_transaction_crawler` | crawler_http_client+playwright | typed_confirmed | date | browser | Y | Y | Y | Y |
| `schedule_crawler` | crawler_http_client+playwright | typed_confirmed | month | browser | - | Y | Y | Y |
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
| `kbo_event_crawler` | playwright | - | - | - | Y | Y | Y | Y |
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
| `player_movement_crawler` | playwright | - | - | - | Y | Y | Y | Y |
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
| `team_history_crawler` | playwright | - | - | - | Y | Y | Y | Y |
| `team_info_crawler` | playwright | - | - | - | Y | - | - | - |
| `team_pitching_stats_crawler` | playwright | - | - | - | - | - | - | - |
| `text_relay_crawler` | playwright | - | - | - | - | - | - | - |
| `ticket_crawler` | raw_httpx | collapsed | team | alternate_source | Y | - | - | - |
| `transit_time_crawler` | - | - | - | - | - | - | - | - |

**Fully adopted (7)**: `award_crawler`, `food_crawler`, `game_detail_crawler`, `parking_crawler`, `relay_crawler`, `roster_transaction_crawler`, `schedule_crawler`

**Migration order** (fewest satisfied axes first):
1. `kbo_event_crawler`
2. `player_movement_crawler`
3. `team_history_crawler`
4. `baserunning_stats_crawler`
5. `broadcast_crawler`
6. `fan_culture_crawler`
7. `fielding_stats_crawler`
8. `foreign_player_crawler`
9. `game_mvp_crawler`
10. `injury_crawler`
11. `manager_change_crawler`
12. `operation_notice_naver_crawler`
13. `player_batting_all_series_crawler`
14. `player_pitching_all_series_crawler`
15. `seat_crawler`
16. `static_text_crawler`
17. `team_batting_stats_crawler`
18. `team_event_crawler`
19. `team_info_crawler`
20. `team_pitching_stats_crawler`
21. `ticket_crawler`
22. `base_naver_crawler`
23. `daily_roster_crawler`
24. `draft_history_crawler`
25. `futures_schedule_crawler`
26. `milestone_crawler`
27. `naver_relay_crawler`
28. `operation_notice_doosan_crawler`
29. `operation_notice_lg_crawler`
30. `player_splits_crawler`
31. `press_release_crawler`
32. `preview_crawler`
33. `external_stats_crawler`
34. `pbp_crawler`
35. `player_profile_crawler`
36. `player_search_crawler`
37. `staff_register_crawler`
38. `text_relay_crawler`

**Advisories**
- congestion_crawler: has a crawl entrypoint but no transport was detected; check the classifier
- dynamic_data_crawler: has a crawl entrypoint but no transport was detected; check the classifier
- historical_season_crawler: has a crawl entrypoint but no transport was detected; check the classifier
- player_list_crawler: has a crawl entrypoint but no transport was detected; check the classifier
- realtime_issue_crawler: has a crawl entrypoint but no transport was detected; check the classifier
- transit_time_crawler: has a crawl entrypoint but no transport was detected; check the classifier
