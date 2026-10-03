| crawler | transport | empty | unit | fallback | snapshot | ledger | DLQ | replay |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `award_crawler` | crawler_http_client | typed | source | none | Y | Y | Y | Y |
| `game_detail_crawler` | crawler_http_client+playwright | collapsed | game | none | - | Y | Y | Y |
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
| `food_crawler` | crawler_http_client+raw_httpx | - | - | - | Y | Y | Y | Y |
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
| `parking_crawler` | crawler_http_client+raw_httpx | - | - | - | Y | Y | Y | Y |
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

**Fully adopted (5)**: `award_crawler`, `game_detail_crawler`, `relay_crawler`, `roster_transaction_crawler`, `schedule_crawler`

**Migration order** (fewest satisfied axes first):
1. `food_crawler`
2. `parking_crawler`
3. `kbo_event_crawler`
4. `player_movement_crawler`
5. `team_history_crawler`
6. `baserunning_stats_crawler`
7. `broadcast_crawler`
8. `fan_culture_crawler`
9. `fielding_stats_crawler`
10. `foreign_player_crawler`
11. `game_mvp_crawler`
12. `injury_crawler`
13. `manager_change_crawler`
14. `operation_notice_naver_crawler`
15. `player_batting_all_series_crawler`
16. `player_pitching_all_series_crawler`
17. `seat_crawler`
18. `static_text_crawler`
19. `team_batting_stats_crawler`
20. `team_event_crawler`
21. `team_info_crawler`
22. `team_pitching_stats_crawler`
23. `ticket_crawler`
24. `base_naver_crawler`
25. `daily_roster_crawler`
26. `draft_history_crawler`
27. `external_stats_crawler`
28. `futures_schedule_crawler`
29. `milestone_crawler`
30. `naver_relay_crawler`
31. `operation_notice_doosan_crawler`
32. `operation_notice_lg_crawler`
33. `player_splits_crawler`
34. `press_release_crawler`
35. `preview_crawler`
36. `text_relay_crawler`
37. `pbp_crawler`
38. `player_profile_crawler`
39. `player_search_crawler`
40. `staff_register_crawler`

**Advisories**
- congestion_crawler: has a crawl entrypoint but no transport was detected; check the classifier
- dynamic_data_crawler: has a crawl entrypoint but no transport was detected; check the classifier
- external_stats_crawler: classifies outcomes but records no run, so failures leave no trace
- food_crawler: uses CrawlerHttpClient but inherits a raw httpx path from BaseHttpCrawler; the second path is unused
- historical_season_crawler: has a crawl entrypoint but no transport was detected; check the classifier
- parking_crawler: uses CrawlerHttpClient but inherits a raw httpx path from BaseHttpCrawler; the second path is unused
- player_list_crawler: has a crawl entrypoint but no transport was detected; check the classifier
- realtime_issue_crawler: has a crawl entrypoint but no transport was detected; check the classifier
- text_relay_crawler: classifies outcomes but records no run, so failures leave no trace
- transit_time_crawler: has a crawl entrypoint but no transport was detected; check the classifier
