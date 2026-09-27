| crawler | transport | empty | unit | fallback | snapshot | ledger | DLQ | replay |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `award_crawler` | crawler_http_client | typed | source | none | Y | Y | Y | Y |
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
| `food_crawler` | crawler_http_client+raw_httpx | - | - | - | Y | - | - | - |
| `foreign_player_crawler` | playwright | - | - | - | - | - | - | - |
| `futures_schedule_crawler` | playwright | - | - | - | - | - | - | - |
| `game_detail_crawler` | raw_httpx+playwright | collapsed | game | none | - | - | - | - |
| `game_mvp_crawler` | raw_httpx | - | - | - | - | - | - | - |
| `historical_season_crawler` | - | - | - | - | - | - | - | - |
| `injury_crawler` | playwright | - | - | - | - | - | - | - |
| `kbo_event_crawler` | playwright | - | - | - | Y | - | - | - |
| `legacy_game_detail_crawler` | - | - | - | - | - | - | - | - |
| `manager_change_crawler` | playwright | - | - | - | - | - | - | - |
| `milestone_crawler` | playwright | - | - | - | - | - | - | - |
| `naver_relay_crawler` | playwright | - | - | - | - | - | - | - |
| `operation_notice_doosan_crawler` | playwright | - | - | - | - | - | - | - |
| `operation_notice_lg_crawler` | raw_httpx | - | - | - | - | - | - | - |
| `operation_notice_naver_crawler` | api_client | - | - | - | - | - | - | - |
| `parking_crawler` | crawler_http_client+raw_httpx | - | - | - | Y | - | - | - |
| `pbp_crawler` | playwright | - | - | - | - | - | - | - |
| `player_batting_all_series_crawler` | playwright | - | - | - | - | - | - | - |
| `player_list_crawler` | - | - | - | - | - | - | - | - |
| `player_movement_crawler` | playwright | - | - | - | Y | - | - | - |
| `player_pitching_all_series_crawler` | playwright | - | - | - | - | - | - | - |
| `player_profile_crawler` | playwright | - | - | - | - | - | - | - |
| `player_search_crawler` | playwright | - | - | - | - | - | - | - |
| `player_splits_crawler` | playwright | - | - | - | - | - | - | - |
| `press_release_crawler` | playwright | - | - | - | - | - | - | - |
| `preview_crawler` | raw_httpx+playwright | - | - | - | - | - | - | - |
| `realtime_issue_crawler` | - | - | - | - | Y | - | - | - |
| `relay_crawler` | raw_httpx+playwright | collapsed | game | none | - | - | - | - |
| `seat_crawler` | raw_httpx | - | - | - | Y | - | - | - |
| `staff_register_crawler` | playwright | - | - | - | - | - | - | - |
| `static_text_crawler` | playwright | - | - | - | Y | - | - | - |
| `team_batting_stats_crawler` | playwright | - | - | - | - | - | - | - |
| `team_event_crawler` | raw_httpx | - | - | - | Y | - | - | - |
| `team_history_crawler` | playwright | - | - | - | Y | - | - | - |
| `team_info_crawler` | playwright | - | - | - | Y | - | - | - |
| `team_pitching_stats_crawler` | playwright | - | - | - | - | - | - | - |
| `text_relay_crawler` | playwright | - | - | - | - | - | - | - |
| `ticket_crawler` | raw_httpx | collapsed | team | alternate_source | Y | - | - | - |
| `transit_time_crawler` | - | - | - | - | - | - | - | - |

**Fully adopted (3)**: `award_crawler`, `roster_transaction_crawler`, `schedule_crawler`

**Migration order** (fewest satisfied axes first):
1. `game_detail_crawler`
2. `relay_crawler`
3. `food_crawler`
4. `parking_crawler`
5. `kbo_event_crawler`
6. `player_movement_crawler`
7. `team_history_crawler`
8. `baserunning_stats_crawler`
9. `broadcast_crawler`
10. `fan_culture_crawler`
11. `fielding_stats_crawler`
12. `foreign_player_crawler`
13. `game_mvp_crawler`
14. `injury_crawler`
15. `manager_change_crawler`
16. `operation_notice_naver_crawler`
17. `player_batting_all_series_crawler`
18. `player_pitching_all_series_crawler`
19. `seat_crawler`
20. `static_text_crawler`
21. `team_batting_stats_crawler`
22. `team_event_crawler`
23. `team_info_crawler`
24. `team_pitching_stats_crawler`
25. `ticket_crawler`
26. `base_naver_crawler`
27. `daily_roster_crawler`
28. `draft_history_crawler`
29. `external_stats_crawler`
30. `futures_schedule_crawler`
31. `milestone_crawler`
32. `naver_relay_crawler`
33. `operation_notice_doosan_crawler`
34. `operation_notice_lg_crawler`
35. `player_splits_crawler`
36. `press_release_crawler`
37. `preview_crawler`
38. `text_relay_crawler`
39. `pbp_crawler`
40. `player_profile_crawler`
41. `player_search_crawler`
42. `staff_register_crawler`

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
