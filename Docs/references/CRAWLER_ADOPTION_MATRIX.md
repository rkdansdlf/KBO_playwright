| crawler | transport | empty | unit | fallback | snapshot | ledger | DLQ | replay |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `award_crawler` | crawler_http_client | typed | source | none | Y | Y | Y | Y |
| `roster_transaction_crawler` | crawler_http_client+playwright | typed_confirmed | date | browser | Y | Y | Y | Y |
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
| `schedule_crawler` | raw_httpx+playwright | collapsed | season | browser | - | - | - | - |
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

**Fully adopted (2)**: `award_crawler`, `roster_transaction_crawler`

**Migration order** (declared priority first, then fewest satisfied axes):
1. `schedule_crawler`
2. `game_detail_crawler`
3. `relay_crawler`
4. `food_crawler`
5. `parking_crawler`
6. `kbo_event_crawler`
7. `player_movement_crawler`
8. `team_history_crawler`
9. `baserunning_stats_crawler`
10. `broadcast_crawler`
11. `fan_culture_crawler`
12. `fielding_stats_crawler`
13. `foreign_player_crawler`
14. `game_mvp_crawler`
15. `injury_crawler`
16. `manager_change_crawler`
17. `operation_notice_naver_crawler`
18. `player_batting_all_series_crawler`
19. `player_pitching_all_series_crawler`
20. `seat_crawler`
21. `static_text_crawler`
22. `team_batting_stats_crawler`
23. `team_event_crawler`
24. `team_info_crawler`
25. `team_pitching_stats_crawler`
26. `ticket_crawler`
27. `base_naver_crawler`
28. `daily_roster_crawler`
29. `draft_history_crawler`
30. `external_stats_crawler`
31. `futures_schedule_crawler`
32. `milestone_crawler`
33. `naver_relay_crawler`
34. `operation_notice_doosan_crawler`
35. `operation_notice_lg_crawler`
36. `player_splits_crawler`
37. `press_release_crawler`
38. `preview_crawler`
39. `text_relay_crawler`
40. `pbp_crawler`
41. `player_profile_crawler`
42. `player_search_crawler`
43. `staff_register_crawler`

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
