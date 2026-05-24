# Database Schema Summary

| Table Name | Column Count | Primary Key |
| --- | --- | --- |
| awards | 8 | id |
| crawl_runs | 10 | id |
| fa_contracts | 17 | id |
| game | 15 | id |
| game_batting_stats | 37 | id |
| game_events | 26 | id |
| game_inning_scores | 9 | id |
| game_lineups | 14 | id |
| game_metadata | 11 | game_id |
| game_pitching_stats | 35 | id |
| game_play_by_play | 11 | id |
| game_summary | 8 | id |
| kbo_seasons | 8 | season_id |
| player_basic | 13 | player_id |
| player_identities | 10 | id |
| player_movements | 11 | id |
| player_season_batting | 34 | id |
| player_season_pitching | 45 | id |
| players | 16 | id |
| stat_rankings | 14 | id |
| team_daily_roster | 11 | id |
| team_franchises | 8 | id |
| team_history | 12 | id |
| team_season_batting | 25 | id |
| team_season_pitching | 24 | id |
| teams | 11 | team_id |

## Integrity Notes

- `player_basic(player_id)` is the canonical player FK target. `players.player_basic_id` is a nullable compatibility mirror and should not become the parent key for new fact tables.
- `team_daily_roster.player_id`, `player_name`, `position`, and `player_movements.team_code`, `player_name` are source snapshots. Canonical reads should prefer `team_daily_roster.player_basic_id`, `team_daily_roster.person_type`, `player_movements.canonical_team_id`, and `player_movements.player_basic_id`.
- `player_movements.team_code` intentionally remains raw snapshot data. OCI migration `024_deletion_anomaly_integrity.sql` drops the old update trigger that rewrote this field; normalized team joins use `canonical_team_id`.
- `player_movements.resolution_status='unresolved_player'` is allowed for ambiguous historical movement rows. Migration `025_player_movement_position_backfill.sql` resolves only rows that are unique after adding the source snapshot position.
- Game child tables use `ON DELETE CASCADE` from `game`; player and team references use `ON DELETE RESTRICT`.

*Generated from database schema.*
