-- SQLite performance indexes for FK lookup columns.
-- FK constraints are enforced by application quality gates; indexes
-- speed up JOINs and avoid full table scans during sync/heal operations.

CREATE INDEX IF NOT EXISTS idx_game_metadata_game_id
    ON game_metadata (game_id);
CREATE INDEX IF NOT EXISTS idx_game_batting_stats_game_id
    ON game_batting_stats (game_id);
CREATE INDEX IF NOT EXISTS idx_game_batting_stats_player_id
    ON game_batting_stats (player_id)
    WHERE player_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_game_pitching_stats_game_id
    ON game_pitching_stats (game_id);
CREATE INDEX IF NOT EXISTS idx_game_pitching_stats_player_id
    ON game_pitching_stats (player_id)
    WHERE player_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_game_lineups_game_id
    ON game_lineups (game_id);
CREATE INDEX IF NOT EXISTS idx_game_lineups_player_id
    ON game_lineups (player_id)
    WHERE player_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_player_season_batting_player_id
    ON player_season_batting (player_id);
CREATE INDEX IF NOT EXISTS idx_player_season_batting_team_code
    ON player_season_batting (team_code)
    WHERE team_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_player_season_pitching_player_id
    ON player_season_pitching (player_id);
CREATE INDEX IF NOT EXISTS idx_player_season_pitching_team_code
    ON player_season_pitching (team_code)
    WHERE team_code IS NOT NULL;
