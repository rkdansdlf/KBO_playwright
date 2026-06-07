-- OCI PostgreSQL migration to add missing performance indexes.

-- Index for game_events game_id lookup
CREATE INDEX IF NOT EXISTS idx_game_events_game_id
    ON game_events (game_id);

-- Index for game_inning_scores game_id lookup
CREATE INDEX IF NOT EXISTS idx_game_inning_scores_game_id
    ON game_inning_scores (game_id);

-- Index for player_season_batting season lookup (used by stats recalculations/audits)
CREATE INDEX IF NOT EXISTS idx_player_season_batting_season
    ON player_season_batting (season);

-- Index for player_season_pitching season lookup (used by stats recalculations/audits)
CREATE INDEX IF NOT EXISTS idx_player_season_pitching_season
    ON player_season_pitching (season);
