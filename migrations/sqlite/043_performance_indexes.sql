-- SQLite migration to add missing indexes for query optimization.

CREATE INDEX IF NOT EXISTS idx_game_game_date
    ON game (game_date);

CREATE INDEX IF NOT EXISTS idx_game_season_id
    ON game (season_id);

CREATE INDEX IF NOT EXISTS idx_game_events_batter_id
    ON game_events (batter_id)
    WHERE batter_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_game_events_pitcher_id
    ON game_events (pitcher_id)
    WHERE pitcher_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_game_summary_game_id
    ON game_summary (game_id);

CREATE INDEX IF NOT EXISTS idx_game_summary_player_id
    ON game_summary (player_id)
    WHERE player_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_game_play_by_play_player_id
    ON game_play_by_play (player_id)
    WHERE player_id IS NOT NULL;
