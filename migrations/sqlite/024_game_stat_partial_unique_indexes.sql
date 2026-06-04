-- Deduplicate and create partial unique indexes on game-player stats tables.
-- Duplicates arise from historical data-load anomalies; keep the earliest row.

DELETE FROM game_batting_stats WHERE id NOT IN (
    SELECT MIN(id) FROM game_batting_stats
    WHERE player_id IS NOT NULL
    GROUP BY game_id, player_id
);
DELETE FROM game_pitching_stats WHERE id NOT IN (
    SELECT MIN(id) FROM game_pitching_stats
    WHERE player_id IS NOT NULL
    GROUP BY game_id, player_id
);
DELETE FROM game_lineups WHERE id NOT IN (
    SELECT MIN(id) FROM game_lineups
    WHERE player_id IS NOT NULL
    GROUP BY game_id, player_id
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_game_batting_stats_game_player_nonnull
    ON game_batting_stats (game_id, player_id)
    WHERE player_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_game_pitching_stats_game_player_nonnull
    ON game_pitching_stats (game_id, player_id)
    WHERE player_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_game_lineups_game_player_nonnull
    ON game_lineups (game_id, player_id)
    WHERE player_id IS NOT NULL;
