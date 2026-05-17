-- Draft integrity hardening after existing duplicate game-player rows are cleaned.
-- This migration intentionally fails before creating indexes if current data still
-- contains non-null duplicate player rows per game.

PRAGMA foreign_keys = ON;
BEGIN IMMEDIATE;

DROP TABLE IF EXISTS temp._game_stat_unique_preflight;
CREATE TEMP TABLE _game_stat_unique_preflight (
    ok INTEGER NOT NULL CHECK (ok = 1)
);

INSERT INTO _game_stat_unique_preflight(ok)
SELECT CASE WHEN EXISTS (
    SELECT 1
    FROM game_batting_stats
    WHERE player_id IS NOT NULL
    GROUP BY game_id, player_id
    HAVING COUNT(*) > 1
) THEN 0 ELSE 1 END;

INSERT INTO _game_stat_unique_preflight(ok)
SELECT CASE WHEN EXISTS (
    SELECT 1
    FROM game_pitching_stats
    WHERE player_id IS NOT NULL
    GROUP BY game_id, player_id
    HAVING COUNT(*) > 1
) THEN 0 ELSE 1 END;

INSERT INTO _game_stat_unique_preflight(ok)
SELECT CASE WHEN EXISTS (
    SELECT 1
    FROM game_lineups
    WHERE player_id IS NOT NULL
    GROUP BY game_id, player_id
    HAVING COUNT(*) > 1
) THEN 0 ELSE 1 END;

CREATE UNIQUE INDEX IF NOT EXISTS uq_game_batting_stats_game_player_nonnull
    ON game_batting_stats (game_id, player_id)
    WHERE player_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_game_pitching_stats_game_player_nonnull
    ON game_pitching_stats (game_id, player_id)
    WHERE player_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_game_lineups_game_player_nonnull
    ON game_lineups (game_id, player_id)
    WHERE player_id IS NOT NULL;

DROP TABLE IF EXISTS temp._game_stat_unique_preflight;

COMMIT;
