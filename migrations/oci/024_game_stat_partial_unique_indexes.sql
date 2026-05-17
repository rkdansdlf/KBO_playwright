-- Draft integrity hardening after existing duplicate game-player rows are cleaned.
-- This migration intentionally raises before creating indexes if current data
-- still contains non-null duplicate player rows per game.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM game_batting_stats
        WHERE player_id IS NOT NULL
        GROUP BY game_id, player_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'game_batting_stats contains duplicate non-null (game_id, player_id) rows';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM game_pitching_stats
        WHERE player_id IS NOT NULL
        GROUP BY game_id, player_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'game_pitching_stats contains duplicate non-null (game_id, player_id) rows';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM game_lineups
        WHERE player_id IS NOT NULL
        GROUP BY game_id, player_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'game_lineups contains duplicate non-null (game_id, player_id) rows';
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_game_batting_stats_game_player_nonnull
    ON game_batting_stats (game_id, player_id)
    WHERE player_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_game_pitching_stats_game_player_nonnull
    ON game_pitching_stats (game_id, player_id)
    WHERE player_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_game_lineups_game_player_nonnull
    ON game_lineups (game_id, player_id)
    WHERE player_id IS NOT NULL;
