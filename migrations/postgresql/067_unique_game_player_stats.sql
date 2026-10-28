-- 067_unique_game_player_stats.sql
-- Ensure (game_id, player_id) unique constraint on player game stats tables
-- for idempotent UPSERT and deduplication.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_game_batting_player'
    ) THEN
        ALTER TABLE game_batting_stats DROP CONSTRAINT uq_game_batting_player;
    END IF;
    ALTER TABLE game_batting_stats ADD CONSTRAINT uq_game_batting_player UNIQUE (game_id, player_id);
EXCEPTION
    WHEN duplicate_table OR duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_game_pitching_player'
    ) THEN
        ALTER TABLE game_pitching_stats DROP CONSTRAINT uq_game_pitching_player;
    END IF;
    ALTER TABLE game_pitching_stats ADD CONSTRAINT uq_game_pitching_player UNIQUE (game_id, player_id);
EXCEPTION
    WHEN duplicate_table OR duplicate_object THEN NULL;
END $$;
