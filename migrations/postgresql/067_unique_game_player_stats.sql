-- 067_unique_game_player_stats.sql
-- Ensure (game_id, player_id) unique constraint on player game stats tables
-- for idempotent UPSERT and deduplication, cleaning up any pre-existing duplicates first.

-- 1. game_batting_stats 기존 중복 데이터 선제 정리 (최신 id 1건만 유지)
DELETE FROM game_batting_stats a
USING game_batting_stats b
WHERE a.game_id = b.game_id
  AND a.player_id = b.player_id
  AND a.player_id IS NOT NULL
  AND a.id < b.id;

ALTER TABLE game_batting_stats
    DROP CONSTRAINT IF EXISTS uq_game_batting_player,
    ADD CONSTRAINT uq_game_batting_player UNIQUE (game_id, player_id);

-- 2. game_pitching_stats 기존 중복 데이터 선제 정리 (최신 id 1건만 유지)
DELETE FROM game_pitching_stats a
USING game_pitching_stats b
WHERE a.game_id = b.game_id
  AND a.player_id = b.player_id
  AND a.player_id IS NOT NULL
  AND a.id < b.id;

ALTER TABLE game_pitching_stats
    DROP CONSTRAINT IF EXISTS uq_game_pitching_player,
    ADD CONSTRAINT uq_game_pitching_player UNIQUE (game_id, player_id);
