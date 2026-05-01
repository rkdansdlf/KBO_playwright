-- game_summary.detail_text can contain large Coach review JSON.
-- Keeping it inside a btree unique index can exceed Postgres index row limits.

ALTER TABLE IF EXISTS game_summary DROP CONSTRAINT IF EXISTS uq_game_summary;
DROP INDEX IF EXISTS uq_game_summary;

CREATE INDEX IF NOT EXISTS idx_game_summary_lookup
    ON game_summary (game_id, summary_type, player_name);
