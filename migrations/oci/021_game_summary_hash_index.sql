-- game_summary table unique constraint redesign to handle large detail_text.
-- B-Tree indexes have a limit of ~2704 bytes. detail_text can exceed this.
-- We use an MD5 hash of detail_text in the unique index instead.

-- 1. Drop old constraints/indexes that might include detail_text
ALTER TABLE IF EXISTS game_summary DROP CONSTRAINT IF EXISTS uq_game_summary;
DROP INDEX IF EXISTS uq_game_summary;
ALTER TABLE IF EXISTS game_summary DROP CONSTRAINT IF EXISTS uq_game_summary_entry;
DROP INDEX IF EXISTS uq_game_summary_entry;

-- 2. Cleanup existing duplicates before creating unique index
DELETE FROM game_summary a
USING game_summary b
WHERE a.id > b.id
  AND a.game_id = b.game_id
  AND a.summary_type = b.summary_type
  AND COALESCE(a.player_id, 0) = COALESCE(b.player_id, 0)
  AND COALESCE(a.player_name, '') = COALESCE(b.player_name, '')
  AND md5(COALESCE(a.detail_text, '')) = md5(COALESCE(b.detail_text, ''));

-- 3. Create a unique index using MD5 hash of detail_text.
-- We include player_id and player_name to distinguish between different players' records.
-- COALESCE is used to ensure NULL values don't bypass uniqueness where not intended.
CREATE UNIQUE INDEX IF NOT EXISTS uq_game_summary_hash
    ON game_summary (
        game_id, 
        summary_type, 
        COALESCE(player_id, 0), 
        COALESCE(player_name, ''), 
        md5(COALESCE(detail_text, ''))
    );

-- 3. Create a non-unique lookup index for common queries (without hash)
CREATE INDEX IF NOT EXISTS idx_game_summary_lookup
    ON game_summary (game_id, summary_type, player_name);
