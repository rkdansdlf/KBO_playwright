-- 054_add_award_player_id.sql
-- Link award winners to the player registry (SQLite).
-- Apply once; idempotent for databases created from the post-link schema.

ALTER TABLE awards ADD COLUMN player_id INTEGER;
ALTER TABLE awards ADD COLUMN team_code VARCHAR(20);
CREATE INDEX idx_award_player_id ON awards(player_id);
