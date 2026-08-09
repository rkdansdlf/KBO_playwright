-- Link award winners to the player registry.
-- Idempotent: safe when SQLAlchemy already created the model columns.

ALTER TABLE awards ADD COLUMN IF NOT EXISTS player_id INTEGER;
ALTER TABLE awards ADD COLUMN IF NOT EXISTS team_code VARCHAR(20);
CREATE INDEX IF NOT EXISTS idx_award_player_id ON awards(player_id);
