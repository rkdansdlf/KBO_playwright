-- migrations/sqlite/016_player_basic_birth_date_index.sql
-- Create indexes on player_basic table if they do not exist.
-- The birth_date_date column addition is handled by the Python runner to prevent syntax crashes on older SQLite versions.

CREATE INDEX IF NOT EXISTS idx_player_basic_name     ON player_basic(name);
CREATE INDEX IF NOT EXISTS idx_player_basic_team     ON player_basic(team);
CREATE INDEX IF NOT EXISTS idx_player_basic_position ON player_basic(position);
CREATE INDEX IF NOT EXISTS idx_player_basic_team_pos ON player_basic(team, position);
