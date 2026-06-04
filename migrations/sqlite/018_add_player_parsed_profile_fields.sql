-- migrations/sqlite/018_add_player_parsed_profile_fields.sql
-- SQLite table modifications for parsed player profiles.
-- Column additions are handled safely by the Python migration runner to avoid crash in SQLite.

-- Create indexes on draft and structured columns if needed
CREATE INDEX IF NOT EXISTS idx_player_basic_draft_year ON player_basic(draft_year);
CREATE INDEX IF NOT EXISTS idx_players_draft_year      ON players(draft_year);
