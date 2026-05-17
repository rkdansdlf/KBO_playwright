-- Increase column length for game_lineups.notes
-- This is necessary to accommodate longer result history strings or JSON-like notes.

ALTER TABLE game_lineups 
  ALTER COLUMN notes TYPE VARCHAR(512);
