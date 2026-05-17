-- Increase column lengths for player_season_pitching to match player_season_batting
-- This is necessary to accommodate 'FINAL_VERIFICATION' source and other potentially long strings.

ALTER TABLE player_season_pitching 
  ALTER COLUMN league TYPE VARCHAR(50),
  ALTER COLUMN level TYPE VARCHAR(50),
  ALTER COLUMN source TYPE VARCHAR(50);
