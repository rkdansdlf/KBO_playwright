-- Remove legacy phase-one unique constraints that still collapse team splits.
ALTER TABLE IF EXISTS player_season_batting
    DROP CONSTRAINT IF EXISTS uq_player_season_batting_new;

ALTER TABLE IF EXISTS player_season_pitching
    DROP CONSTRAINT IF EXISTS uq_player_season_pitching_idx;
