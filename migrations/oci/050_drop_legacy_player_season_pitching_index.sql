-- Remove the legacy unique index that still collapses team-split pitching rows.
DROP INDEX IF EXISTS uq_player_season_pitching_idx;
