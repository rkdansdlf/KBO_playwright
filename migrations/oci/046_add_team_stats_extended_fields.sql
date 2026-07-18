-- 046_add_team_stats_extended_fields.sql (OCI / PostgreSQL)
-- Add missing raw-count and derived-ratio columns to team_season_batting and team_season_pitching.
-- These fields are already computed by TeamStatAggregator from player-level data
-- but were silently stripped by _filter_model_fields because they had no matching column.

-- TeamSeasonBatting extended fields
ALTER TABLE team_season_batting ADD COLUMN IF NOT EXISTS intentional_walks INTEGER;
ALTER TABLE team_season_batting ADD COLUMN IF NOT EXISTS hbp INTEGER;
ALTER TABLE team_season_batting ADD COLUMN IF NOT EXISTS sacrifice_hits INTEGER;
ALTER TABLE team_season_batting ADD COLUMN IF NOT EXISTS sacrifice_flies INTEGER;
ALTER TABLE team_season_batting ADD COLUMN IF NOT EXISTS gdp INTEGER;
ALTER TABLE team_season_batting ADD COLUMN IF NOT EXISTS iso DOUBLE PRECISION;
ALTER TABLE team_season_batting ADD COLUMN IF NOT EXISTS babip DOUBLE PRECISION;

-- TeamSeasonPitching extended fields
ALTER TABLE team_season_pitching ADD COLUMN IF NOT EXISTS innings_outs INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN IF NOT EXISTS intentional_walks INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN IF NOT EXISTS hit_batters INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN IF NOT EXISTS tbf INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN IF NOT EXISTS complete_games INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN IF NOT EXISTS shutouts INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN IF NOT EXISTS wild_pitches INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN IF NOT EXISTS balks INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN IF NOT EXISTS sacrifices_allowed INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN IF NOT EXISTS sacrifice_flies_allowed INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN IF NOT EXISTS k_per_nine DOUBLE PRECISION;
ALTER TABLE team_season_pitching ADD COLUMN IF NOT EXISTS bb_per_nine DOUBLE PRECISION;
ALTER TABLE team_season_pitching ADD COLUMN IF NOT EXISTS kbb DOUBLE PRECISION;
ALTER TABLE team_season_pitching ADD COLUMN IF NOT EXISTS fip DOUBLE PRECISION;
