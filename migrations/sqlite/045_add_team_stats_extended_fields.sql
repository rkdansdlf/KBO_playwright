-- 045_add_team_stats_extended_fields.sql
-- Add missing raw-count and derived-ratio columns to team_season_batting and team_season_pitching.
-- These fields are already computed by TeamStatAggregator from player-level data
-- but were silently stripped by _filter_model_fields because they had no matching column.

-- TeamSeasonBatting extended fields
ALTER TABLE team_season_batting ADD COLUMN intentional_walks INTEGER;
ALTER TABLE team_season_batting ADD COLUMN hbp INTEGER;
ALTER TABLE team_season_batting ADD COLUMN sacrifice_hits INTEGER;
ALTER TABLE team_season_batting ADD COLUMN sacrifice_flies INTEGER;
ALTER TABLE team_season_batting ADD COLUMN gdp INTEGER;
ALTER TABLE team_season_batting ADD COLUMN iso FLOAT;
ALTER TABLE team_season_batting ADD COLUMN babip FLOAT;

-- TeamSeasonPitching extended fields
ALTER TABLE team_season_pitching ADD COLUMN innings_outs INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN intentional_walks INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN hit_batters INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN tbf INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN complete_games INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN shutouts INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN wild_pitches INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN balks INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN sacrifices_allowed INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN sacrifice_flies_allowed INTEGER;
ALTER TABLE team_season_pitching ADD COLUMN k_per_nine FLOAT;
ALTER TABLE team_season_pitching ADD COLUMN bb_per_nine FLOAT;
ALTER TABLE team_season_pitching ADD COLUMN kbb FLOAT;
ALTER TABLE team_season_pitching ADD COLUMN fip FLOAT;
