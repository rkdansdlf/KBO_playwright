-- 008_data_quality_constraints.sql
-- Phase 4d: Add CHECK constraints for data quality
--
-- NOTE: SQLite parses ALTER TABLE ADD CONSTRAINT CHECK but does NOT enforce
-- CHECK constraints added via ALTER TABLE. These are for:
--   1. Documentation of expected data quality rules
--   2. Compatibility with PostgreSQL (OCI) which enforces them
--   3. Future migration when transitioning to PostgreSQL
--
-- For SQLite enforcement, constraints must be defined at CREATE TABLE time
-- or the table must be recreated. These constraints are enforced in OCI (PostgreSQL).

-- Batting: hits cannot exceed at-bats (NULL-safe: skip if either is NULL)
ALTER TABLE player_season_batting ADD CONSTRAINT ck_batting_hits_lte_ab CHECK (hits IS NULL OR at_bats IS NULL OR hits <= at_bats);
ALTER TABLE player_season_batting ADD CONSTRAINT ck_batting_ab_lte_pa CHECK (at_bats IS NULL OR plate_appearances IS NULL OR at_bats <= plate_appearances);

-- Pitching: earned runs cannot exceed runs allowed
ALTER TABLE player_season_pitching ADD CONSTRAINT ck_pitching_er_lte_r CHECK (earned_runs IS NULL OR runs_allowed IS NULL OR earned_runs <= runs_allowed);

-- Game scores: home/away scores must be non-negative
ALTER TABLE game ADD CONSTRAINT ck_game_home_score_nonneg CHECK (home_score IS NULL OR home_score >= 0);
ALTER TABLE game ADD CONSTRAINT ck_game_away_score_nonneg CHECK (away_score IS NULL OR away_score >= 0);

-- Game inning scores: inning runs must be non-negative
ALTER TABLE game_inning_scores ADD CONSTRAINT ck_inning_score_nonneg CHECK (runs IS NULL OR runs >= 0);

-- Player stats: games cannot be negative
ALTER TABLE player_season_batting ADD CONSTRAINT ck_batting_games_nonneg CHECK (games IS NULL OR games >= 0);
ALTER TABLE player_season_pitching ADD CONSTRAINT ck_pitching_games_nonneg CHECK (games IS NULL OR games >= 0);
