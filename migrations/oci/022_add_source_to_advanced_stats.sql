-- 022_add_source_to_advanced_stats.sql
-- Add source column to player_season_fielding and player_season_baserunning for OCI (PostgreSQL)

ALTER TABLE player_season_fielding ADD COLUMN IF NOT EXISTS source VARCHAR(20) DEFAULT 'CRAWLER';
ALTER TABLE player_season_baserunning ADD COLUMN IF NOT EXISTS source VARCHAR(20) DEFAULT 'CRAWLER';
