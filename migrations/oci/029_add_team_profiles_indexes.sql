-- Migration: Add team_profiles table and indexes
-- Description: Create team_profiles table (if not exists) and indexes to speed up lookup by profile and enforce uniqueness.

-- 0. Create team_profiles table (matches SQLite migration 029)
CREATE TABLE IF NOT EXISTS team_profiles (
    team_id VARCHAR(10) NOT NULL,
    profile VARCHAR(64) NOT NULL,
    PRIMARY KEY (team_id, profile)
);

-- 1. Create a unique index on (team_id, profile) to prevent duplicate tags/profiles per team
CREATE UNIQUE INDEX IF NOT EXISTS uq_team_profiles_team_id_profile
    ON team_profiles (team_id, profile);

-- 2. Create a lookup index on the profile column to speed up searching teams by keyword/tag
CREATE INDEX IF NOT EXISTS idx_team_profiles_profile
    ON team_profiles (profile);
