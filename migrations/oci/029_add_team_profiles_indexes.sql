-- Migration: Add Indexes for team_profiles
-- Description: Create indexes on the team_profiles table to speed up lookup by profile and enforce uniqueness.

-- 1. Create a unique index on (team_id, profile) to prevent duplicate tags/profiles per team
CREATE UNIQUE INDEX IF NOT EXISTS uq_team_profiles_team_id_profile
    ON team_profiles (team_id, profile);

-- 2. Create a lookup index on the profile column to speed up searching teams by keyword/tag
CREATE INDEX IF NOT EXISTS idx_team_profiles_profile
    ON team_profiles (profile);
