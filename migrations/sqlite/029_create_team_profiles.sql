-- team_profiles: tag/keyword table linking teams to descriptive profiles.
-- Used for team categorization (e.g., 'original_team', 'dynasty_era').

CREATE TABLE IF NOT EXISTS team_profiles (
    team_id VARCHAR(10) NOT NULL,
    profile VARCHAR(64) NOT NULL,
    PRIMARY KEY (team_id, profile)
);

CREATE INDEX IF NOT EXISTS idx_team_profiles_profile
    ON team_profiles (profile);
