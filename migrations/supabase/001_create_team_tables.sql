-- Supabase Migration: Create Team Tables
-- Based on src/models/team.py SQLAlchemy models
-- Generated: 2025-10-16

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- Table: franchises
-- ============================================================
CREATE TABLE IF NOT EXISTS franchises (
    id SERIAL PRIMARY KEY,
    key VARCHAR(16) NOT NULL UNIQUE,
    canonical_name VARCHAR(64) NOT NULL,
    first_season INTEGER,
    last_season INTEGER,
    status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'DISSOLVED')),
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_franchises_status ON franchises(status);
CREATE INDEX idx_franchises_key ON franchises(key);

COMMENT ON TABLE franchises IS 'KBO franchises representing historical continuity';
COMMENT ON COLUMN franchises.key IS 'Unique franchise identifier (e.g., SAMSUNG, LG, KIA)';
COMMENT ON COLUMN franchises.canonical_name IS 'Current official name';
COMMENT ON COLUMN franchises.status IS 'ACTIVE or DISSOLVED';

-- ============================================================
-- Table: team_identities
-- ============================================================
CREATE TABLE IF NOT EXISTS team_identities (
    id SERIAL PRIMARY KEY,
    franchise_id INTEGER NOT NULL REFERENCES franchises(id) ON DELETE CASCADE,
    name_kor VARCHAR(64) NOT NULL,
    name_eng VARCHAR(64),
    short_code VARCHAR(8),
    city_kor VARCHAR(32),
    city_eng VARCHAR(32),
    start_season INTEGER,
    end_season INTEGER,
    is_current SMALLINT NOT NULL DEFAULT 0 CHECK (is_current IN (0, 1)),
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(franchise_id, name_kor, start_season)
);

CREATE INDEX idx_team_identities_franchise ON team_identities(franchise_id);
CREATE INDEX idx_team_identities_current ON team_identities(is_current);
CREATE INDEX idx_team_identities_season ON team_identities(start_season, end_season);
CREATE INDEX idx_team_identities_short_code ON team_identities(short_code);

COMMENT ON TABLE team_identities IS 'Team branding changes over time (e.g., MBC → LG, Haitai → KIA)';
COMMENT ON COLUMN team_identities.is_current IS '1 if currently active branding, 0 if historical';

-- ============================================================
-- Table: franchise_events
-- ============================================================
CREATE TABLE IF NOT EXISTS franchise_events (
    id SERIAL PRIMARY KEY,
    franchise_id INTEGER NOT NULL REFERENCES franchises(id) ON DELETE CASCADE,
    event_type VARCHAR(32) NOT NULL CHECK (event_type IN ('FOUNDED', 'OWNERSHIP_CHANGE', 'REBRAND', 'RELOCATION', 'DISSOLVED')),
    event_date DATE NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_franchise_events_franchise ON franchise_events(franchise_id);
CREATE INDEX idx_franchise_events_date ON franchise_events(event_date);

COMMENT ON TABLE franchise_events IS 'Major franchise events (founding, ownership changes, rebrands, etc.)';

-- ============================================================
-- Table: ballparks
-- ============================================================
CREATE TABLE IF NOT EXISTS ballparks (
    id SERIAL PRIMARY KEY,
    name_kor VARCHAR(64) NOT NULL UNIQUE,
    name_eng VARCHAR(64),
    city_kor VARCHAR(32),
    city_eng VARCHAR(32),
    opened_year INTEGER,
    closed_year INTEGER,
    capacity INTEGER,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ballparks_city ON ballparks(city_kor);

COMMENT ON TABLE ballparks IS 'KBO ballparks/stadiums';

-- ============================================================
-- Table: home_ballpark_assignments
-- ============================================================
CREATE TABLE IF NOT EXISTS home_ballpark_assignments (
    franchise_id INTEGER NOT NULL REFERENCES franchises(id) ON DELETE CASCADE,
    ballpark_id INTEGER NOT NULL REFERENCES ballparks(id) ON DELETE CASCADE,
    start_season INTEGER NOT NULL DEFAULT -1,
    end_season INTEGER,
    is_primary SMALLINT NOT NULL DEFAULT 1 CHECK (is_primary IN (0, 1)),
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (franchise_id, ballpark_id, start_season)
);

CREATE INDEX idx_home_ballpark_franchise ON home_ballpark_assignments(franchise_id);
CREATE INDEX idx_home_ballpark_ballpark ON home_ballpark_assignments(ballpark_id);
CREATE INDEX idx_home_ballpark_season ON home_ballpark_assignments(start_season, end_season);

COMMENT ON TABLE home_ballpark_assignments IS 'Franchise home ballpark assignments over time';
COMMENT ON COLUMN home_ballpark_assignments.start_season IS 'Use -1 for NULL/unknown start season';
COMMENT ON COLUMN home_ballpark_assignments.is_primary IS '1 if primary home, 0 if secondary/temporary';

-- ============================================================
-- Update Trigger for updated_at
-- ============================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_franchises_updated_at BEFORE UPDATE ON franchises
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_team_identities_updated_at BEFORE UPDATE ON team_identities
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_franchise_events_updated_at BEFORE UPDATE ON franchise_events
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_ballparks_updated_at BEFORE UPDATE ON ballparks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_home_ballpark_assignments_updated_at BEFORE UPDATE ON home_ballpark_assignments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
