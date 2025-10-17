-- Supabase Migration: Create player_basic Table
-- Simple player table from player search crawler
-- Source: Docs/PLAYERID_CRAWLING.md
-- Generated: 2025-10-16

-- ============================================================
-- Table: player_basic
-- ============================================================
CREATE TABLE IF NOT EXISTS player_basic (
    player_id INTEGER PRIMARY KEY,  -- KBO player ID (not autoincrement)
    name VARCHAR(100) NOT NULL,     -- Player name (Korean)

    uniform_no VARCHAR(10),          -- Current uniform number
    team VARCHAR(50),                -- Current team
    position VARCHAR(50),            -- Primary position

    -- Birth date: original string + parsed date
    birth_date VARCHAR(20),          -- Birth date (original string from website)
    birth_date_date DATE,            -- Parsed birth date for querying

    -- Physical stats
    height_cm INTEGER,               -- Height in cm
    weight_kg INTEGER,               -- Weight in kg

    -- Career/origin
    career VARCHAR(200)              -- School/origin (출신교)
);

-- ============================================================
-- Indexes for query optimization
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_player_basic_name      ON player_basic(name);
CREATE INDEX IF NOT EXISTS idx_player_basic_team      ON player_basic(team);
CREATE INDEX IF NOT EXISTS idx_player_basic_position  ON player_basic(position);
CREATE INDEX IF NOT EXISTS idx_player_basic_team_pos  ON player_basic(team, position);

-- ============================================================
-- Comments
-- ============================================================
COMMENT ON TABLE player_basic IS 'Basic player information from KBO search crawler';
COMMENT ON COLUMN player_basic.player_id IS 'KBO official player ID (primary key)';
COMMENT ON COLUMN player_basic.name IS 'Player name in Korean';
COMMENT ON COLUMN player_basic.uniform_no IS 'Current uniform number';
COMMENT ON COLUMN player_basic.team IS 'Current team affiliation';
COMMENT ON COLUMN player_basic.position IS 'Primary position';
COMMENT ON COLUMN player_basic.birth_date IS 'Birth date as string (original from website)';
COMMENT ON COLUMN player_basic.birth_date_date IS 'Parsed birth date (DATE type)';
COMMENT ON COLUMN player_basic.height_cm IS 'Height in centimeters';
COMMENT ON COLUMN player_basic.weight_kg IS 'Weight in kilograms';
COMMENT ON COLUMN player_basic.career IS 'School or origin (출신교)';
