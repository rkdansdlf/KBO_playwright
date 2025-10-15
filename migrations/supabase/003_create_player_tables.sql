-- Supabase Migration: Create Player Tables
-- Based on src/models/player.py SQLAlchemy models
-- Generated: 2025-10-16

-- ============================================================
-- Table: players
-- ============================================================
CREATE TABLE IF NOT EXISTS players (
    id SERIAL PRIMARY KEY,
    kbo_person_id VARCHAR(32) UNIQUE,
    birth_date DATE,
    birth_place VARCHAR(64),
    height_cm INTEGER,
    weight_kg INTEGER,
    bats VARCHAR(1),  -- R/L/S
    throws VARCHAR(1),  -- R/L
    is_foreign_player BOOLEAN NOT NULL DEFAULT FALSE,
    debut_year INTEGER,
    retire_year INTEGER,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_players_status ON players(status);
CREATE INDEX idx_players_debut ON players(debut_year);

COMMENT ON TABLE players IS 'Player master records';
COMMENT ON COLUMN players.kbo_person_id IS 'KBO official person ID';
COMMENT ON COLUMN players.status IS 'ACTIVE, RETIRED, INACTIVE, etc.';

-- ============================================================
-- Table: player_identities
-- ============================================================
CREATE TABLE IF NOT EXISTS player_identities (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    name_kor VARCHAR(64) NOT NULL,
    name_eng VARCHAR(64),
    start_date DATE,
    end_date DATE,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_player_identities_player ON player_identities(player_id);
CREATE INDEX idx_player_identities_period ON player_identities(player_id, start_date, end_date);
CREATE INDEX idx_player_identities_name ON player_identities(name_kor);

COMMENT ON TABLE player_identities IS 'Player name history and alternative spellings';

-- ============================================================
-- Table: player_codes
-- ============================================================
CREATE TABLE IF NOT EXISTS player_codes (
    player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    source VARCHAR(16) NOT NULL,  -- KBO, STATIZ, etc.
    code VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (player_id, source)
);

CREATE INDEX idx_player_codes_code ON player_codes(source, code);

COMMENT ON TABLE player_codes IS 'Mapping external IDs to player records';
COMMENT ON COLUMN player_codes.source IS 'Source system (KBO, STATIZ, etc.)';

-- ============================================================
-- Table: player_stints
-- ============================================================
CREATE TABLE IF NOT EXISTS player_stints (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    franchise_id INTEGER NOT NULL REFERENCES franchises(id) ON DELETE RESTRICT,
    identity_id INTEGER REFERENCES team_identities(id) ON DELETE SET NULL,
    uniform_number VARCHAR(8),
    primary_pos VARCHAR(4),  -- 1B, 2B, SS, 3B, OF, C, DH, P, etc.
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (player_id, franchise_id, start_date, end_date)
);

CREATE INDEX idx_player_stints_player ON player_stints(player_id);
CREATE INDEX idx_player_stints_franchise ON player_stints(franchise_id);
CREATE INDEX idx_player_stints_current ON player_stints(is_current);

COMMENT ON TABLE player_stints IS 'Player team affiliation history';

-- ============================================================
-- Table: player_season_batting
-- ============================================================
CREATE TABLE IF NOT EXISTS player_season_batting (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    season INTEGER NOT NULL,
    league VARCHAR(16) NOT NULL DEFAULT 'REGULAR',  -- REGULAR, FUTURES, etc.
    level VARCHAR(16) NOT NULL DEFAULT 'KBO1',  -- KBO1, KBO2, etc.
    source VARCHAR(16) NOT NULL DEFAULT 'ROLLUP',  -- ROLLUP, PROFILE, etc.
    team_code VARCHAR(8),

    -- Core stats
    games INTEGER,
    plate_appearances INTEGER,
    at_bats INTEGER,
    runs INTEGER,
    hits INTEGER,
    doubles INTEGER,
    triples INTEGER,
    home_runs INTEGER,
    rbi INTEGER,
    walks INTEGER,
    intentional_walks INTEGER,
    hbp INTEGER,
    strikeouts INTEGER,
    stolen_bases INTEGER,
    caught_stealing INTEGER,
    sacrifice_hits INTEGER,
    sacrifice_flies INTEGER,
    gdp INTEGER,

    -- Calculated stats
    avg FLOAT,
    obp FLOAT,
    slg FLOAT,
    ops FLOAT,
    iso FLOAT,
    babip FLOAT,

    -- Extra stats (JSON)
    extra_stats JSONB,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (player_id, season, league, level)
);

CREATE INDEX idx_psb_player_season ON player_season_batting(player_id, season);
CREATE INDEX idx_psb_season ON player_season_batting(season);

COMMENT ON TABLE player_season_batting IS 'Season-level batting statistics';

-- ============================================================
-- Table: player_season_pitching
-- ============================================================
CREATE TABLE IF NOT EXISTS player_season_pitching (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    season INTEGER NOT NULL,
    league VARCHAR(16) NOT NULL DEFAULT 'REGULAR',
    level VARCHAR(16) NOT NULL DEFAULT 'KBO1',
    source VARCHAR(16) NOT NULL DEFAULT 'ROLLUP',
    team_code VARCHAR(8),

    -- Core stats
    games INTEGER,
    games_started INTEGER,
    wins INTEGER,
    losses INTEGER,
    saves INTEGER,
    holds INTEGER,
    innings_outs INTEGER,  -- Innings * 3 (e.g., 5.2 IP = 17 outs)
    hits_allowed INTEGER,
    runs_allowed INTEGER,
    earned_runs INTEGER,
    home_runs_allowed INTEGER,
    walks_allowed INTEGER,
    intentional_walks INTEGER,
    hit_batters INTEGER,
    strikeouts INTEGER,
    wild_pitches INTEGER,
    balks INTEGER,

    -- Calculated stats
    era FLOAT,
    whip FLOAT,
    fip FLOAT,
    k_per_nine FLOAT,
    bb_per_nine FLOAT,
    kbb FLOAT,  -- K/BB ratio

    -- Extra stats (JSON)
    extra_stats JSONB,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (player_id, season, league, level)
);

CREATE INDEX idx_psp_player_season ON player_season_pitching(player_id, season);
CREATE INDEX idx_psp_season ON player_season_pitching(season);

COMMENT ON TABLE player_season_pitching IS 'Season-level pitching statistics';

-- ============================================================
-- Update Triggers
-- ============================================================
CREATE TRIGGER update_players_updated_at BEFORE UPDATE ON players
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_player_identities_updated_at BEFORE UPDATE ON player_identities
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_player_codes_updated_at BEFORE UPDATE ON player_codes
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_player_stints_updated_at BEFORE UPDATE ON player_stints
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_player_season_batting_updated_at BEFORE UPDATE ON player_season_batting
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_player_season_pitching_updated_at BEFORE UPDATE ON player_season_pitching
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
