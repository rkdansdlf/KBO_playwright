-- Supabase Migration: Create Player Game Stats Tables (Batting & Pitching)
-- Replaces generic player_game_stats JSONB table with structured tables
-- Generated: 2025-10-16

-- ============================================================
-- Table: player_game_batting
-- ============================================================
CREATE TABLE IF NOT EXISTS player_game_batting (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL,
    player_id INTEGER,
    player_name VARCHAR(64),

    team_side VARCHAR(8) NOT NULL,
    team_code VARCHAR(8),

    batting_order SMALLINT,
    appearance_seq SMALLINT NOT NULL DEFAULT 1,
    position VARCHAR(8),
    is_starter BOOLEAN NOT NULL DEFAULT FALSE,

    source VARCHAR(20) NOT NULL DEFAULT 'GAMECENTER',

    -- Core batting stats
    plate_appearances SMALLINT,
    at_bats SMALLINT,
    runs SMALLINT,
    hits SMALLINT,
    doubles SMALLINT,
    triples SMALLINT,
    home_runs SMALLINT,
    rbi SMALLINT,

    -- Discipline stats
    walks SMALLINT,
    intentional_walks SMALLINT,
    hbp SMALLINT,
    strikeouts SMALLINT,

    -- Base running
    stolen_bases SMALLINT,
    caught_stealing SMALLINT,

    -- Situational
    sacrifice_hits SMALLINT,
    sacrifice_flies SMALLINT,
    gdp SMALLINT,

    -- Calculated rates
    avg DECIMAL(5,3),
    obp DECIMAL(5,3),
    slg DECIMAL(5,3),
    ops DECIMAL(5,3),
    iso DECIMAL(5,3),
    babip DECIMAL(5,3),

    -- Additional data
    extras JSONB,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(game_id, player_id)
);

CREATE INDEX idx_player_game_batting_game ON player_game_batting(game_id);
CREATE INDEX idx_player_game_batting_player ON player_game_batting(player_id);
CREATE INDEX idx_player_game_batting_team ON player_game_batting(team_code);
CREATE INDEX idx_player_game_batting_team_side ON player_game_batting(team_side);

COMMENT ON TABLE player_game_batting IS 'Per-game batting statistics for hitters';
COMMENT ON COLUMN player_game_batting.team_side IS 'home or away';
COMMENT ON COLUMN player_game_batting.source IS 'Data source (GAMECENTER, LINEUP_API, etc.)';

-- ============================================================
-- Table: player_game_pitching
-- ============================================================
CREATE TABLE IF NOT EXISTS player_game_pitching (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL,
    player_id INTEGER,
    player_name VARCHAR(64),

    team_side VARCHAR(8) NOT NULL,
    team_code VARCHAR(8),

    is_starting BOOLEAN NOT NULL DEFAULT FALSE,
    appearance_seq SMALLINT NOT NULL DEFAULT 1,

    source VARCHAR(20) NOT NULL DEFAULT 'GAMECENTER',

    -- Pitching volume
    innings_outs INTEGER,
    batters_faced SMALLINT,

    -- Pitching outcomes
    hits_allowed SMALLINT,
    runs_allowed SMALLINT,
    earned_runs SMALLINT,
    home_runs_allowed SMALLINT,
    walks_allowed SMALLINT,
    strikeouts SMALLINT,
    hit_batters SMALLINT,
    wild_pitches SMALLINT,
    balks SMALLINT,

    -- Game result
    wins SMALLINT,
    losses SMALLINT,
    saves SMALLINT,
    holds SMALLINT,
    decision VARCHAR(8),

    -- Calculated rates
    era DECIMAL(5,2),
    whip DECIMAL(5,3),
    fip DECIMAL(5,2),
    k_per_nine DECIMAL(5,2),
    bb_per_nine DECIMAL(5,2),
    kbb DECIMAL(5,3),

    -- Additional data
    extras JSONB,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(game_id, player_id)
);

CREATE INDEX idx_player_game_pitching_game ON player_game_pitching(game_id);
CREATE INDEX idx_player_game_pitching_player ON player_game_pitching(player_id);
CREATE INDEX idx_player_game_pitching_team ON player_game_pitching(team_code);
CREATE INDEX idx_player_game_pitching_team_side ON player_game_pitching(team_side);
CREATE INDEX idx_player_game_pitching_is_starting ON player_game_pitching(is_starting);

COMMENT ON TABLE player_game_pitching IS 'Per-game pitching statistics for pitchers';
COMMENT ON COLUMN player_game_pitching.team_side IS 'home or away';
COMMENT ON COLUMN player_game_pitching.innings_outs IS 'Innings pitched as outs (e.g., 5.1 IP = 16 outs)';
COMMENT ON COLUMN player_game_pitching.decision IS 'W, L, ND, etc.';

-- ============================================================
-- Update Triggers
-- ============================================================
CREATE TRIGGER update_player_game_batting_updated_at BEFORE UPDATE ON player_game_batting
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_player_game_pitching_updated_at BEFORE UPDATE ON player_game_pitching
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
