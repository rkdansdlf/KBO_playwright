-- 020_create_advanced_stats.sql
-- Migration to add player_season_fielding and player_season_baserunning tables to OCI (PostgreSQL)

-- 1. Player Season Fielding Stats
CREATE TABLE IF NOT EXISTS player_season_fielding (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES player_basic(player_id),
    team_id VARCHAR(10) NOT NULL REFERENCES teams(team_id),
    year INTEGER NOT NULL,
    position_id VARCHAR(10) NOT NULL, -- POS (e.g. C, 1B, SS)
    games INTEGER,
    games_started INTEGER,
    innings FLOAT,
    putouts INTEGER,
    assists INTEGER,
    errors INTEGER,
    double_plays INTEGER,
    fielding_pct FLOAT,
    pickoffs INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_player_season_fielding UNIQUE (player_id, team_id, year, position_id)
);

CREATE INDEX IF NOT EXISTS idx_psf_player_year ON player_season_fielding (player_id, year);

-- 2. Player Season Baserunning Stats
CREATE TABLE IF NOT EXISTS player_season_baserunning (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES player_basic(player_id),
    team_id VARCHAR(10) NOT NULL REFERENCES teams(team_id),
    year INTEGER NOT NULL,
    player_name VARCHAR(100),
    games INTEGER,
    stolen_base_attempts INTEGER,
    stolen_bases INTEGER,
    caught_stealing INTEGER,
    stolen_base_percentage FLOAT,
    out_on_base INTEGER,
    picked_off INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_player_season_baserunning UNIQUE (player_id, team_id, year)
);

CREATE INDEX IF NOT EXISTS idx_psb_run_player_year ON player_season_baserunning (player_id, year);

-- 3. Team Season Batting Stats (Ensuring it exists for Phase 1)
CREATE TABLE IF NOT EXISTS team_season_batting (
    id SERIAL PRIMARY KEY,
    team_id VARCHAR(10) NOT NULL REFERENCES teams(team_id),
    team_name VARCHAR(64) NOT NULL,
    season INTEGER NOT NULL,
    league VARCHAR(16) NOT NULL DEFAULT 'REGULAR',
    games INTEGER,
    plate_appearances INTEGER,
    at_bats INTEGER,
    runs INTEGER,
    hits INTEGER,
    doubles INTEGER,
    triples INTEGER,
    home_runs INTEGER,
    rbi INTEGER,
    stolen_bases INTEGER,
    caught_stealing INTEGER,
    walks INTEGER,
    strikeouts INTEGER,
    avg FLOAT,
    obp FLOAT,
    slg FLOAT,
    ops FLOAT,
    extra_stats JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_team_season_batting UNIQUE (team_id, season, league)
);

CREATE INDEX IF NOT EXISTS idx_team_batting_season ON team_season_batting (season, league);

-- 4. Team Season Pitching Stats (Ensuring it exists for Phase 1)
CREATE TABLE IF NOT EXISTS team_season_pitching (
    id SERIAL PRIMARY KEY,
    team_id VARCHAR(10) NOT NULL REFERENCES teams(team_id),
    team_name VARCHAR(64) NOT NULL,
    season INTEGER NOT NULL,
    league VARCHAR(16) NOT NULL DEFAULT 'REGULAR',
    games INTEGER,
    wins INTEGER,
    losses INTEGER,
    ties INTEGER,
    saves INTEGER,
    holds INTEGER,
    innings_pitched FLOAT,
    runs_allowed INTEGER,
    earned_runs INTEGER,
    hits_allowed INTEGER,
    home_runs_allowed INTEGER,
    walks_allowed INTEGER,
    strikeouts INTEGER,
    era FLOAT,
    whip FLOAT,
    avg_against FLOAT,
    extra_stats JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_team_season_pitching UNIQUE (team_id, season, league)
);

CREATE INDEX IF NOT EXISTS idx_team_pitching_season ON team_season_pitching (season, league);
