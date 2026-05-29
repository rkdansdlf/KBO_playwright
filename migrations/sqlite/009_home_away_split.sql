-- 009_home_away_split.sql
-- Phase 4e: Home/Away split tables for batting and pitching

CREATE TABLE IF NOT EXISTS matchup_batter_home_away (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL REFERENCES player_basic(player_id) ON DELETE RESTRICT,
    season_year INTEGER NOT NULL,
    location TEXT NOT NULL,
    games INTEGER DEFAULT 0,
    plate_appearances INTEGER DEFAULT 0,
    at_bats INTEGER DEFAULT 0,
    hits INTEGER DEFAULT 0,
    doubles INTEGER DEFAULT 0,
    triples INTEGER DEFAULT 0,
    home_runs INTEGER DEFAULT 0,
    rbi INTEGER DEFAULT 0,
    walks INTEGER DEFAULT 0,
    strikeouts INTEGER DEFAULT 0,
    stolen_bases INTEGER DEFAULT 0,
    caught_stealing INTEGER DEFAULT 0,
    hbp INTEGER DEFAULT 0,
    sacrifice_flies INTEGER DEFAULT 0,
    avg REAL,
    obp REAL,
    slg REAL,
    ops REAL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, season_year, location)
);

CREATE TABLE IF NOT EXISTS matchup_pitcher_home_away (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL REFERENCES player_basic(player_id) ON DELETE RESTRICT,
    season_year INTEGER NOT NULL,
    location TEXT NOT NULL,
    games INTEGER DEFAULT 0,
    innings_outs INTEGER DEFAULT 0,
    hits_allowed INTEGER DEFAULT 0,
    home_runs_allowed INTEGER DEFAULT 0,
    walks_allowed INTEGER DEFAULT 0,
    strikeouts INTEGER DEFAULT 0,
    runs_allowed INTEGER DEFAULT 0,
    earned_runs INTEGER DEFAULT 0,
    era REAL,
    whip REAL,
    avg_against REAL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, season_year, location)
);
