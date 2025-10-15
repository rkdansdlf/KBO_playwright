-- Supabase Migration: Create Game Tables
-- Based on src/models/game.py SQLAlchemy models
-- Generated: 2025-10-16

-- ============================================================
-- Custom ENUM Types
-- ============================================================
DO $$ BEGIN
    CREATE TYPE season_type AS ENUM ('preseason', 'regular', 'postseason');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE game_status AS ENUM ('scheduled', 'postponed', 'in_progress', 'completed', 'cancelled');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE crawl_status AS ENUM ('pending', 'crawled', 'parsed', 'saved', 'failed');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- ============================================================
-- Table: game_schedules
-- ============================================================
CREATE TABLE IF NOT EXISTS game_schedules (
    schedule_id SERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL UNIQUE,

    season_year INTEGER NOT NULL,
    season_type season_type NOT NULL,

    game_date DATE NOT NULL,
    game_time TIME,

    home_team_code VARCHAR(8),
    away_team_code VARCHAR(8),

    stadium VARCHAR(64),

    game_status game_status NOT NULL DEFAULT 'scheduled',
    postpone_reason VARCHAR(200),

    doubleheader_no INTEGER NOT NULL DEFAULT 0,

    series_id INTEGER,
    series_name VARCHAR(64),

    crawl_status crawl_status NOT NULL DEFAULT 'pending',
    crawl_attempts INTEGER NOT NULL DEFAULT 0,
    last_crawl_at TIMESTAMP WITH TIME ZONE,
    crawl_error TEXT,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_game_schedules_game_id ON game_schedules(game_id);
CREATE INDEX idx_game_schedules_date ON game_schedules(game_date);
CREATE INDEX idx_game_schedules_season ON game_schedules(season_year, season_type);
CREATE INDEX idx_game_schedules_teams ON game_schedules(home_team_code, away_team_code);
CREATE INDEX idx_game_schedules_crawl_status ON game_schedules(crawl_status);
CREATE INDEX idx_game_schedules_game_status ON game_schedules(game_status);

COMMENT ON TABLE game_schedules IS 'KBO game schedules with crawl tracking';
COMMENT ON COLUMN game_schedules.game_id IS 'KBO official game ID (e.g., 20250308HTLT0)';
COMMENT ON COLUMN game_schedules.crawl_status IS 'Tracks crawling pipeline progress';

-- ============================================================
-- Table: games
-- ============================================================
CREATE TABLE IF NOT EXISTS games (
    game_id VARCHAR(20) PRIMARY KEY,
    schedule_id INTEGER REFERENCES game_schedules(schedule_id) ON DELETE CASCADE,

    game_date DATE NOT NULL,
    game_time TIME,

    home_team_code VARCHAR(8),
    away_team_code VARCHAR(8),

    stadium VARCHAR(64),
    attendance INTEGER,
    weather VARCHAR(32),

    home_score INTEGER,
    away_score INTEGER,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_games_date ON games(game_date);
CREATE INDEX idx_games_teams ON games(home_team_code, away_team_code);
CREATE INDEX idx_games_schedule ON games(schedule_id);

COMMENT ON TABLE games IS 'Game metadata and final scores';

-- ============================================================
-- Table: game_lineups
-- ============================================================
CREATE TABLE IF NOT EXISTS game_lineups (
    lineup_id SERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL REFERENCES games(game_id) ON DELETE CASCADE,

    player_id VARCHAR(10) NOT NULL,
    player_name VARCHAR(64),
    team_code VARCHAR(8),

    position VARCHAR(8),
    batting_order SMALLINT,

    is_home BOOLEAN NOT NULL,
    is_pitcher BOOLEAN NOT NULL DEFAULT FALSE,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(game_id, player_id, team_code)
);

CREATE INDEX idx_game_lineups_game ON game_lineups(game_id);
CREATE INDEX idx_game_lineups_player ON game_lineups(player_id);
CREATE INDEX idx_game_lineups_team ON game_lineups(team_code);

COMMENT ON TABLE game_lineups IS 'Game lineup information (starting and substitute players)';

-- ============================================================
-- Table: player_game_stats
-- ============================================================
CREATE TABLE IF NOT EXISTS player_game_stats (
    stats_id SERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL REFERENCES games(game_id) ON DELETE CASCADE,
    player_id VARCHAR(10) NOT NULL,
    player_name VARCHAR(64),
    team_code VARCHAR(8),

    is_pitcher BOOLEAN NOT NULL DEFAULT FALSE,

    stats_json JSONB NOT NULL,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(game_id, player_id, is_pitcher)
);

CREATE INDEX idx_player_game_stats_game ON player_game_stats(game_id);
CREATE INDEX idx_player_game_stats_player ON player_game_stats(player_id);
CREATE INDEX idx_player_game_stats_team ON player_game_stats(team_code);
CREATE INDEX idx_player_game_stats_json ON player_game_stats USING gin(stats_json);

COMMENT ON TABLE player_game_stats IS 'Player statistics per game (stored as JSON for flexibility)';
COMMENT ON COLUMN player_game_stats.stats_json IS 'Raw stats as JSON (e.g., {"AB": 4, "H": 2, "HR": 1})';

-- ============================================================
-- Update Triggers
-- ============================================================
CREATE TRIGGER update_game_schedules_updated_at BEFORE UPDATE ON game_schedules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_games_updated_at BEFORE UPDATE ON games
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_game_lineups_updated_at BEFORE UPDATE ON game_lineups
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_player_game_stats_updated_at BEFORE UPDATE ON player_game_stats
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
