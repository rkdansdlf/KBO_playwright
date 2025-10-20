-- Create player_season_batting table for KBO batting statistics
-- This corresponds to the PlayerSeasonBatting model in src/models/player.py

CREATE TABLE IF NOT EXISTS public.player_season_batting (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL, -- KBO player ID (not foreign key)
    season INTEGER NOT NULL,
    league VARCHAR(16) NOT NULL DEFAULT 'REGULAR',
    level VARCHAR(16) NOT NULL DEFAULT 'KBO1',
    source VARCHAR(16) NOT NULL DEFAULT 'ROLLUP',
    team_code VARCHAR(8),
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
    avg FLOAT,
    obp FLOAT,
    slg FLOAT,
    ops FLOAT,
    iso FLOAT,
    babip FLOAT,
    extra_stats JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add column comments
COMMENT ON COLUMN public.player_season_batting.player_id IS 'KBO player ID (not foreign key)';
COMMENT ON COLUMN public.player_season_batting.season IS 'Season year';
COMMENT ON COLUMN public.player_season_batting.league IS 'League type (REGULAR, EXHIBITION, etc.)';
COMMENT ON COLUMN public.player_season_batting.level IS 'League level (KBO1, KBO2, etc.)';
COMMENT ON COLUMN public.player_season_batting.source IS 'Data source (ROLLUP, CRAWLER, etc.)';
COMMENT ON COLUMN public.player_season_batting.team_code IS 'Team code (LG, NC, etc.)';
COMMENT ON COLUMN public.player_season_batting.extra_stats IS 'Additional statistics in JSON format';

-- Create unique constraint
ALTER TABLE public.player_season_batting 
ADD CONSTRAINT uq_player_season_batting 
UNIQUE (player_id, season, league, level);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_psb_player ON public.player_season_batting (player_id, season);

-- Enable RLS
ALTER TABLE public.player_season_batting ENABLE ROW LEVEL SECURITY;

-- Create policies (allow all operations for now)
CREATE POLICY "Allow all operations on player_season_batting" ON public.player_season_batting
    FOR ALL USING (true);