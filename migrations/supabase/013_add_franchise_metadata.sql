
-- Migration: Add metadata, history, and optimize schema
-- Description: Adds JSONB metadata, team_history table, and aliases. Renames franchises if needed.

-- 1. Rename table if needed (Standardize on team_franchises)
DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'franchises') THEN
    ALTER TABLE public.franchises RENAME CONSTRAINT pk_franchises TO pk_team_franchises;
    ALTER TABLE public.franchises RENAME TO team_franchises;
  END IF;
END $$;

-- 2. Add Columns to team_franchises
ALTER TABLE public.team_franchises ADD COLUMN IF NOT EXISTS metadata_json JSONB;
ALTER TABLE public.team_franchises ADD COLUMN IF NOT EXISTS web_url VARCHAR(255);

-- 3. Update teams table
ALTER TABLE public.teams ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;
ALTER TABLE public.teams ADD COLUMN IF NOT EXISTS aliases TEXT[];

-- 4. Handle Legacy team_history and Create New Table
-- If team_history exists but has start_season (legacy), rename it.
DO $$
BEGIN
  IF EXISTS (SELECT FROM information_schema.columns WHERE table_name = 'team_history' AND column_name = 'start_season') THEN
    ALTER TABLE public.team_history RENAME TO team_history_legacy;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.team_history (
    id SERIAL PRIMARY KEY,
    franchise_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    team_name VARCHAR(50) NOT NULL,
    team_code VARCHAR(10) NOT NULL, -- The code used in that season
    logo_url VARCHAR(255),
    ranking INTEGER,
    stadium VARCHAR(50),
    city VARCHAR(30),
    color VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
    
    CONSTRAINT fk_team_history_franchise FOREIGN KEY (franchise_id) REFERENCES public.team_franchises(id)
);

-- Index for unique history lookups
CREATE UNIQUE INDEX IF NOT EXISTS uq_team_history_season_code ON public.team_history (season, team_code);
