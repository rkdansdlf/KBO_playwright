
-- 017_create_team_daily_roster.sql
-- Create table for daily team rosters (Players registered for 1st team)
-- Source: https://www.koreabaseball.com/Player/Register.aspx

CREATE TABLE IF NOT EXISTS public.team_daily_roster (
    id SERIAL PRIMARY KEY,
    roster_date DATE NOT NULL,
    team_code VARCHAR(10) NOT NULL,
    player_id INTEGER NOT NULL,
    player_name VARCHAR(50) NOT NULL,
    position VARCHAR(20),
    back_number VARCHAR(10),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_team_daily_roster UNIQUE (roster_date, team_code, player_id)
);

CREATE INDEX IF NOT EXISTS idx_team_daily_roster_date
ON public.team_daily_roster (roster_date);

CREATE INDEX IF NOT EXISTS idx_team_daily_roster_team
ON public.team_daily_roster (team_code);

-- RLS
ALTER TABLE public.team_daily_roster ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all on team_daily_roster" ON public.team_daily_roster FOR ALL USING (true);
