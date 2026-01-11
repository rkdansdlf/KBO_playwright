
-- Create player_movements table
CREATE TABLE IF NOT EXISTS public.player_movements (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    section VARCHAR(50) NOT NULL,
    team_code VARCHAR(20) NOT NULL,
    player_name VARCHAR(100) NOT NULL,
    remarks TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- Comments
COMMENT ON TABLE public.player_movements IS 'Player status changes (Trade, FA, Waiver, etc.)';
COMMENT ON COLUMN public.player_movements.date IS 'Event date';
COMMENT ON COLUMN public.player_movements.section IS 'Movement type (e.g. Trade)';
COMMENT ON COLUMN public.player_movements.team_code IS 'Related team';
COMMENT ON COLUMN public.player_movements.player_name IS 'Player name (with position info)';
COMMENT ON COLUMN public.player_movements.remarks IS 'Detailed remarks';

-- Constraints: Ensure uniqueness
ALTER TABLE public.player_movements 
    ADD CONSTRAINT uq_player_movement 
    UNIQUE (date, team_code, player_name, section);

-- Indexes
CREATE INDEX idx_player_movement_date ON public.player_movements(date);
CREATE INDEX idx_player_movement_player ON public.player_movements(player_name);
CREATE INDEX idx_player_movement_team ON public.player_movements(team_code);

-- RLS
ALTER TABLE public.player_movements ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all on player_movements"
    ON public.player_movements
    FOR ALL
    USING (true);
