-- Add status/staff_role/status_source columns to player_basic
ALTER TABLE public.player_basic
    ADD COLUMN IF NOT EXISTS status text,
    ADD COLUMN IF NOT EXISTS staff_role text,
    ADD COLUMN IF NOT EXISTS status_source text;
