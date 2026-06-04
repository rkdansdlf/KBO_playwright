-- migrations/oci/038_player_basic_birth_date_index.sql
-- Create column birth_date_date and corresponding indexes on public.player_basic if they do not exist.

BEGIN;

ALTER TABLE public.player_basic
  ADD COLUMN IF NOT EXISTS birth_date_date date;

CREATE INDEX IF NOT EXISTS idx_player_basic_name     ON public.player_basic(name);
CREATE INDEX IF NOT EXISTS idx_player_basic_team     ON public.player_basic(team);
CREATE INDEX IF NOT EXISTS idx_player_basic_position ON public.player_basic(position);
CREATE INDEX IF NOT EXISTS idx_player_basic_team_pos ON public.player_basic(team, position);

COMMIT;
