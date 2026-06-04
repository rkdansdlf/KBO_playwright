-- migrations/oci/040_add_player_parsed_profile_fields.sql
-- Add parsed and structured profile fields to both player_basic and players tables in OCI (PostgreSQL).

BEGIN;

ALTER TABLE public.player_basic
  ADD COLUMN IF NOT EXISTS salary_amount bigint,
  ADD COLUMN IF NOT EXISTS salary_currency varchar(8),
  ADD COLUMN IF NOT EXISTS signing_bonus_amount bigint,
  ADD COLUMN IF NOT EXISTS signing_bonus_currency varchar(8),
  ADD COLUMN IF NOT EXISTS draft_year integer,
  ADD COLUMN IF NOT EXISTS draft_round integer,
  ADD COLUMN IF NOT EXISTS draft_pick_overall integer,
  ADD COLUMN IF NOT EXISTS draft_type varchar(32),
  ADD COLUMN IF NOT EXISTS education_path jsonb;

ALTER TABLE public.players
  ADD COLUMN IF NOT EXISTS salary_amount bigint,
  ADD COLUMN IF NOT EXISTS salary_currency varchar(8),
  ADD COLUMN IF NOT EXISTS signing_bonus_amount bigint,
  ADD COLUMN IF NOT EXISTS signing_bonus_currency varchar(8),
  ADD COLUMN IF NOT EXISTS draft_year integer,
  ADD COLUMN IF NOT EXISTS draft_round integer,
  ADD COLUMN IF NOT EXISTS draft_pick_overall integer,
  ADD COLUMN IF NOT EXISTS draft_type varchar(32),
  ADD COLUMN IF NOT EXISTS education_path jsonb;

CREATE INDEX IF NOT EXISTS idx_player_basic_draft_year ON public.player_basic(draft_year);
CREATE INDEX IF NOT EXISTS idx_players_draft_year      ON public.players(draft_year);

COMMIT;
