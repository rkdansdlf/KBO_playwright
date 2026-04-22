-- 025_add_game_id_aliases.sql
-- Canonicalize KBO game identity while preserving alternate source IDs.

ALTER TABLE public.game
ADD COLUMN IF NOT EXISTS is_primary BOOLEAN NOT NULL DEFAULT TRUE;

CREATE TABLE IF NOT EXISTS public.game_id_aliases (
    alias_game_id VARCHAR(20) PRIMARY KEY,
    canonical_game_id VARCHAR(20) NOT NULL REFERENCES public.game(game_id),
    source VARCHAR(50),
    reason VARCHAR(120),
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_game_id_aliases_canonical_game_id
ON public.game_id_aliases (canonical_game_id);
