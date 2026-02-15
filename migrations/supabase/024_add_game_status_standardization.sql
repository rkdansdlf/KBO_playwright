-- 024_add_game_status_standardization.sql
-- Ensure game_status exists on game table and normalize legacy values.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'game'
          AND column_name = 'game_status'
    ) THEN
        ALTER TABLE public.game
        ADD COLUMN game_status VARCHAR(32);
    END IF;
END $$;

UPDATE public.game
SET game_status = CASE
    WHEN home_score IS NOT NULL AND away_score IS NOT NULL THEN 'COMPLETED'
    WHEN game_date > CURRENT_DATE THEN 'SCHEDULED'
    WHEN game_status IS NULL THEN NULL
    WHEN UPPER(game_status) IN ('COMPLETED', 'PLAYED', 'FINAL', 'FINISHED', 'END') THEN 'COMPLETED'
    WHEN UPPER(game_status) IN ('SCHEDULED', 'SCHEDULE', 'PENDING') THEN 'SCHEDULED'
    WHEN UPPER(game_status) IN ('CANCELLED', 'CANCELED', 'RAINOUT') THEN 'CANCELLED'
    WHEN UPPER(game_status) IN ('POSTPONED', 'DELAYED', 'DEFERRED') THEN 'POSTPONED'
    WHEN UPPER(game_status) IN ('UNRESOLVED_MISSING') THEN 'UNRESOLVED_MISSING'
    ELSE game_status
END;

DO $$
DECLARE
    existing_definition TEXT;
BEGIN
    SELECT pg_get_constraintdef(c.oid)
      INTO existing_definition
    FROM pg_constraint c
    WHERE c.conrelid = 'public.game'::regclass
      AND c.conname = 'game_status_check'
      AND c.contype = 'c';

    IF existing_definition IS NOT NULL THEN
        ALTER TABLE public.game
        DROP CONSTRAINT game_status_check;
    END IF;

    ALTER TABLE public.game
    ADD CONSTRAINT game_status_check
    CHECK (
        game_status IN (
            'SCHEDULED',
            'COMPLETED',
            'CANCELLED',
            'POSTPONED',
            'DRAW',
            'UNRESOLVED_MISSING'
        )
    );
END $$;

CREATE INDEX IF NOT EXISTS idx_game_game_status
ON public.game (game_status);
