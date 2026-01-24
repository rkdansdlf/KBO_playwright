-- ===================================================================
-- Game Events 테이블 생성 및 WPA 컬럼 추가
-- 012_add_wpa_columns.sql
-- ===================================================================

-- 1. game_events 테이블 생성 (없을 경우)
CREATE TABLE IF NOT EXISTS public.game_events (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL REFERENCES public.game(game_id) ON DELETE CASCADE,
    event_seq INTEGER NOT NULL,
    inning INTEGER,
    inning_half VARCHAR(6),
    outs INTEGER,
    batter_id INTEGER,
    batter_name VARCHAR(64),
    pitcher_id INTEGER,
    pitcher_name VARCHAR(64),
    description TEXT,
    event_type VARCHAR(32),
    result_code VARCHAR(16),
    rbi INTEGER,
    bases_before VARCHAR(3),
    bases_after VARCHAR(3),
    extra_json JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_game_event_seq UNIQUE (game_id, event_seq)
);

-- 2. 상세 상태 및 WPA 컬럼 추가 (이미 있을 경우 무시)
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='game_events' AND column_name='wpa') THEN
        ALTER TABLE public.game_events ADD COLUMN wpa FLOAT;
        ALTER TABLE public.game_events ADD COLUMN win_expectancy_before FLOAT;
        ALTER TABLE public.game_events ADD COLUMN win_expectancy_after FLOAT;
        ALTER TABLE public.game_events ADD COLUMN score_diff INTEGER; -- (Home - Away)
        ALTER TABLE public.game_events ADD COLUMN base_state INTEGER; -- Bitmask: 1=1B, 2=2B, 4=3B
        ALTER TABLE public.game_events ADD COLUMN home_score INTEGER;
        ALTER TABLE public.game_events ADD COLUMN away_score INTEGER;
    END IF;
END $$;

-- 3. 인덱스
CREATE INDEX IF NOT EXISTS idx_game_events_wpa ON public.game_events (wpa);
CREATE INDEX IF NOT EXISTS idx_game_events_inning ON public.game_events (game_id, inning, inning_half);

-- 4. RLS 설정
ALTER TABLE public.game_events ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies WHERE tablename = 'game_events' AND policyname = 'Allow all on game_events'
    ) THEN
        CREATE POLICY "Allow all on game_events" ON public.game_events FOR ALL USING (true);
    END IF;
END $$;
