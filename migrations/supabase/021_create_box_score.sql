-- ===================================================================
-- 경기 박스스코어 테이블 생성
-- 021_create_box_score.sql
-- 경기별 이닝별 점수 및 최종 합계(R, H, E)를 저장한다.
-- ===================================================================

CREATE TABLE IF NOT EXISTS public.box_score (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL REFERENCES public.game(game_id) ON DELETE CASCADE,
    
    -- 원정팀 이닝별 점수 (1~15이닝)
    away_1 INTEGER, away_2 INTEGER, away_3 INTEGER, away_4 INTEGER, away_5 INTEGER,
    away_6 INTEGER, away_7 INTEGER, away_8 INTEGER, away_9 INTEGER, away_10 INTEGER,
    away_11 INTEGER, away_12 INTEGER, away_13 INTEGER, away_14 INTEGER, away_15 INTEGER,
    
    -- 홈팀 이닝별 점수 (1~15이닝)
    home_1 INTEGER, home_2 INTEGER, home_3 INTEGER, home_4 INTEGER, home_5 INTEGER,
    home_6 INTEGER, home_7 INTEGER, home_8 INTEGER, home_9 INTEGER, home_10 INTEGER,
    home_11 INTEGER, home_12 INTEGER, home_13 INTEGER, home_14 INTEGER, home_15 INTEGER,
    
    -- 최종 합계 (Runs, Hits, Errors)
    away_r INTEGER, away_h INTEGER, away_e INTEGER,
    home_r INTEGER, home_h INTEGER, home_e INTEGER,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT uq_box_score_game UNIQUE (game_id)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_box_score_game_id ON public.box_score (game_id);

-- RLS 설정
ALTER TABLE public.box_score ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policy WHERE polname = 'Allow all on box_score'
    ) THEN
        CREATE POLICY "Allow all on box_score" ON public.box_score FOR ALL USING (true);
    END IF;
END $$;
