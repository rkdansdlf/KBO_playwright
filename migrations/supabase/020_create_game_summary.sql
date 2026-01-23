-- ===================================================================
-- 경기 주요활약 테이블 생성
-- 020_create_game_summary.sql
-- 경기별 결승타, 홈런, 2루타, 실책 등의 요약 정보를 저장한다.
-- ===================================================================

CREATE TABLE IF NOT EXISTS public.game_summary (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL REFERENCES public.game(game_id) ON DELETE CASCADE,
    summary_type VARCHAR(50),  -- 예: '결승타', '홈런', '실책' 등
    player_name VARCHAR(50),   -- 해당 선수명 (옵션)
    detail_text TEXT,          -- 구체적인 활약 내용
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 인덱스 생성 (조회 성능 향상)
CREATE INDEX IF NOT EXISTS idx_game_summary_game_id ON public.game_summary (game_id);
CREATE INDEX IF NOT EXISTS idx_game_summary_summary_type ON public.game_summary (summary_type);

-- RLS 설정
ALTER TABLE public.game_summary ENABLE ROW LEVEL SECURITY;

-- 모든 사용자에게 읽기/쓰기 권한 부여 (crawler 및 bot 용)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policy WHERE polname = 'Allow all on game_summary'
    ) THEN
        CREATE POLICY "Allow all on game_summary" ON public.game_summary FOR ALL USING (true);
    END IF;
END $$;
