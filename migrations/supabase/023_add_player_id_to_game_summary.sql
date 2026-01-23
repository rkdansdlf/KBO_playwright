-- ===================================================================
-- 경기 주요활약 테이블에 선수 ID 컬럼 추가
-- 023_add_player_id_to_game_summary.sql
-- ===================================================================

ALTER TABLE public.game_summary 
ADD COLUMN IF NOT EXISTS player_id INTEGER REFERENCES public.player_basic(player_id);

-- 인덱스 추가 (선수별 활약 조회 최적화)
CREATE INDEX IF NOT EXISTS idx_game_summary_player_id ON public.game_summary (player_id);

-- UPSERT 지원을 위한 유니크 인덱스
-- player_name이 NULL일 수 있으므로 COALESCE를 사용하거나 UNIQUE CONSTRAINT 대신 UNIQUE INDEX (COALESCE...) 사용 고려
-- 여기서는 SupabaseSync와의 호환성을 위해 최대한 단순하게 구성 (NULL은 서로 다른 값으로 처리됨)
CREATE UNIQUE INDEX IF NOT EXISTS uq_game_summary_entry 
ON public.game_summary (game_id, summary_type, player_name, detail_text);
