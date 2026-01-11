
-- 015_add_game_summary_constraint.sql
-- GameSummary 테이블의 멱등성 보장을 위한 유니크 제약 조건 추가

DO $$
BEGIN
    -- 기존 중복 데이터가 있다면 제거 (안전장치)
    -- (game_id, summary_type, detail_text)가 같은 행 중 id가 큰 것을 삭제
    DELETE FROM public.game_summary a USING public.game_summary b
    WHERE a.id < b.id
      AND a.game_id = b.game_id
      AND a.summary_type = b.summary_type
      AND a.detail_text = b.detail_text;

    -- 유니크 제약 조건 추가
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_game_summary_entry'
    ) THEN
        ALTER TABLE public.game_summary
        ADD CONSTRAINT uq_game_summary_entry UNIQUE (game_id, summary_type, detail_text);
    END IF;
END $$;
