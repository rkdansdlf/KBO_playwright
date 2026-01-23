-- ===================================================================
-- KBO 데이터 임베딩 테이블 생성
-- 022_create_embeddings.sql
-- RAG를 위한 텍스트 임베딩 데이터를 저장한다.
-- ===================================================================

CREATE TABLE IF NOT EXISTS public.embeddings (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,    -- 출처 테이블 (예: 'player_basic', 'game')
    record_id VARCHAR(50) NOT NULL,     -- 해당 레코드의 PK
    content TEXT NOT NULL,              -- 임베딩 처리된 자연어 텍스트
    vector_data JSONB,                  -- 벡터 데이터 (미래에 pgvector로 확장 가능)
    metadata_json JSONB,                -- 필터링용 메타데이터 (연도, 팀 등)
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT uq_embedding_source UNIQUE (table_name, record_id)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_embeddings_table_name ON public.embeddings (table_name);
CREATE INDEX IF NOT EXISTS idx_embeddings_record_id ON public.embeddings (record_id);

-- RLS 설정
ALTER TABLE public.embeddings ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policy WHERE polname = 'Allow all on embeddings'
    ) THEN
        CREATE POLICY "Allow all on embeddings" ON public.embeddings FOR ALL USING (true);
    END IF;
END $$;
