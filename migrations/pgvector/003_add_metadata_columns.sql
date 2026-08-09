-- 003_add_metadata_columns.sql
-- RAG 청크 메타데이터 전용 컬럼 추가 (플랫폼 필터링 대비)
-- 실행 대상: 로컬 Docker pgvector (kbo_rag DB)
-- 사전 조건: 001, 002 실행 완료

ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS document_type VARCHAR(30);
ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS game_date DATE;
ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS published_at TIMESTAMP;
ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS source_url VARCHAR(500);
ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS language VARCHAR(10);

-- 필터링용 인덱스
CREATE INDEX IF NOT EXISTS idx_rag_chunks_league_type_code ON rag_chunks (league_type_code);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_document_type ON rag_chunks (document_type);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_game_date ON rag_chunks (game_date);