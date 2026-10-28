-- 066_rag_chunks_metadata_columns.sql
-- rag_chunks에 청크 메타데이터 컬럼을 추가한다.
--
-- 왜 필요한가: `RagChunkVector`(src/models/rag_chunk_vector.py)가 이 다섯 컬럼을
-- SELECT하는데 운영 DB에 없어서 **벡터 검색이 매번 UndefinedColumn으로 실패**했다
-- (`column rag_chunks.document_type does not exist`). 2026-08-10 커밋 72a1679c가
-- 모델을 확장하면서 `migrations/pgvector/003_add_metadata_columns.sql`을 함께
-- 만들었지만 그 체인에는 러너가 없다 — 그 파일의 주석도 "실행 대상: 로컬 Docker
-- pgvector (kbo_rag DB)"라고 적고 있다. 그래서 모델만 앞서 나가고 운영 DB는
-- 따라오지 않았고, RAG 조회가 전부 실패했다. 생성기(build_rag_index)는 같은
-- 이름의 필드를 청크마다 만들지만 모델에 컬럼이 없어 저장되지도 않았다.
--
-- 저장 경로는 `RagChunk`(이 컬럼들이 없는 모델)를 쓰므로 인덱싱은 성공했고,
-- 검색 경로만 조용히 죽어 있었다 — 즉 "인덱스는 채워졌는데 조회가 안 되는"
-- 상태였다.
--
-- pgvector/003과 같은 내용이며, 그 체인 대신 정식 러너가 관리하도록 여기에 둔다.
-- IF NOT EXISTS이므로 양쪽이 모두 적용돼도 안전하다. 기존 행의 값은 NULL로
-- 남는다 — 지금까지 저장되지 못한 값은 원천에서 다시 만들 수 있지만, 검색이
-- 동작하는 데는 필요하지 않다(컬럼은 필터용이다).

ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS document_type VARCHAR(30);
ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS game_date DATE;
ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS published_at TIMESTAMP;
ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS source_url VARCHAR(500);
ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS language VARCHAR(10);

-- 검색이 실제로 거르는 필드들. document_type과 game_date는 필터로 쓰이고,
-- league_type_code는 이전 체인(051~053)에 이미 있었지만 인덱스가 없을 수 있어
-- 함께 만든다(IF NOT EXISTS).
CREATE INDEX IF NOT EXISTS idx_rag_chunks_league_type_code ON rag_chunks (league_type_code);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_document_type ON rag_chunks (document_type);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_game_date ON rag_chunks (game_date);
