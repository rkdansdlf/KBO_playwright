-- 069_rag_chunks_meta_game_date_index.sql
-- rag_chunks.meta->>'game_date' 필터 표현식 인덱스를 추가한다.
--
-- 왜 필요한가:
-- RagSearchEngine._apply_postgresql_filters는 meta->>'game_date' 표현식을 비교하며,
-- 인덱스가 없으면 후보 검색이 Sequential Scan으로 이어져 수십만 건의 행을 매번 읽는다.
-- (Docs/runbooks/RAG_ANSWER_PIPELINE.md "The game_date filter had no index" 참조)
--
-- 프로덕션 환경:
-- 프로덕션 DB에는 이미 idx_rag_chunks_meta_game_date 인덱스가 수동으로 생성되어 있으므로
-- CREATE INDEX IF NOT EXISTS는 프로덕션에서 no-op으로 동작하며 schema_migrations에
-- 적용 기록만 남긴다.
--
-- CONCURRENTLY 미사용 사유:
-- 마이그레이션 러너(src/cli/sync/apply_postgres_migrations.py)가 트랜잭션 블록(with engine.begin())
-- 안에서 실행하므로 CONCURRENTLY 키워드를 사용할 수 없다 (PostgreSQL은 트랜잭션 내
-- CREATE INDEX CONCURRENTLY를 거부함). 락 없이 생성해야 하는 환경에서는 운영자가 사전에
-- CONCURRENTLY로 수동 생성한 후 마이그레이션을 실행할 수 있다.

CREATE INDEX IF NOT EXISTS idx_rag_chunks_meta_game_date
    ON rag_chunks ((meta ->> 'game_date'));
