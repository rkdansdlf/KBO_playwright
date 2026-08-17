-- 004_switch_embedding_dimension.sql
-- 임베딩 모델 전환 (voyageai/voyage-4-lite(256차원) → perplexity/pplx-embed-v1-4b(1536차원))에
-- 따른 스키마 차원 변경.
-- 기존 256차원 임베딩은 새 모델에서 무효이므로 컬럼을 재생성하고,
-- build_rag_index 재빌드 시 1536차원 임베딩으로 다시 채워집니다.
-- 멱등성: 컬럼이 이미 vector(1536)이면 아무 작업도 하지 않습니다.

DO $$
DECLARE
    rag_type TEXT;
    cache_type TEXT;
BEGIN
    SELECT format_type(a.atttypid, a.atttypmod)
    INTO rag_type
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    WHERE c.relname = 'rag_chunks' AND a.attname = 'embedding' AND NOT a.attisdropped;

    IF rag_type = 'vector(256)' THEN
        ALTER TABLE rag_chunks DROP COLUMN embedding;
        ALTER TABLE rag_chunks ADD COLUMN embedding vector(1536);
    END IF;

    SELECT format_type(a.atttypid, a.atttypmod)
    INTO cache_type
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    WHERE c.relname = 'embedding_cache' AND a.attname = 'embedding' AND NOT a.attisdropped;

    IF cache_type = 'vector(256)' THEN
        ALTER TABLE embedding_cache DROP COLUMN embedding;
        ALTER TABLE embedding_cache ADD COLUMN embedding vector(1536) NOT NULL;
    END IF;
END $$;
