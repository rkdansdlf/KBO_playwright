-- 053_rag_search_text_index.sql
-- Indexed lexical candidate retrieval for the PostgreSQL RAG sparse index.

CREATE INDEX IF NOT EXISTS idx_rag_chunks_search_tsvector
    ON rag_chunks
    USING GIN (
        to_tsvector('simple', coalesce(title, '') || ' ' || content)
    );
