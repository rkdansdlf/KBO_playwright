-- 006_embedding_ivfflat_index.sql
-- Approximate cosine search for the populated pgvector RAG index.

CREATE INDEX IF NOT EXISTS idx_rag_chunks_embedding_ivfflat
    ON rag_chunks
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);
