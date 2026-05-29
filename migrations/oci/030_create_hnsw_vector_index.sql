-- Create HNSW index for cosine similarity search on the 256-dimensional embedding column.
-- The pgvector extension is already enabled in the 'extensions' schema.
DROP INDEX IF EXISTS idx_rag_chunks_embedding_hnsw;

CREATE INDEX IF NOT EXISTS idx_rag_chunks_embedding_hnsw
ON rag_chunks USING hnsw (embedding extensions.vector_cosine_ops);
