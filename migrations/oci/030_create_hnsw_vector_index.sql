-- Create HNSW index for cosine similarity search on the 256-dimensional embedding column.
-- The pgvector extension is already enabled in the 'extensions' schema.
SET maintenance_work_mem = '512MB';
SET max_parallel_maintenance_workers = 0;


-- Commented out HNSW index creation because building it on 600k rows requires substantial CPU/RAM
-- and causes Docker / OCI free-tier instances to crash (OOM / DiskFull / connection timeout).
-- Exact similarity search (flat scan) will be used instead.
-- If performance tuning is required, this index should be built on a dedicated instance.
/*
DROP INDEX IF EXISTS idx_rag_chunks_embedding_hnsw;

CREATE INDEX IF NOT EXISTS idx_rag_chunks_embedding_hnsw
ON rag_chunks USING hnsw (embedding extensions.vector_cosine_ops);
*/


