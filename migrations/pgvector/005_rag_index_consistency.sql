-- 005_rag_index_consistency.sql
-- Keep pgvector rows auditable against the canonical sparse index.

ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS content_hash VARCHAR(64);
ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS index_version VARCHAR(64);
ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS index_status VARCHAR(24) NOT NULL DEFAULT 'ACTIVE';
ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS indexed_at TIMESTAMP;

UPDATE rag_chunks SET index_status = 'ACTIVE' WHERE index_status IS NULL;

CREATE INDEX IF NOT EXISTS idx_rag_chunks_content_hash ON rag_chunks (content_hash);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_index_version ON rag_chunks (index_version);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_index_status ON rag_chunks (index_status);
