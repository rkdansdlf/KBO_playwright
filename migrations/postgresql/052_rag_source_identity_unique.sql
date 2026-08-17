-- 052_rag_source_identity_unique.sql
-- Source identity uniqueness required by ON CONFLICT bulk upserts
-- (src/services/rag_index_propagation.py: on_conflict_do_update index_elements).

DELETE FROM rag_chunks a USING rag_chunks b
WHERE a.id > b.id
  AND a.source_table = b.source_table
  AND a.source_row_id = b.source_row_id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_rag_chunks_source_identity
    ON rag_chunks (source_table, source_row_id);
