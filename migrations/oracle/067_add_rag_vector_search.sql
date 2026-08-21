-- Store RAG embeddings in Oracle AI Vector Search.
--
-- The original ORM baseline created EMBEDDING as a JSON/CLOB-compatible
-- column. Keep that legacy column intact and add a native VECTOR projection so
-- existing rows and rollback inspection remain possible.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_tab_columns
     WHERE table_name = 'RAG_CHUNKS'
       AND column_name = 'EMBEDDING_VECTOR';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE RAG_CHUNKS ADD (EMBEDDING_VECTOR VECTOR(1536, FLOAT32, DENSE))';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_indexes
     WHERE index_name = 'IDX_RAG_CHUNKS_EMBEDDING_HNSW';

    IF v_exists = 0 THEN
        BEGIN
            EXECUTE IMMEDIATE
                'CREATE VECTOR INDEX IDX_RAG_CHUNKS_EMBEDDING_HNSW '
                || 'ON RAG_CHUNKS (EMBEDDING_VECTOR) '
                || 'ORGANIZATION INMEMORY NEIGHBOR GRAPH '
                || 'DISTANCE COSINE WITH TARGET ACCURACY 90 '
                || 'PARAMETERS (TYPE HNSW, NEIGHBORS 32, EFCONSTRUCTION 200)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
    END IF;
END;
/
