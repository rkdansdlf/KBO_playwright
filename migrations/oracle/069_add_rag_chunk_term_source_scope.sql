-- Store source scope on sparse postings so source-filtered searches avoid a
-- second large join before token aggregation. The postings table is derived
-- data and is rebuilt by build_oracle_sparse_index after this migration.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_tab_columns
     WHERE table_name = 'RAG_CHUNK_TERMS'
       AND column_name = 'SOURCE_TABLE';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE RAG_CHUNK_TERMS ADD (SOURCE_TABLE VARCHAR2(100 CHAR))';
    END IF;
END;
/

TRUNCATE TABLE RAG_CHUNK_TERMS;
/

ALTER TABLE RAG_CHUNK_TERMS MODIFY (SOURCE_TABLE VARCHAR2(100 CHAR) NOT NULL);
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_indexes
     WHERE index_name = 'IDX_RAG_CHUNK_TERMS_SOURCE_TOKEN';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'CREATE INDEX IDX_RAG_CHUNK_TERMS_SOURCE_TOKEN '
            || 'ON RAG_CHUNK_TERMS (SOURCE_TABLE, TOKEN, RAG_CHUNK_ID)';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_indexes
     WHERE index_name = 'IDX_RAG_CHUNK_TERMS_SOURCE_DATE';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'CREATE INDEX IDX_RAG_CHUNK_TERMS_SOURCE_DATE '
            || 'ON RAG_CHUNK_TERMS (SOURCE_TABLE, GAME_DATE, TOKEN, RAG_CHUNK_ID)';
    END IF;
END;
/
