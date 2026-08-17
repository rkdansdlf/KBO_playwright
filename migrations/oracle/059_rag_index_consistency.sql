-- Keep Oracle RAG chunk lifecycle columns aligned with the current model.
DECLARE
    v_exists NUMBER;

    PROCEDURE add_column_if_missing(p_column_name VARCHAR2, p_ddl VARCHAR2) IS
    BEGIN
        SELECT COUNT(*)
          INTO v_exists
          FROM user_tab_columns
         WHERE table_name = 'RAG_CHUNKS'
           AND column_name = UPPER(p_column_name);
        IF v_exists = 0 THEN
            EXECUTE IMMEDIATE p_ddl;
        END IF;
    END;
BEGIN
    add_column_if_missing('CONTENT_HASH', 'ALTER TABLE RAG_CHUNKS ADD (CONTENT_HASH VARCHAR2(64))');
    add_column_if_missing('INDEX_VERSION', 'ALTER TABLE RAG_CHUNKS ADD (INDEX_VERSION VARCHAR2(64))');
    add_column_if_missing(
        'INDEX_STATUS',
        'ALTER TABLE RAG_CHUNKS ADD (INDEX_STATUS VARCHAR2(24) DEFAULT ''ACTIVE'' NOT NULL)'
    );
    add_column_if_missing('INDEXED_AT', 'ALTER TABLE RAG_CHUNKS ADD (INDEXED_AT TIMESTAMP)');
END;
/

UPDATE RAG_CHUNKS
   SET INDEX_STATUS = 'ACTIVE'
 WHERE INDEX_STATUS IS NULL;
/

DECLARE
    v_exists NUMBER;

    PROCEDURE create_index_if_missing(p_index_name VARCHAR2, p_ddl VARCHAR2) IS
    BEGIN
        SELECT COUNT(*)
          INTO v_exists
          FROM user_indexes
        WHERE index_name = UPPER(p_index_name);
        IF v_exists = 0 THEN
            BEGIN
                EXECUTE IMMEDIATE p_ddl;
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1408 THEN
                        RAISE;
                    END IF;
            END;
        END IF;
    END;
BEGIN
    create_index_if_missing(
        'IDX_RAG_CHUNKS_CONTENT_HASH',
        'CREATE INDEX IDX_RAG_CHUNKS_CONTENT_HASH ON RAG_CHUNKS (CONTENT_HASH)'
    );
    create_index_if_missing(
        'IDX_RAG_CHUNKS_INDEX_VERSION',
        'CREATE INDEX IDX_RAG_CHUNKS_INDEX_VERSION ON RAG_CHUNKS (INDEX_VERSION)'
    );
    create_index_if_missing(
        'IDX_RAG_CHUNKS_INDEX_STATUS',
        'CREATE INDEX IDX_RAG_CHUNKS_INDEX_STATUS ON RAG_CHUNKS (INDEX_STATUS)'
    );
END;
/
