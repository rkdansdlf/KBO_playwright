-- Add the Oracle sparse postings table used to avoid CLOB scans.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_tables
     WHERE table_name = 'RAG_CHUNK_TERMS';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'CREATE TABLE RAG_CHUNK_TERMS ('
            || 'RAG_CHUNK_ID NUMBER(19) NOT NULL, '
            || 'TOKEN VARCHAR2(128 CHAR) NOT NULL, '
            || 'TERM_COUNT NUMBER(10) DEFAULT 1 NOT NULL, '
            || 'TITLE_COUNT NUMBER(10) DEFAULT 0 NOT NULL, '
            || 'GAME_DATE VARCHAR2(10 CHAR), '
            || 'CONSTRAINT PK_RAG_CHUNK_TERMS PRIMARY KEY (RAG_CHUNK_ID, TOKEN), '
            || 'CONSTRAINT FK_RAG_CHUNK_TERMS_CHUNK FOREIGN KEY (RAG_CHUNK_ID) '
            || 'REFERENCES RAG_CHUNKS (ID) ON DELETE CASCADE)';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_indexes
     WHERE index_name = 'IDX_RAG_CHUNK_TERMS_TOKEN_CHUNK';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'CREATE INDEX IDX_RAG_CHUNK_TERMS_TOKEN_CHUNK '
            || 'ON RAG_CHUNK_TERMS (TOKEN, RAG_CHUNK_ID)';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_indexes
     WHERE index_name = 'IDX_RAG_CHUNK_TERMS_GAME_DATE';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'CREATE INDEX IDX_RAG_CHUNK_TERMS_GAME_DATE '
            || 'ON RAG_CHUNK_TERMS (GAME_DATE, TOKEN, RAG_CHUNK_ID)';
    END IF;
END;
/
