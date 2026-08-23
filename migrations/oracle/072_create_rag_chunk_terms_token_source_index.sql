-- Source-scoped sparse searches need an index-order STOPKEY slice over
-- (token, source) so high-frequency tokens stay bounded under filters.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_indexes
     WHERE index_name = 'IDX_RAG_CHUNK_TERMS_TOKEN_SOURCE';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'CREATE INDEX IDX_RAG_CHUNK_TERMS_TOKEN_SOURCE '
            || 'ON RAG_CHUNK_TERMS (TOKEN, SOURCE_TABLE, RAG_CHUNK_ID) ONLINE';
    END IF;
END;
/
