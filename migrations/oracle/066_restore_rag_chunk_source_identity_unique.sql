-- Restore the model-declared uniqueness of the RAG source identity.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_constraints
     WHERE table_name = 'RAG_CHUNKS'
       AND constraint_name = 'UQ_RAG_CHUNKS_SOURCE_IDENTITY'
       AND constraint_type = 'U';

    IF v_exists = 0 THEN
        BEGIN
            EXECUTE IMMEDIATE
                'ALTER TABLE RAG_CHUNKS ADD CONSTRAINT UQ_RAG_CHUNKS_SOURCE_IDENTITY '
                || 'UNIQUE (SOURCE_TABLE, SOURCE_ROW_ID)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE NOT IN (-2261, -1408) THEN
                    RAISE;
                END IF;
        END;
    END IF;
END;
/
