-- Convert RAG lookup keys from CLOB to VARCHAR2 for Oracle comparisons.
-- Text columns created by the ORM baseline become CLOB on Oracle, but
-- source_table/source_row_id are equality lookup keys used by upserts.
DECLARE
    v_data_type VARCHAR2(30);
    v_new_exists NUMBER;

    PROCEDURE convert_key(
        p_old_column VARCHAR2,
        p_new_column VARCHAR2,
        p_length NUMBER
    ) IS
    BEGIN
        SELECT data_type
          INTO v_data_type
          FROM user_tab_columns
         WHERE table_name = 'RAG_CHUNKS'
           AND column_name = UPPER(p_old_column);

        IF v_data_type = 'CLOB' THEN
            SELECT COUNT(*)
              INTO v_new_exists
              FROM user_tab_columns
             WHERE table_name = 'RAG_CHUNKS'
               AND column_name = UPPER(p_new_column);

            IF v_new_exists = 0 THEN
                EXECUTE IMMEDIATE
                    'ALTER TABLE RAG_CHUNKS ADD (' || p_new_column
                    || ' VARCHAR2(' || TO_CHAR(p_length) || '))';
            END IF;

            EXECUTE IMMEDIATE
                'UPDATE RAG_CHUNKS SET ' || p_new_column
                || ' = DBMS_LOB.SUBSTR(' || p_old_column || ', '
                || TO_CHAR(p_length) || ', 1) WHERE ' || p_new_column || ' IS NULL';
            EXECUTE IMMEDIATE
                'ALTER TABLE RAG_CHUNKS MODIFY (' || p_new_column
                || ' VARCHAR2(' || TO_CHAR(p_length) || ') NOT NULL)';
            EXECUTE IMMEDIATE 'ALTER TABLE RAG_CHUNKS DROP COLUMN ' || p_old_column;
            EXECUTE IMMEDIATE
                'ALTER TABLE RAG_CHUNKS RENAME COLUMN ' || p_new_column
                || ' TO ' || p_old_column;
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            NULL;
    END;
BEGIN
    convert_key('SOURCE_TABLE', 'SOURCE_TABLE_VC', 100);
    convert_key('SOURCE_ROW_ID', 'SOURCE_ROW_ID_VC', 1000);
END;
/
