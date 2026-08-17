-- Preserve SQLite TIME values that include fractional seconds.
DECLARE
    v_data_type VARCHAR2(128);
    v_char_length NUMBER;
BEGIN
    FOR column_rec IN (
        SELECT 'START_TIME' AS name FROM dual
        UNION ALL
        SELECT 'END_TIME' AS name FROM dual
    ) LOOP
        SELECT data_type, char_length
          INTO v_data_type, v_char_length
          FROM user_tab_columns
         WHERE table_name = 'GAME_METADATA'
           AND column_name = column_rec.name;

        IF v_data_type = 'VARCHAR2' AND v_char_length < 32 THEN
            EXECUTE IMMEDIATE
                'ALTER TABLE GAME_METADATA MODIFY (' || column_rec.name || ' VARCHAR2(32 CHAR))';
        END IF;
    END LOOP;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        NULL;
END;
/
