-- Use character length rather than byte length for OPEN_TIME on multibyte DBs.
DECLARE
    v_data_type VARCHAR2(128);
    v_char_length NUMBER;
BEGIN
    SELECT data_type, char_length
      INTO v_data_type, v_char_length
      FROM user_tab_columns
     WHERE table_name = 'TICKET_OPEN_RULES'
       AND column_name = 'OPEN_TIME';

    IF v_data_type = 'VARCHAR2' AND v_char_length < 32 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE TICKET_OPEN_RULES MODIFY (OPEN_TIME VARCHAR2(32 CHAR))';
    END IF;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        NULL;
END;
/
