-- Keep the batting source column aligned with the current SQLite text values.
DECLARE
    v_data_type VARCHAR2(128);
    v_char_length NUMBER;
BEGIN
    SELECT data_type, char_length
      INTO v_data_type, v_char_length
      FROM user_tab_columns
     WHERE table_name = 'PLAYER_SEASON_BATTING'
       AND column_name = 'SOURCE';

    IF v_data_type = 'VARCHAR2' AND v_char_length < 50 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE PLAYER_SEASON_BATTING MODIFY (SOURCE VARCHAR2(50 CHAR))';
    END IF;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        NULL;
END;
/
