-- Keep game play-by-play batter names aligned with current SQLite values.
DECLARE
    v_data_type VARCHAR2(128);
    v_char_length NUMBER;
BEGIN
    SELECT data_type, char_length
      INTO v_data_type, v_char_length
      FROM user_tab_columns
     WHERE table_name = 'GAME_PLAY_BY_PLAY'
       AND column_name = 'BATTER_NAME';

    IF v_data_type = 'VARCHAR2' AND v_char_length < 100 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE GAME_PLAY_BY_PLAY MODIFY (BATTER_NAME VARCHAR2(100 CHAR))';
    END IF;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        NULL;
END;
/
