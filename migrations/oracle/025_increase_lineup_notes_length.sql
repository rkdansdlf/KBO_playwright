-- Oracle port of 025_increase_lineup_notes_length.sql.
DECLARE
    v_data_type VARCHAR2(128);
    v_data_length NUMBER;
BEGIN
    SELECT data_type, data_length
      INTO v_data_type, v_data_length
      FROM user_tab_columns
     WHERE table_name = 'GAME_LINEUPS'
       AND column_name = 'NOTES';

    IF v_data_type = 'VARCHAR2' AND v_data_length < 512 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE GAME_LINEUPS MODIFY (NOTES VARCHAR2(512))';
    END IF;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        NULL;
END;
/
