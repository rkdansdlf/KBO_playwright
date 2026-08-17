-- Oracle port of 024_increase_pitching_string_lengths.sql.
-- Older OCI schemas may predate the model's LEVEL and SOURCE columns. Add
-- those columns with the model defaults before widening the text contract.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_tab_columns
     WHERE table_name = 'PLAYER_SEASON_PITCHING'
       AND column_name IN ('LEVEL', 'level');
    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE PLAYER_SEASON_PITCHING '
            || 'ADD ("level" VARCHAR2(50) DEFAULT ''KBO1'' NOT NULL)';
    END IF;

    SELECT COUNT(*)
      INTO v_exists
      FROM user_tab_columns
     WHERE table_name = 'PLAYER_SEASON_PITCHING'
       AND column_name = 'SOURCE';
    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE PLAYER_SEASON_PITCHING '
            || 'ADD (SOURCE VARCHAR2(50) DEFAULT ''CRAWLER'' NOT NULL)';
    END IF;

    EXECUTE IMMEDIATE
        'ALTER TABLE PLAYER_SEASON_PITCHING MODIFY '
        || '(LEAGUE VARCHAR2(50), "level" VARCHAR2(50), SOURCE VARCHAR2(50))';
END;
/
