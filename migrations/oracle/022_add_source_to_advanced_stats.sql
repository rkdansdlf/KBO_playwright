-- Oracle port of 022_add_source_to_advanced_stats.sql.
-- The migration runner must execute this PL/SQL block as one statement.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_exists
      FROM user_tab_columns
     WHERE table_name = 'PLAYER_SEASON_FIELDING'
       AND column_name = 'SOURCE';
    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE PLAYER_SEASON_FIELDING ADD (SOURCE VARCHAR2(20) DEFAULT ''CRAWLER'')';
    END IF;

    SELECT COUNT(*) INTO v_exists
      FROM user_tab_columns
     WHERE table_name = 'PLAYER_SEASON_BASERUNNING'
       AND column_name = 'SOURCE';
    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE PLAYER_SEASON_BASERUNNING ADD (SOURCE VARCHAR2(20) DEFAULT ''CRAWLER'')';
    END IF;
END;
/
