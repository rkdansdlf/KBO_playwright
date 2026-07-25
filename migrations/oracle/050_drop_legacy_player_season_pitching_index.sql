-- Oracle port of 050_drop_legacy_player_season_pitching_index.sql.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_exists
      FROM user_indexes
     WHERE index_name = 'UQ_PLAYER_SEASON_PITCHING_IDX';
    IF v_exists = 1 THEN
        EXECUTE IMMEDIATE 'DROP INDEX UQ_PLAYER_SEASON_PITCHING_IDX';
    END IF;
END;
/
