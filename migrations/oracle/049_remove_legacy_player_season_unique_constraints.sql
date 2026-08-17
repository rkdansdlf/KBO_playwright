-- Oracle port of 049_remove_legacy_player_season_unique_constraints.sql.
DECLARE
    PROCEDURE drop_constraint_if_exists(p_table_name VARCHAR2, p_constraint_name VARCHAR2) IS
        v_exists NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_exists
          FROM user_constraints
         WHERE table_name = UPPER(p_table_name)
           AND constraint_name = UPPER(p_constraint_name);
        IF v_exists = 1 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE ' || UPPER(p_table_name)
                || ' DROP CONSTRAINT ' || UPPER(p_constraint_name);
        END IF;
    END;
BEGIN
    drop_constraint_if_exists('PLAYER_SEASON_BATTING', 'UQ_PLAYER_SEASON_BATTING_NEW');
    drop_constraint_if_exists('PLAYER_SEASON_PITCHING', 'UQ_PLAYER_SEASON_PITCHING_IDX');
END;
/
