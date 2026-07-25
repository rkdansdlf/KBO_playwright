-- Oracle port of 048_add_team_code_to_player_season_unique.sql.
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

    PROCEDURE add_constraint_if_missing(
        p_table_name VARCHAR2,
        p_constraint_name VARCHAR2,
        p_columns VARCHAR2
    ) IS
        v_exists NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_exists
          FROM user_constraints
         WHERE table_name = UPPER(p_table_name)
           AND constraint_name = UPPER(p_constraint_name);
        IF v_exists = 0 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE ' || UPPER(p_table_name)
                || ' ADD CONSTRAINT ' || UPPER(p_constraint_name)
                || ' UNIQUE (' || p_columns || ')';
        END IF;
    END;
BEGIN
    drop_constraint_if_exists('PLAYER_SEASON_BATTING', 'UQ_PLAYER_SEASON_BATTING');
    drop_constraint_if_exists('PLAYER_SEASON_BATTING', 'UQ_PLAYER_SEASON_BATTING_TEAM');
    add_constraint_if_missing(
        'PLAYER_SEASON_BATTING',
        'UQ_PLAYER_SEASON_BATTING_TEAM',
        'PLAYER_ID, SEASON, LEAGUE, LEVEL, TEAM_CODE'
    );

    drop_constraint_if_exists('PLAYER_SEASON_PITCHING', 'UQ_PLAYER_SEASON_PITCHING');
    drop_constraint_if_exists('PLAYER_SEASON_PITCHING', 'UQ_PLAYER_SEASON_PITCHING_TEAM');
    add_constraint_if_missing(
        'PLAYER_SEASON_PITCHING',
        'UQ_PLAYER_SEASON_PITCHING_TEAM',
        'PLAYER_ID, SEASON, LEAGUE, LEVEL, TEAM_CODE'
    );
END;
/
