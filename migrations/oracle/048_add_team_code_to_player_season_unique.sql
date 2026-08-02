-- Oracle port of 048_add_team_code_to_player_season_unique.sql.
DECLARE
    v_duplicates NUMBER;

    PROCEDURE assert_no_duplicates(p_table_name VARCHAR2) IS
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT COUNT(*) FROM ('
            || 'SELECT PLAYER_ID, SEASON, LEAGUE, "level", TEAM_CODE FROM '
            || p_table_name
            || ' GROUP BY PLAYER_ID, SEASON, LEAGUE, "level", TEAM_CODE '
            || 'HAVING COUNT(*) > 1)'
            INTO v_duplicates;
        IF v_duplicates > 0 THEN
            RAISE_APPLICATION_ERROR(
                -20048,
                p_table_name || ' contains duplicate season/team rows: ' || v_duplicates
            );
        END IF;
    END;

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
    assert_no_duplicates('PLAYER_SEASON_BATTING');
    drop_constraint_if_exists('PLAYER_SEASON_BATTING', 'UQ_PLAYER_SEASON_BATTING');
    drop_constraint_if_exists('PLAYER_SEASON_BATTING', 'UQ_PLAYER_SEASON_BATTING_TEAM');
    add_constraint_if_missing(
        'PLAYER_SEASON_BATTING',
        'UQ_PLAYER_SEASON_BATTING_TEAM',
        'PLAYER_ID, SEASON, LEAGUE, "level", TEAM_CODE'
    );

    assert_no_duplicates('PLAYER_SEASON_PITCHING');
    drop_constraint_if_exists('PLAYER_SEASON_PITCHING', 'UQ_PLAYER_SEASON_PITCHING');
    drop_constraint_if_exists('PLAYER_SEASON_PITCHING', 'UQ_PLAYER_SEASON_PITCHING_TEAM');
    add_constraint_if_missing(
        'PLAYER_SEASON_PITCHING',
        'UQ_PLAYER_SEASON_PITCHING_TEAM',
        'PLAYER_ID, SEASON, LEAGUE, "level", TEAM_CODE'
    );
END;
/
