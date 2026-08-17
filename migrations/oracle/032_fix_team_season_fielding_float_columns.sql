-- Oracle port of 032_fix_team_season_fielding_float_columns.sql.
--
-- This is intentionally not applied with the other DDL batch. It changes
-- column types and requires a separate data-preservation review.
DECLARE
    v_type USER_TAB_COLUMNS.DATA_TYPE%TYPE;
    v_scale USER_TAB_COLUMNS.DATA_SCALE%TYPE;

    PROCEDURE convert_integer_to_float(p_table_name VARCHAR2, p_column_name VARCHAR2) IS
    BEGIN
        SELECT data_type, data_scale
          INTO v_type, v_scale
          FROM user_tab_columns
         WHERE table_name = UPPER(p_table_name)
           AND column_name = UPPER(p_column_name);
        IF v_type = 'NUMBER' AND NVL(v_scale, 0) = 0 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE ' || UPPER(p_table_name)
                || ' MODIFY (' || UPPER(p_column_name) || ' FLOAT)';
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            NULL;
    END;
BEGIN
    convert_integer_to_float('TEAM_SEASON_FIELDING', 'DEF_INNINGS');
    convert_integer_to_float('TEAM_SEASON_FIELDING', 'FIELDING_PCT');
    convert_integer_to_float('TEAM_SEASON_FIELDING', 'RANGE_FACTOR_PER_GAME');
    convert_integer_to_float('TEAM_SEASON_BASERUNNING', 'SB_SUCCESS_RATE');
END;
/
