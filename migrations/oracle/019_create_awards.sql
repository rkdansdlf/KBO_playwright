-- Oracle port of 019_create_awards.sql.
--
-- OCI may contain the legacy AWARDS shape (AWARD_YEAR/POSITION). Add the
-- current model columns without dropping the legacy columns, then backfill
-- values that have an unambiguous mapping.
DECLARE
    v_exists NUMBER;

    PROCEDURE add_column_if_missing(p_table_name VARCHAR2, p_column_name VARCHAR2, p_ddl VARCHAR2) IS
    BEGIN
        SELECT COUNT(*) INTO v_exists
          FROM user_tab_columns
         WHERE table_name = UPPER(p_table_name)
           AND column_name = UPPER(p_column_name);
        IF v_exists = 0 THEN
            EXECUTE IMMEDIATE p_ddl;
        END IF;
    END;
BEGIN
    add_column_if_missing('AWARDS', 'YEAR', 'ALTER TABLE AWARDS ADD (YEAR NUMBER(4))');
    add_column_if_missing('AWARDS', 'CATEGORY', 'ALTER TABLE AWARDS ADD (CATEGORY VARCHAR2(50))');
    add_column_if_missing('AWARDS', 'TEAM_NAME', 'ALTER TABLE AWARDS ADD (TEAM_NAME VARCHAR2(50))');
    add_column_if_missing(
        'AWARDS',
        'CREATED_AT',
        'ALTER TABLE AWARDS ADD (CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'
    );
    add_column_if_missing(
        'AWARDS',
        'UPDATED_AT',
        'ALTER TABLE AWARDS ADD (UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'
    );
END;
/

DECLARE
    v_award_year_exists NUMBER;
    v_position_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_award_year_exists
      FROM user_tab_columns
     WHERE table_name = 'AWARDS'
       AND column_name = 'AWARD_YEAR';
    IF v_award_year_exists > 0 THEN
        EXECUTE IMMEDIATE
            'UPDATE AWARDS '
            || 'SET YEAR = AWARD_YEAR '
            || 'WHERE YEAR IS NULL AND AWARD_YEAR IS NOT NULL';
    END IF;

    SELECT COUNT(*)
      INTO v_position_exists
      FROM user_tab_columns
     WHERE table_name = 'AWARDS'
       AND column_name = 'POSITION';
    IF v_position_exists > 0 THEN
        EXECUTE IMMEDIATE
            'UPDATE AWARDS '
            || 'SET CATEGORY = POSITION '
            || 'WHERE CATEGORY IS NULL AND POSITION IS NOT NULL';
    END IF;
END;
/
/

DECLARE
    v_exists NUMBER;
    v_missing NUMBER;

    PROCEDURE create_index_if_missing(p_index_name VARCHAR2, p_ddl VARCHAR2) IS
    BEGIN
        SELECT COUNT(*) INTO v_exists
          FROM user_indexes
         WHERE index_name = UPPER(p_index_name);
        IF v_exists = 0 THEN
            BEGIN
                EXECUTE IMMEDIATE p_ddl;
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1408 THEN
                        RAISE;
                    END IF;
            END;
        END IF;
    END;
BEGIN
    SELECT COUNT(*) INTO v_missing
      FROM AWARDS
     WHERE YEAR IS NULL OR TEAM_NAME IS NULL;
    IF v_missing > 0 THEN
        RAISE_APPLICATION_ERROR(
            -20019,
            'AWARDS has rows missing required YEAR or TEAM_NAME values: ' || v_missing
        );
    END IF;

    create_index_if_missing('IDX_AWARD_YEAR', 'CREATE INDEX IDX_AWARD_YEAR ON AWARDS (YEAR)');
    create_index_if_missing('IDX_AWARD_TYPE', 'CREATE INDEX IDX_AWARD_TYPE ON AWARDS (AWARD_TYPE)');
    create_index_if_missing('IDX_AWARD_PLAYER', 'CREATE INDEX IDX_AWARD_PLAYER ON AWARDS (PLAYER_NAME)');
END;
/
