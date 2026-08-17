-- Oracle port of 040_add_player_parsed_profile_fields.sql.
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
    add_column_if_missing('PLAYER_BASIC', 'SALARY_AMOUNT', 'ALTER TABLE PLAYER_BASIC ADD (SALARY_AMOUNT NUMBER(19))');
    add_column_if_missing('PLAYER_BASIC', 'SALARY_CURRENCY', 'ALTER TABLE PLAYER_BASIC ADD (SALARY_CURRENCY VARCHAR2(8))');
    add_column_if_missing('PLAYER_BASIC', 'SIGNING_BONUS_AMOUNT', 'ALTER TABLE PLAYER_BASIC ADD (SIGNING_BONUS_AMOUNT NUMBER(19))');
    add_column_if_missing('PLAYER_BASIC', 'SIGNING_BONUS_CURRENCY', 'ALTER TABLE PLAYER_BASIC ADD (SIGNING_BONUS_CURRENCY VARCHAR2(8))');
    add_column_if_missing('PLAYER_BASIC', 'DRAFT_YEAR', 'ALTER TABLE PLAYER_BASIC ADD (DRAFT_YEAR NUMBER(4))');
    add_column_if_missing('PLAYER_BASIC', 'DRAFT_ROUND', 'ALTER TABLE PLAYER_BASIC ADD (DRAFT_ROUND NUMBER(4))');
    add_column_if_missing('PLAYER_BASIC', 'DRAFT_PICK_OVERALL', 'ALTER TABLE PLAYER_BASIC ADD (DRAFT_PICK_OVERALL NUMBER(10))');
    add_column_if_missing('PLAYER_BASIC', 'DRAFT_TYPE', 'ALTER TABLE PLAYER_BASIC ADD (DRAFT_TYPE VARCHAR2(32))');
    add_column_if_missing('PLAYER_BASIC', 'EDUCATION_PATH', 'ALTER TABLE PLAYER_BASIC ADD (EDUCATION_PATH CLOB)');

    add_column_if_missing('PLAYERS', 'SALARY_AMOUNT', 'ALTER TABLE PLAYERS ADD (SALARY_AMOUNT NUMBER(19))');
    add_column_if_missing('PLAYERS', 'SALARY_CURRENCY', 'ALTER TABLE PLAYERS ADD (SALARY_CURRENCY VARCHAR2(8))');
    add_column_if_missing('PLAYERS', 'SIGNING_BONUS_AMOUNT', 'ALTER TABLE PLAYERS ADD (SIGNING_BONUS_AMOUNT NUMBER(19))');
    add_column_if_missing('PLAYERS', 'SIGNING_BONUS_CURRENCY', 'ALTER TABLE PLAYERS ADD (SIGNING_BONUS_CURRENCY VARCHAR2(8))');
    add_column_if_missing('PLAYERS', 'DRAFT_YEAR', 'ALTER TABLE PLAYERS ADD (DRAFT_YEAR NUMBER(4))');
    add_column_if_missing('PLAYERS', 'DRAFT_ROUND', 'ALTER TABLE PLAYERS ADD (DRAFT_ROUND NUMBER(4))');
    add_column_if_missing('PLAYERS', 'DRAFT_PICK_OVERALL', 'ALTER TABLE PLAYERS ADD (DRAFT_PICK_OVERALL NUMBER(10))');
    add_column_if_missing('PLAYERS', 'DRAFT_TYPE', 'ALTER TABLE PLAYERS ADD (DRAFT_TYPE VARCHAR2(32))');
    add_column_if_missing('PLAYERS', 'EDUCATION_PATH', 'ALTER TABLE PLAYERS ADD (EDUCATION_PATH CLOB)');

    create_index_if_missing(
        'IDX_PLAYER_BASIC_DRAFT_YEAR',
        'CREATE INDEX IDX_PLAYER_BASIC_DRAFT_YEAR ON PLAYER_BASIC (DRAFT_YEAR)'
    );
    create_index_if_missing(
        'IDX_PLAYERS_DRAFT_YEAR',
        'CREATE INDEX IDX_PLAYERS_DRAFT_YEAR ON PLAYERS (DRAFT_YEAR)'
    );
END;
/
