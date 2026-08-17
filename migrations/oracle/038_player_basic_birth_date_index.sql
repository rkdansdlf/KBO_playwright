-- Oracle port of 038_player_basic_birth_date_index.sql.
DECLARE
    v_exists NUMBER;

    PROCEDURE add_column_if_missing(p_column_name VARCHAR2, p_ddl VARCHAR2) IS
    BEGIN
        SELECT COUNT(*) INTO v_exists
          FROM user_tab_columns
         WHERE table_name = 'PLAYER_BASIC'
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
    add_column_if_missing(
        'BIRTH_DATE_DATE',
        'ALTER TABLE PLAYER_BASIC ADD (BIRTH_DATE_DATE DATE)'
    );
    create_index_if_missing('IDX_PLAYER_BASIC_NAME', 'CREATE INDEX IDX_PLAYER_BASIC_NAME ON PLAYER_BASIC (NAME)');
    create_index_if_missing('IDX_PLAYER_BASIC_TEAM', 'CREATE INDEX IDX_PLAYER_BASIC_TEAM ON PLAYER_BASIC (TEAM)');
    create_index_if_missing('IDX_PLAYER_BASIC_POSITION', 'CREATE INDEX IDX_PLAYER_BASIC_POSITION ON PLAYER_BASIC (POSITION)');
    create_index_if_missing(
        'IDX_PLAYER_BASIC_TEAM_POS',
        'CREATE INDEX IDX_PLAYER_BASIC_TEAM_POS ON PLAYER_BASIC (TEAM, POSITION)'
    );
END;
/
