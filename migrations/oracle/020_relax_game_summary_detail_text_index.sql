-- Oracle port of 020_relax_game_summary_detail_text_index.sql.
DECLARE
    v_exists NUMBER;

    PROCEDURE drop_constraint_if_exists(p_name VARCHAR2) IS
    BEGIN
        SELECT COUNT(*) INTO v_exists
          FROM user_constraints
         WHERE table_name = 'GAME_SUMMARY'
           AND constraint_name = UPPER(p_name);
        IF v_exists = 1 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE GAME_SUMMARY DROP CONSTRAINT ' || UPPER(p_name);
        END IF;
    END;

    PROCEDURE drop_index_if_exists(p_name VARCHAR2) IS
    BEGIN
        SELECT COUNT(*) INTO v_exists
          FROM user_indexes
         WHERE index_name = UPPER(p_name);
        IF v_exists = 1 THEN
            EXECUTE IMMEDIATE 'DROP INDEX ' || UPPER(p_name);
        END IF;
    END;

    PROCEDURE create_index_if_missing(p_name VARCHAR2, p_ddl VARCHAR2) IS
    BEGIN
        SELECT COUNT(*) INTO v_exists
          FROM user_indexes
         WHERE index_name = UPPER(p_name);
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
    drop_constraint_if_exists('UQ_GAME_SUMMARY');
    drop_index_if_exists('UQ_GAME_SUMMARY');
    create_index_if_missing(
        'IDX_GAME_SUMMARY_LOOKUP',
        'CREATE INDEX IDX_GAME_SUMMARY_LOOKUP ON GAME_SUMMARY (GAME_ID, SUMMARY_TYPE, PLAYER_NAME)'
    );
END;
/
