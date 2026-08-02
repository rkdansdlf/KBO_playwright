-- Oracle port of 045_additional_performance_indexes.sql.
DECLARE
    v_exists NUMBER;

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
    create_index_if_missing('IDX_GAME_EVENTS_GAME_ID', 'CREATE INDEX IDX_GAME_EVENTS_GAME_ID ON GAME_EVENTS (GAME_ID)');
    create_index_if_missing(
        'IDX_GAME_INNING_SCORES_GAME_ID',
        'CREATE INDEX IDX_GAME_INNING_SCORES_GAME_ID ON GAME_INNING_SCORES (GAME_ID)'
    );
    create_index_if_missing(
        'IDX_PLAYER_SEASON_BATTING_SEASON',
        'CREATE INDEX IDX_PLAYER_SEASON_BATTING_SEASON ON PLAYER_SEASON_BATTING (SEASON)'
    );
    create_index_if_missing(
        'IDX_PLAYER_SEASON_PITCHING_SEASON',
        'CREATE INDEX IDX_PLAYER_SEASON_PITCHING_SEASON ON PLAYER_SEASON_PITCHING (SEASON)'
    );
END;
/
