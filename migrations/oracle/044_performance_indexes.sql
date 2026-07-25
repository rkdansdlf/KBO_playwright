-- Oracle port of 044_performance_indexes.sql.
-- Nullable PostgreSQL partial indexes are represented as function-based
-- indexes whose expressions return NULL for unresolved rows.
DECLARE
    v_exists NUMBER;

    PROCEDURE create_index_if_missing(p_index_name VARCHAR2, p_ddl VARCHAR2) IS
    BEGIN
        SELECT COUNT(*) INTO v_exists
          FROM user_indexes
         WHERE index_name = UPPER(p_index_name);
        IF v_exists = 0 THEN
            EXECUTE IMMEDIATE p_ddl;
        END IF;
    END;
BEGIN
    create_index_if_missing('IDX_GAME_GAME_DATE', 'CREATE INDEX IDX_GAME_GAME_DATE ON GAME (GAME_DATE)');
    create_index_if_missing('IDX_GAME_SEASON_ID', 'CREATE INDEX IDX_GAME_SEASON_ID ON GAME (SEASON_ID)');
    create_index_if_missing(
        'IDX_PLAYER_GAME_BATTING_PLAYER_ID',
        'CREATE INDEX IDX_PLAYER_GAME_BATTING_PLAYER_ID ON PLAYER_GAME_BATTING (PLAYER_ID)'
    );
    create_index_if_missing(
        'IDX_PLAYER_GAME_PITCHING_PLAYER_ID',
        'CREATE INDEX IDX_PLAYER_GAME_PITCHING_PLAYER_ID ON PLAYER_GAME_PITCHING (PLAYER_ID)'
    );
    create_index_if_missing(
        'IDX_GAME_EVENTS_BATTER_ID',
        'CREATE INDEX IDX_GAME_EVENTS_BATTER_ID ON GAME_EVENTS '
        || '(CASE WHEN BATTER_ID IS NOT NULL THEN BATTER_ID END)'
    );
    create_index_if_missing(
        'IDX_GAME_EVENTS_PITCHER_ID',
        'CREATE INDEX IDX_GAME_EVENTS_PITCHER_ID ON GAME_EVENTS '
        || '(CASE WHEN PITCHER_ID IS NOT NULL THEN PITCHER_ID END)'
    );
    create_index_if_missing(
        'IDX_GAME_SUMMARY_PLAYER_ID',
        'CREATE INDEX IDX_GAME_SUMMARY_PLAYER_ID ON GAME_SUMMARY '
        || '(CASE WHEN PLAYER_ID IS NOT NULL THEN PLAYER_ID END)'
    );
    create_index_if_missing(
        'IDX_GAME_PLAY_BY_PLAY_PLAYER_ID',
        'CREATE INDEX IDX_GAME_PLAY_BY_PLAY_PLAYER_ID ON GAME_PLAY_BY_PLAY '
        || '(CASE WHEN PLAYER_ID IS NOT NULL THEN PLAYER_ID END)'
    );
END;
/
