-- Oracle port of 024_game_stat_partial_unique_indexes.sql.
--
-- Oracle has no partial-index WHERE clause. Function-based unique indexes
-- return NULL for rows whose player_id is NULL, so those rows are omitted from
-- the uniqueness key while resolved player rows remain unique.

DECLARE
    v_duplicates NUMBER;
    v_exists NUMBER;

    PROCEDURE assert_no_duplicates(p_table_name VARCHAR2) IS
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT COUNT(*) FROM ('
            || 'SELECT GAME_ID, PLAYER_ID FROM ' || p_table_name
            || ' WHERE PLAYER_ID IS NOT NULL GROUP BY GAME_ID, PLAYER_ID HAVING COUNT(*) > 1)'
            INTO v_duplicates;
        IF v_duplicates > 0 THEN
            RAISE_APPLICATION_ERROR(
                -20024,
                p_table_name || ' contains duplicate non-null (GAME_ID, PLAYER_ID) rows: '
                || v_duplicates
            );
        END IF;
    END;

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
    assert_no_duplicates('GAME_BATTING_STATS');
    assert_no_duplicates('GAME_PITCHING_STATS');
    assert_no_duplicates('GAME_LINEUPS');

    create_index_if_missing(
        'UQ_GAME_BATTING_STATS_GAME_PLAYER_NONNULL',
        'CREATE UNIQUE INDEX UQ_GAME_BATTING_STATS_GAME_PLAYER_NONNULL '
        || 'ON GAME_BATTING_STATS ('
        || 'CASE WHEN PLAYER_ID IS NOT NULL THEN GAME_ID END, '
        || 'CASE WHEN PLAYER_ID IS NOT NULL THEN PLAYER_ID END)'
    );
    create_index_if_missing(
        'UQ_GAME_PITCHING_STATS_GAME_PLAYER_NONNULL',
        'CREATE UNIQUE INDEX UQ_GAME_PITCHING_STATS_GAME_PLAYER_NONNULL '
        || 'ON GAME_PITCHING_STATS ('
        || 'CASE WHEN PLAYER_ID IS NOT NULL THEN GAME_ID END, '
        || 'CASE WHEN PLAYER_ID IS NOT NULL THEN PLAYER_ID END)'
    );
    create_index_if_missing(
        'UQ_GAME_LINEUPS_GAME_PLAYER_NONNULL',
        'CREATE UNIQUE INDEX UQ_GAME_LINEUPS_GAME_PLAYER_NONNULL '
        || 'ON GAME_LINEUPS ('
        || 'CASE WHEN PLAYER_ID IS NOT NULL THEN GAME_ID END, '
        || 'CASE WHEN PLAYER_ID IS NOT NULL THEN PLAYER_ID END)'
    );
END;
/
