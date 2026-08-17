-- Oracle port of 047_remove_redundant_phase1_indexes.sql.
DECLARE
    PROCEDURE drop_index_if_exists(p_index_name VARCHAR2) IS
        v_exists NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_exists
          FROM user_indexes
         WHERE index_name = UPPER(p_index_name);
        IF v_exists = 1 THEN
            EXECUTE IMMEDIATE 'DROP INDEX ' || UPPER(p_index_name);
        END IF;
    END;
BEGIN
    drop_index_if_exists('IX_GAME_BROADCASTS_GAME_ID');
    drop_index_if_exists('IX_CHEER_SONGS_TEAM_ID');
    drop_index_if_exists('IX_CHEER_CHANTS_TEAM_ID');
    drop_index_if_exists('IX_FOREIGN_PLAYER_CHANGES_TEAM_ID');
    drop_index_if_exists('IX_FOREIGN_PLAYER_CHANGES_PLAYER_ID');
    drop_index_if_exists('IX_GAME_MVPS_GAME_ID');
    drop_index_if_exists('IX_GAME_MVPS_PLAYER_ID');
    drop_index_if_exists('IX_INJURY_ENTRIES_TEAM_ID');
    drop_index_if_exists('IX_INJURY_ENTRIES_PLAYER_ID');
    drop_index_if_exists('IX_MANAGER_CHANGES_TEAM_ID');
END;
/
