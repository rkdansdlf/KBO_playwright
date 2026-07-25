-- Oracle port of 023_reference_integrity_foreign_keys.sql.
--
-- Existing rows are accepted with ENABLE NOVALIDATE first. The final
-- validation calls intentionally fail if the staging data still contains
-- orphan references.

DECLARE
    FUNCTION has_single_column_fk(
        p_child_table VARCHAR2,
        p_child_column VARCHAR2,
        p_parent_table VARCHAR2,
        p_parent_column VARCHAR2
    ) RETURN BOOLEAN IS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_count
          FROM user_constraints child_constraint
          JOIN user_cons_columns child_column
            ON child_column.constraint_name = child_constraint.constraint_name
           AND child_column.table_name = child_constraint.table_name
          JOIN user_constraints parent_constraint
            ON parent_constraint.constraint_name = child_constraint.r_constraint_name
           AND parent_constraint.owner = child_constraint.r_owner
          JOIN user_cons_columns parent_column
            ON parent_column.constraint_name = parent_constraint.constraint_name
           AND parent_column.table_name = parent_constraint.table_name
         WHERE child_constraint.constraint_type = 'R'
           AND child_constraint.table_name = UPPER(p_child_table)
           AND child_column.column_name = UPPER(p_child_column)
           AND child_column.position = 1
           AND parent_constraint.table_name = UPPER(p_parent_table)
           AND parent_column.column_name = UPPER(p_parent_column)
           AND parent_column.position = 1
           AND (SELECT COUNT(*)
                  FROM user_cons_columns child_column_count
                 WHERE child_column_count.constraint_name = child_constraint.constraint_name
                   AND child_column_count.table_name = child_constraint.table_name) = 1
           AND (SELECT COUNT(*)
                  FROM user_cons_columns parent_column_count
                 WHERE parent_column_count.constraint_name = parent_constraint.constraint_name
                   AND parent_column_count.table_name = parent_constraint.table_name) = 1;
        RETURN v_count > 0;
    END;

    PROCEDURE add_fk_if_missing(
        p_constraint_name VARCHAR2,
        p_child_table VARCHAR2,
        p_child_column VARCHAR2,
        p_parent_table VARCHAR2,
        p_parent_column VARCHAR2
    ) IS
        v_named_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_named_count
          FROM user_constraints
         WHERE constraint_name = UPPER(p_constraint_name)
           AND table_name = UPPER(p_child_table);
        IF v_named_count = 0 AND NOT has_single_column_fk(
            p_child_table,
            p_child_column,
            p_parent_table,
            p_parent_column
        ) THEN
            EXECUTE IMMEDIATE
                'ALTER TABLE ' || UPPER(p_child_table)
                || ' ADD CONSTRAINT ' || UPPER(p_constraint_name)
                || ' FOREIGN KEY (' || UPPER(p_child_column) || ') REFERENCES '
                || UPPER(p_parent_table) || ' (' || UPPER(p_parent_column) || ') ENABLE NOVALIDATE';
        END IF;
    END;

    PROCEDURE create_index_if_missing(p_index_name VARCHAR2, p_ddl VARCHAR2) IS
        v_exists NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_exists
          FROM user_indexes
         WHERE index_name = UPPER(p_index_name);
        IF v_exists = 0 THEN
            EXECUTE IMMEDIATE p_ddl;
        END IF;
    END;
BEGIN
    create_index_if_missing(
        'IDX_GAME_METADATA_GAME_ID',
        'CREATE INDEX IDX_GAME_METADATA_GAME_ID ON GAME_METADATA (GAME_ID)'
    );
    create_index_if_missing(
        'IDX_GAME_BATTING_STATS_GAME_ID',
        'CREATE INDEX IDX_GAME_BATTING_STATS_GAME_ID ON GAME_BATTING_STATS (GAME_ID)'
    );
    create_index_if_missing(
        'IDX_GAME_BATTING_STATS_PLAYER_ID',
        'CREATE INDEX IDX_GAME_BATTING_STATS_PLAYER_ID ON GAME_BATTING_STATS '
        || '(CASE WHEN PLAYER_ID IS NOT NULL THEN PLAYER_ID END)'
    );
    create_index_if_missing(
        'IDX_GAME_PITCHING_STATS_GAME_ID',
        'CREATE INDEX IDX_GAME_PITCHING_STATS_GAME_ID ON GAME_PITCHING_STATS (GAME_ID)'
    );
    create_index_if_missing(
        'IDX_GAME_PITCHING_STATS_PLAYER_ID',
        'CREATE INDEX IDX_GAME_PITCHING_STATS_PLAYER_ID ON GAME_PITCHING_STATS '
        || '(CASE WHEN PLAYER_ID IS NOT NULL THEN PLAYER_ID END)'
    );
    create_index_if_missing(
        'IDX_GAME_LINEUPS_GAME_ID',
        'CREATE INDEX IDX_GAME_LINEUPS_GAME_ID ON GAME_LINEUPS (GAME_ID)'
    );
    create_index_if_missing(
        'IDX_GAME_LINEUPS_PLAYER_ID',
        'CREATE INDEX IDX_GAME_LINEUPS_PLAYER_ID ON GAME_LINEUPS '
        || '(CASE WHEN PLAYER_ID IS NOT NULL THEN PLAYER_ID END)'
    );
    create_index_if_missing(
        'IDX_PLAYER_SEASON_BATTING_PLAYER_ID',
        'CREATE INDEX IDX_PLAYER_SEASON_BATTING_PLAYER_ID ON PLAYER_SEASON_BATTING (PLAYER_ID)'
    );
    create_index_if_missing(
        'IDX_PLAYER_SEASON_BATTING_TEAM_CODE',
        'CREATE INDEX IDX_PLAYER_SEASON_BATTING_TEAM_CODE ON PLAYER_SEASON_BATTING '
        || '(CASE WHEN TEAM_CODE IS NOT NULL THEN TEAM_CODE END)'
    );
    create_index_if_missing(
        'IDX_PLAYER_SEASON_PITCHING_PLAYER_ID',
        'CREATE INDEX IDX_PLAYER_SEASON_PITCHING_PLAYER_ID ON PLAYER_SEASON_PITCHING (PLAYER_ID)'
    );
    create_index_if_missing(
        'IDX_PLAYER_SEASON_PITCHING_TEAM_CODE',
        'CREATE INDEX IDX_PLAYER_SEASON_PITCHING_TEAM_CODE ON PLAYER_SEASON_PITCHING '
        || '(CASE WHEN TEAM_CODE IS NOT NULL THEN TEAM_CODE END)'
    );

    add_fk_if_missing('FK_REFINT_GAME_METADATA_GAME', 'GAME_METADATA', 'GAME_ID', 'GAME', 'GAME_ID');
    add_fk_if_missing('FK_REFINT_GAME_BATTING_STATS_GAME', 'GAME_BATTING_STATS', 'GAME_ID', 'GAME', 'GAME_ID');
    add_fk_if_missing('FK_REFINT_GAME_BATTING_STATS_PLAYER', 'GAME_BATTING_STATS', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_REFINT_GAME_PITCHING_STATS_GAME', 'GAME_PITCHING_STATS', 'GAME_ID', 'GAME', 'GAME_ID');
    add_fk_if_missing('FK_REFINT_GAME_PITCHING_STATS_PLAYER', 'GAME_PITCHING_STATS', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_REFINT_GAME_LINEUPS_GAME', 'GAME_LINEUPS', 'GAME_ID', 'GAME', 'GAME_ID');
    add_fk_if_missing('FK_REFINT_GAME_LINEUPS_PLAYER', 'GAME_LINEUPS', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_REFINT_PLAYER_SEASON_BATTING_PLAYER', 'PLAYER_SEASON_BATTING', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_REFINT_PLAYER_SEASON_BATTING_TEAM', 'PLAYER_SEASON_BATTING', 'TEAM_CODE', 'TEAMS', 'TEAM_ID');
    add_fk_if_missing('FK_REFINT_PLAYER_SEASON_PITCHING_PLAYER', 'PLAYER_SEASON_PITCHING', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_REFINT_PLAYER_SEASON_PITCHING_TEAM', 'PLAYER_SEASON_PITCHING', 'TEAM_CODE', 'TEAMS', 'TEAM_ID');
END;
/

DECLARE
    PROCEDURE validate_fk(p_table_name VARCHAR2, p_constraint_name VARCHAR2) IS
        v_exists NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_exists
          FROM user_constraints
         WHERE table_name = UPPER(p_table_name)
           AND constraint_name = UPPER(p_constraint_name)
           AND constraint_type = 'R';
        IF v_exists = 1 THEN
            EXECUTE IMMEDIATE
                'ALTER TABLE ' || UPPER(p_table_name)
                || ' ENABLE VALIDATE CONSTRAINT ' || UPPER(p_constraint_name);
        END IF;
    END;
BEGIN
    validate_fk('GAME_METADATA', 'FK_REFINT_GAME_METADATA_GAME');
    validate_fk('GAME_BATTING_STATS', 'FK_REFINT_GAME_BATTING_STATS_GAME');
    validate_fk('GAME_BATTING_STATS', 'FK_REFINT_GAME_BATTING_STATS_PLAYER');
    validate_fk('GAME_PITCHING_STATS', 'FK_REFINT_GAME_PITCHING_STATS_GAME');
    validate_fk('GAME_PITCHING_STATS', 'FK_REFINT_GAME_PITCHING_STATS_PLAYER');
    validate_fk('GAME_LINEUPS', 'FK_REFINT_GAME_LINEUPS_GAME');
    validate_fk('GAME_LINEUPS', 'FK_REFINT_GAME_LINEUPS_PLAYER');
    validate_fk('PLAYER_SEASON_BATTING', 'FK_REFINT_PLAYER_SEASON_BATTING_PLAYER');
    validate_fk('PLAYER_SEASON_BATTING', 'FK_REFINT_PLAYER_SEASON_BATTING_TEAM');
    validate_fk('PLAYER_SEASON_PITCHING', 'FK_REFINT_PLAYER_SEASON_PITCHING_PLAYER');
    validate_fk('PLAYER_SEASON_PITCHING', 'FK_REFINT_PLAYER_SEASON_PITCHING_TEAM');
END;
/
