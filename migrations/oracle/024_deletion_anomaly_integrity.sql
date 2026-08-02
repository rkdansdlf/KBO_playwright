-- DRAFT ONLY: Oracle port of 024_deletion_anomaly_integrity.sql.
--
-- This file is intentionally blocked by the safety gate below. Remove the
-- gate only after an Oracle schema backup/restore point, a reviewed orphan
-- census, and explicit approval for the data rewrite have been recorded.
-- Oracle DDL auto-commits; this is not an automatically reversible migration.
BEGIN
    RAISE_APPLICATION_ERROR(
        -20924,
        '024 deletion-anomaly integrity draft is blocked pending explicit approval'
    );
END;
/

-- Add the columns required by the canonical identity and team resolution.
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
    add_column_if_missing(
        'PLAYERS',
        'PLAYER_BASIC_ID',
        'ALTER TABLE PLAYERS ADD (PLAYER_BASIC_ID NUMBER(10))'
    );
    add_column_if_missing(
        'TEAM_DAILY_ROSTER',
        'PLAYER_BASIC_ID',
        'ALTER TABLE TEAM_DAILY_ROSTER ADD (PLAYER_BASIC_ID NUMBER(10))'
    );
    add_column_if_missing(
        'TEAM_DAILY_ROSTER',
        'PERSON_TYPE',
        'ALTER TABLE TEAM_DAILY_ROSTER ADD (PERSON_TYPE VARCHAR2(16) DEFAULT ''player'' NOT NULL)'
    );
    add_column_if_missing(
        'PLAYER_MOVEMENTS',
        'CANONICAL_TEAM_ID',
        'ALTER TABLE PLAYER_MOVEMENTS ADD (CANONICAL_TEAM_ID VARCHAR2(10))'
    );
    add_column_if_missing(
        'PLAYER_MOVEMENTS',
        'PLAYER_BASIC_ID',
        'ALTER TABLE PLAYER_MOVEMENTS ADD (PLAYER_BASIC_ID NUMBER(10))'
    );
    add_column_if_missing(
        'PLAYER_MOVEMENTS',
        'RESOLUTION_STATUS',
        'ALTER TABLE PLAYER_MOVEMENTS ADD (RESOLUTION_STATUS VARCHAR2(24) DEFAULT ''unresolved'' NOT NULL)'
    );
END;
/

-- Remove matching single-column foreign keys before canonical backfills.
DECLARE
    PROCEDURE drop_single_column_fk(
        p_child_table VARCHAR2,
        p_child_column VARCHAR2,
        p_parent_table VARCHAR2,
        p_parent_column VARCHAR2
    ) IS
    BEGIN
        FOR fk IN (
            SELECT child_constraint.constraint_name
              FROM user_constraints child_constraint
              JOIN user_cons_columns child_column
                ON child_column.constraint_name = child_constraint.constraint_name
               AND child_column.table_name = child_constraint.table_name
              JOIN user_constraints parent_constraint
                ON parent_constraint.constraint_name = child_constraint.r_constraint_name
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
                      FROM user_cons_columns c
                     WHERE c.constraint_name = child_constraint.constraint_name
                       AND c.table_name = child_constraint.table_name) = 1
               AND (SELECT COUNT(*)
                      FROM user_cons_columns c
                     WHERE c.constraint_name = parent_constraint.constraint_name
                       AND c.table_name = parent_constraint.table_name) = 1
        ) LOOP
            EXECUTE IMMEDIATE 'ALTER TABLE ' || UPPER(p_child_table)
                || ' DROP CONSTRAINT ' || fk.constraint_name;
        END LOOP;
    END;
BEGIN
    drop_single_column_fk('PLAYERS', 'PLAYER_BASIC_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    drop_single_column_fk('TEAM_DAILY_ROSTER', 'TEAM_CODE', 'TEAMS', 'TEAM_ID');
    drop_single_column_fk('TEAM_DAILY_ROSTER', 'PLAYER_BASIC_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    drop_single_column_fk('PLAYER_MOVEMENTS', 'CANONICAL_TEAM_ID', 'TEAMS', 'TEAM_ID');
    drop_single_column_fk('PLAYER_MOVEMENTS', 'PLAYER_BASIC_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    drop_single_column_fk('GAME_ID_ALIASES', 'CANONICAL_GAME_ID', 'GAME', 'GAME_ID');
    drop_single_column_fk('GAME_METADATA', 'GAME_ID', 'GAME', 'GAME_ID');
    drop_single_column_fk('GAME_INNING_SCORES', 'GAME_ID', 'GAME', 'GAME_ID');
    drop_single_column_fk('GAME_LINEUPS', 'GAME_ID', 'GAME', 'GAME_ID');
    drop_single_column_fk('GAME_LINEUPS', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    drop_single_column_fk('GAME_BATTING_STATS', 'GAME_ID', 'GAME', 'GAME_ID');
    drop_single_column_fk('GAME_BATTING_STATS', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    drop_single_column_fk('GAME_PITCHING_STATS', 'GAME_ID', 'GAME', 'GAME_ID');
    drop_single_column_fk('GAME_PITCHING_STATS', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    drop_single_column_fk('GAME_EVENTS', 'GAME_ID', 'GAME', 'GAME_ID');
    drop_single_column_fk('GAME_EVENTS', 'BATTER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    drop_single_column_fk('GAME_EVENTS', 'PITCHER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    drop_single_column_fk('GAME_SUMMARY', 'GAME_ID', 'GAME', 'GAME_ID');
    drop_single_column_fk('GAME_SUMMARY', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    drop_single_column_fk('GAME_PLAY_BY_PLAY', 'GAME_ID', 'GAME', 'GAME_ID');
    drop_single_column_fk('MATCHUP_BVP', 'BATTER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    drop_single_column_fk('MATCHUP_BVP', 'PITCHER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    drop_single_column_fk('MATCHUP_BATTER_SPLITS', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    drop_single_column_fk('MATCHUP_PITCHER_SPLITS', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    drop_single_column_fk('MATCHUP_BATTER_TEAM_SPLIT', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    drop_single_column_fk('MATCHUP_PITCHER_TEAM_SPLIT', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    drop_single_column_fk('MATCHUP_BATTER_STADIUM_SPLIT', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    drop_single_column_fk('MATCHUP_BATTER_VS_STARTER', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
END;
/

-- Backfill canonical player mirrors and roster person classification.
UPDATE PLAYERS p
   SET p.PLAYER_BASIC_ID = TO_NUMBER(TRIM(p.KBO_PERSON_ID))
 WHERE REGEXP_LIKE(TRIM(p.KBO_PERSON_ID), '^[0-9]+$')
   AND EXISTS (
       SELECT 1 FROM PLAYER_BASIC pb
        WHERE pb.PLAYER_ID = TO_NUMBER(TRIM(p.KBO_PERSON_ID))
   )
   AND (p.PLAYER_BASIC_ID IS NULL OR p.PLAYER_BASIC_ID <> TO_NUMBER(TRIM(p.KBO_PERSON_ID)));

DECLARE
    v_table_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'TEAM_DAILY_ROSTER';
    IF v_table_exists = 1 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE TEAM_DAILY_ROSTER DISABLE ALL TRIGGERS';
        UPDATE TEAM_DAILY_ROSTER roster
           SET roster.POSITION = COALESCE(
               (
                   SELECT NULLIF(pb.POSITION, '')
                     FROM PLAYER_BASIC pb
                    WHERE pb.PLAYER_ID = roster.PLAYER_ID
                      AND pb.POSITION IN ('투수', '포수', '내야수', '외야수')
               ),
               CASE
                   WHEN EXISTS (SELECT 1 FROM PLAYER_BASIC pb WHERE pb.PLAYER_ID = roster.PLAYER_ID)
                   THEN '선수'
                   ELSE '코치'
               END
           )
         WHERE roster.POSITION = '포지션';

        UPDATE TEAM_DAILY_ROSTER roster
           SET roster.PERSON_TYPE = CASE
               WHEN roster.POSITION IN ('투수', '포수', '내야수', '외야수', '선수') THEN 'player'
               WHEN roster.POSITION IN ('감독', '코치') THEN 'staff'
               ELSE 'unknown'
           END;

        UPDATE TEAM_DAILY_ROSTER roster
           SET roster.PLAYER_BASIC_ID = roster.PLAYER_ID
         WHERE roster.PERSON_TYPE = 'player'
           AND EXISTS (SELECT 1 FROM PLAYER_BASIC pb WHERE pb.PLAYER_ID = roster.PLAYER_ID);

        UPDATE TEAM_DAILY_ROSTER roster
           SET roster.PLAYER_BASIC_ID = NULL
         WHERE roster.PERSON_TYPE <> 'player';
        EXECUTE IMMEDIATE 'ALTER TABLE TEAM_DAILY_ROSTER ENABLE ALL TRIGGERS';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        BEGIN
            EXECUTE IMMEDIATE 'ALTER TABLE TEAM_DAILY_ROSTER ENABLE ALL TRIGGERS';
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
        RAISE;
END;
/

-- Preserve raw TEAM_CODE and resolve a canonical team code separately.
UPDATE PLAYER_MOVEMENTS pm
   SET pm.CANONICAL_TEAM_ID = CASE UPPER(TRIM(pm.TEAM_CODE))
       WHEN 'KIA' THEN 'KIA'
       WHEN '기아' THEN 'KIA'
       WHEN '두산' THEN 'DB'
       WHEN 'DB' THEN 'DB'
       WHEN 'OB' THEN 'OB'
       WHEN '롯데' THEN 'LT'
       WHEN 'LT' THEN 'LT'
       WHEN '삼성' THEN 'SS'
       WHEN 'SS' THEN 'SS'
       WHEN '한화' THEN 'HH'
       WHEN 'HH' THEN 'HH'
       WHEN '키움' THEN 'KH'
       WHEN 'KH' THEN 'KH'
       WHEN '넥센' THEN 'NX'
       WHEN 'NX' THEN 'NX'
       WHEN '우리' THEN 'WO'
       WHEN 'WO' THEN 'WO'
       WHEN 'SSG' THEN 'SSG'
       WHEN 'SK' THEN 'SK'
       WHEN 'LG' THEN 'LG'
       WHEN 'KT' THEN 'KT'
       WHEN 'NC' THEN 'NC'
       WHEN '현대' THEN 'HU'
       WHEN 'HU' THEN 'HU'
       WHEN 'HD' THEN 'HU'
       WHEN '해태' THEN 'HT'
       WHEN 'HT' THEN 'HT'
       WHEN '쌍방울' THEN 'SL'
       WHEN 'SL' THEN 'SL'
       WHEN '태평양' THEN 'TP'
       WHEN 'TP' THEN 'TP'
       WHEN '청보' THEN 'CB'
       WHEN 'CB' THEN 'CB'
       WHEN '삼미' THEN 'SM'
       WHEN 'SM' THEN 'SM'
       WHEN '빙그레' THEN 'BE'
       WHEN 'BE' THEN 'BE'
       WHEN 'MBC' THEN 'MBC'
       ELSE TRIM(pm.TEAM_CODE)
   END;

UPDATE PLAYER_MOVEMENTS pm
   SET pm.CANONICAL_TEAM_ID = NULL
 WHERE pm.CANONICAL_TEAM_ID IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM TEAMS t WHERE t.TEAM_ID = pm.CANONICAL_TEAM_ID
   );

-- Resolve only a unique player-name candidate. Ambiguous rows remain pending
-- for the separately reviewed 025-028 backfills.
MERGE INTO PLAYER_MOVEMENTS target
USING (
    SELECT scope.ID, MIN(pb.PLAYER_ID) AS PLAYER_BASIC_ID
      FROM (
          SELECT pm.ID,
                 REGEXP_REPLACE(NVL(pm.PLAYER_NAME, ''), '\s*\([^)]*\)\s*$', '') AS NORMALIZED_NAME
            FROM PLAYER_MOVEMENTS pm
           WHERE pm.CANONICAL_TEAM_ID IS NOT NULL
      ) scope
      JOIN PLAYER_BASIC pb ON pb.NAME = scope.NORMALIZED_NAME
     GROUP BY scope.ID
    HAVING COUNT(DISTINCT pb.PLAYER_ID) = 1
) resolved
   ON (target.ID = resolved.ID)
WHEN MATCHED THEN UPDATE SET target.PLAYER_BASIC_ID = resolved.PLAYER_BASIC_ID;

UPDATE PLAYER_MOVEMENTS pm
   SET pm.RESOLUTION_STATUS = CASE
       WHEN pm.CANONICAL_TEAM_ID IS NULL THEN 'unresolved_team'
       WHEN pm.PLAYER_BASIC_ID IS NULL THEN 'unresolved_player'
       ELSE 'resolved'
   END;

-- Nullable identity fields use function-based indexes so unresolved rows do
-- not collide with one another.
DECLARE
    v_exists NUMBER;

    PROCEDURE create_index_if_missing(p_index_name VARCHAR2, p_ddl VARCHAR2) IS
    BEGIN
        SELECT COUNT(*) INTO v_exists FROM user_indexes WHERE index_name = UPPER(p_index_name);
        IF v_exists = 0 THEN
            BEGIN
                EXECUTE IMMEDIATE p_ddl;
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1408 THEN RAISE; END IF;
            END;
        END IF;
    END;
BEGIN
    create_index_if_missing(
        'UQ_PLAYERS_PLAYER_BASIC_ID',
        'CREATE UNIQUE INDEX UQ_PLAYERS_PLAYER_BASIC_ID ON PLAYERS '
        || '(CASE WHEN PLAYER_BASIC_ID IS NOT NULL THEN PLAYER_BASIC_ID END)'
    );
    create_index_if_missing(
        'IDX_PLAYERS_PLAYER_BASIC_ID',
        'CREATE INDEX IDX_PLAYERS_PLAYER_BASIC_ID ON PLAYERS '
        || '(CASE WHEN PLAYER_BASIC_ID IS NOT NULL THEN PLAYER_BASIC_ID END)'
    );
    create_index_if_missing(
        'IDX_TEAM_DAILY_ROSTER_PLAYER_BASIC_ID',
        'CREATE INDEX IDX_TEAM_DAILY_ROSTER_PLAYER_BASIC_ID ON TEAM_DAILY_ROSTER '
        || '(CASE WHEN PLAYER_BASIC_ID IS NOT NULL THEN PLAYER_BASIC_ID END)'
    );
    create_index_if_missing(
        'IDX_PLAYER_MOVEMENTS_PLAYER_BASIC_ID',
        'CREATE INDEX IDX_PLAYER_MOVEMENTS_PLAYER_BASIC_ID ON PLAYER_MOVEMENTS '
        || '(CASE WHEN PLAYER_BASIC_ID IS NOT NULL THEN PLAYER_BASIC_ID END)'
    );
    create_index_if_missing(
        'IDX_PLAYER_MOVEMENTS_CANONICAL_TEAM_ID',
        'CREATE INDEX IDX_PLAYER_MOVEMENTS_CANONICAL_TEAM_ID ON PLAYER_MOVEMENTS '
        || '(CASE WHEN CANONICAL_TEAM_ID IS NOT NULL THEN CANONICAL_TEAM_ID END)'
    );
END;
/

-- The following FK rebuild is intentionally written as ENABLE NOVALIDATE.
-- Validation is performed only after the explicit orphan census below.
DECLARE
    v_exists NUMBER;

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
          JOIN user_cons_columns parent_column
            ON parent_column.constraint_name = parent_constraint.constraint_name
           AND parent_column.table_name = parent_constraint.table_name
         WHERE child_constraint.constraint_type = 'R'
           AND child_constraint.table_name = UPPER(p_child_table)
           AND child_column.column_name = UPPER(p_child_column)
           AND parent_constraint.table_name = UPPER(p_parent_table)
           AND parent_column.column_name = UPPER(p_parent_column)
           AND (SELECT COUNT(*) FROM user_cons_columns c
                 WHERE c.constraint_name = child_constraint.constraint_name
                   AND c.table_name = child_constraint.table_name) = 1
           AND (SELECT COUNT(*) FROM user_cons_columns c
                 WHERE c.constraint_name = parent_constraint.constraint_name
                   AND c.table_name = parent_constraint.table_name) = 1;
        RETURN v_count > 0;
    END;

    PROCEDURE add_fk_if_missing(
        p_name VARCHAR2,
        p_child_table VARCHAR2,
        p_child_column VARCHAR2,
        p_parent_table VARCHAR2,
        p_parent_column VARCHAR2,
        p_delete_action VARCHAR2 DEFAULT NULL
    ) IS
        v_child_table NUMBER;
        v_parent_table NUMBER;
        v_child_column NUMBER;
        v_named NUMBER;
        v_sql VARCHAR2(1000);
    BEGIN
        SELECT COUNT(*) INTO v_child_table FROM user_tables WHERE table_name = UPPER(p_child_table);
        SELECT COUNT(*) INTO v_parent_table FROM user_tables WHERE table_name = UPPER(p_parent_table);
        SELECT COUNT(*) INTO v_child_column FROM user_tab_columns
         WHERE table_name = UPPER(p_child_table) AND column_name = UPPER(p_child_column);
        IF v_child_table = 0 OR v_parent_table = 0 OR v_child_column = 0 THEN RETURN; END IF;
        SELECT COUNT(*) INTO v_named FROM user_constraints
         WHERE table_name = UPPER(p_child_table) AND constraint_name = UPPER(p_name);
        IF v_named = 0 AND NOT has_single_column_fk(p_child_table, p_child_column, p_parent_table, p_parent_column) THEN
            v_sql := 'ALTER TABLE ' || UPPER(p_child_table)
                || ' ADD CONSTRAINT ' || UPPER(p_name)
                || ' FOREIGN KEY (' || UPPER(p_child_column) || ') REFERENCES '
                || UPPER(p_parent_table) || ' (' || UPPER(p_parent_column) || ')';
            IF UPPER(NVL(p_delete_action, '')) = 'CASCADE' THEN v_sql := v_sql || ' ON DELETE CASCADE'; END IF;
            v_sql := v_sql || ' ENABLE NOVALIDATE';
            EXECUTE IMMEDIATE v_sql;
        END IF;
    END;
BEGIN
    add_fk_if_missing('FK_024_PLAYERS_BASIC', 'PLAYERS', 'PLAYER_BASIC_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_024_ROSTER_TEAM', 'TEAM_DAILY_ROSTER', 'TEAM_CODE', 'TEAMS', 'TEAM_ID');
    add_fk_if_missing('FK_024_ROSTER_BASIC', 'TEAM_DAILY_ROSTER', 'PLAYER_BASIC_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_024_MOVEMENT_TEAM', 'PLAYER_MOVEMENTS', 'CANONICAL_TEAM_ID', 'TEAMS', 'TEAM_ID');
    add_fk_if_missing('FK_024_MOVEMENT_BASIC', 'PLAYER_MOVEMENTS', 'PLAYER_BASIC_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_024_ALIAS_GAME', 'GAME_ID_ALIASES', 'CANONICAL_GAME_ID', 'GAME', 'GAME_ID', 'CASCADE');
    add_fk_if_missing('FK_024_METADATA_GAME', 'GAME_METADATA', 'GAME_ID', 'GAME', 'GAME_ID', 'CASCADE');
    add_fk_if_missing('FK_024_INNING_GAME', 'GAME_INNING_SCORES', 'GAME_ID', 'GAME', 'GAME_ID', 'CASCADE');
    add_fk_if_missing('FK_024_LINEUP_GAME', 'GAME_LINEUPS', 'GAME_ID', 'GAME', 'GAME_ID', 'CASCADE');
    add_fk_if_missing('FK_024_LINEUP_PLAYER', 'GAME_LINEUPS', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_024_BATTING_GAME', 'GAME_BATTING_STATS', 'GAME_ID', 'GAME', 'GAME_ID', 'CASCADE');
    add_fk_if_missing('FK_024_BATTING_PLAYER', 'GAME_BATTING_STATS', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_024_PITCHING_GAME', 'GAME_PITCHING_STATS', 'GAME_ID', 'GAME', 'GAME_ID', 'CASCADE');
    add_fk_if_missing('FK_024_PITCHING_PLAYER', 'GAME_PITCHING_STATS', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_024_EVENTS_GAME', 'GAME_EVENTS', 'GAME_ID', 'GAME', 'GAME_ID', 'CASCADE');
    add_fk_if_missing('FK_024_EVENTS_BATTER', 'GAME_EVENTS', 'BATTER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_024_EVENTS_PITCHER', 'GAME_EVENTS', 'PITCHER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_024_SUMMARY_GAME', 'GAME_SUMMARY', 'GAME_ID', 'GAME', 'GAME_ID', 'CASCADE');
    add_fk_if_missing('FK_024_SUMMARY_PLAYER', 'GAME_SUMMARY', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_024_PBP_GAME', 'GAME_PLAY_BY_PLAY', 'GAME_ID', 'GAME', 'GAME_ID', 'CASCADE');
    add_fk_if_missing('FK_024_BVP_BATTER', 'MATCHUP_BVP', 'BATTER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_024_BVP_PITCHER', 'MATCHUP_BVP', 'PITCHER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_024_BATTER_SPLIT', 'MATCHUP_BATTER_SPLITS', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_024_PITCHER_SPLIT', 'MATCHUP_PITCHER_SPLITS', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_024_BATTER_TEAM', 'MATCHUP_BATTER_TEAM_SPLIT', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_024_PITCHER_TEAM', 'MATCHUP_PITCHER_TEAM_SPLIT', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_024_BATTER_STADIUM', 'MATCHUP_BATTER_STADIUM_SPLIT', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    add_fk_if_missing('FK_024_BATTER_STARTER', 'MATCHUP_BATTER_VS_STARTER', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
END;
/

-- Explicit orphan census. This block intentionally raises before any final
-- validation if the data is not clean.
DECLARE
    v_orphans NUMBER;

    PROCEDURE assert_no_orphans(
        p_child_table VARCHAR2,
        p_child_column VARCHAR2,
        p_parent_table VARCHAR2,
        p_parent_column VARCHAR2
    ) IS
        v_child NUMBER;
        v_parent NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_child FROM user_tables WHERE table_name = UPPER(p_child_table);
        SELECT COUNT(*) INTO v_parent FROM user_tables WHERE table_name = UPPER(p_parent_table);
        IF v_child = 0 OR v_parent = 0 THEN RETURN; END IF;
        EXECUTE IMMEDIATE
            'SELECT COUNT(*) FROM ' || UPPER(p_child_table) || ' child_row '
            || 'WHERE child_row.' || UPPER(p_child_column) || ' IS NOT NULL '
            || 'AND NOT EXISTS (SELECT 1 FROM ' || UPPER(p_parent_table) || ' parent_row '
            || 'WHERE parent_row.' || UPPER(p_parent_column) || ' = child_row.' || UPPER(p_child_column) || ')'
            INTO v_orphans;
        IF v_orphans > 0 THEN
            RAISE_APPLICATION_ERROR(
                -20024,
                UPPER(p_child_table) || '.' || UPPER(p_child_column)
                || ' has orphan rows: ' || v_orphans
            );
        END IF;
    END;
BEGIN
    assert_no_orphans('PLAYERS', 'PLAYER_BASIC_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    assert_no_orphans('TEAM_DAILY_ROSTER', 'TEAM_CODE', 'TEAMS', 'TEAM_ID');
    assert_no_orphans('TEAM_DAILY_ROSTER', 'PLAYER_BASIC_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    assert_no_orphans('PLAYER_MOVEMENTS', 'CANONICAL_TEAM_ID', 'TEAMS', 'TEAM_ID');
    assert_no_orphans('PLAYER_MOVEMENTS', 'PLAYER_BASIC_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    assert_no_orphans('GAME_ID_ALIASES', 'CANONICAL_GAME_ID', 'GAME', 'GAME_ID');
    assert_no_orphans('GAME_METADATA', 'GAME_ID', 'GAME', 'GAME_ID');
    assert_no_orphans('GAME_INNING_SCORES', 'GAME_ID', 'GAME', 'GAME_ID');
    assert_no_orphans('GAME_LINEUPS', 'GAME_ID', 'GAME', 'GAME_ID');
    assert_no_orphans('GAME_LINEUPS', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    assert_no_orphans('GAME_BATTING_STATS', 'GAME_ID', 'GAME', 'GAME_ID');
    assert_no_orphans('GAME_BATTING_STATS', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    assert_no_orphans('GAME_PITCHING_STATS', 'GAME_ID', 'GAME', 'GAME_ID');
    assert_no_orphans('GAME_PITCHING_STATS', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    assert_no_orphans('GAME_EVENTS', 'GAME_ID', 'GAME', 'GAME_ID');
    assert_no_orphans('GAME_EVENTS', 'BATTER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    assert_no_orphans('GAME_EVENTS', 'PITCHER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    assert_no_orphans('GAME_SUMMARY', 'GAME_ID', 'GAME', 'GAME_ID');
    assert_no_orphans('GAME_SUMMARY', 'PLAYER_ID', 'PLAYER_BASIC', 'PLAYER_ID');
    assert_no_orphans('GAME_PLAY_BY_PLAY', 'GAME_ID', 'GAME', 'GAME_ID');
END;
/

-- Validate only the named constraints that exist after the census.
DECLARE
    PROCEDURE validate_if_present(p_table_name VARCHAR2, p_constraint_name VARCHAR2) IS
        v_exists NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_exists
          FROM user_constraints
         WHERE table_name = UPPER(p_table_name)
           AND constraint_name = UPPER(p_constraint_name)
           AND constraint_type = 'R';
        IF v_exists = 1 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE ' || UPPER(p_table_name)
                || ' ENABLE VALIDATE CONSTRAINT ' || UPPER(p_constraint_name);
        END IF;
    END;
BEGIN
    validate_if_present('PLAYERS', 'FK_024_PLAYERS_BASIC');
    validate_if_present('TEAM_DAILY_ROSTER', 'FK_024_ROSTER_TEAM');
    validate_if_present('TEAM_DAILY_ROSTER', 'FK_024_ROSTER_BASIC');
    validate_if_present('PLAYER_MOVEMENTS', 'FK_024_MOVEMENT_TEAM');
    validate_if_present('PLAYER_MOVEMENTS', 'FK_024_MOVEMENT_BASIC');
    validate_if_present('GAME_ID_ALIASES', 'FK_024_ALIAS_GAME');
    validate_if_present('GAME_METADATA', 'FK_024_METADATA_GAME');
    validate_if_present('GAME_INNING_SCORES', 'FK_024_INNING_GAME');
    validate_if_present('GAME_LINEUPS', 'FK_024_LINEUP_GAME');
    validate_if_present('GAME_LINEUPS', 'FK_024_LINEUP_PLAYER');
    validate_if_present('GAME_BATTING_STATS', 'FK_024_BATTING_GAME');
    validate_if_present('GAME_BATTING_STATS', 'FK_024_BATTING_PLAYER');
    validate_if_present('GAME_PITCHING_STATS', 'FK_024_PITCHING_GAME');
    validate_if_present('GAME_PITCHING_STATS', 'FK_024_PITCHING_PLAYER');
    validate_if_present('GAME_EVENTS', 'FK_024_EVENTS_GAME');
    validate_if_present('GAME_EVENTS', 'FK_024_EVENTS_BATTER');
    validate_if_present('GAME_EVENTS', 'FK_024_EVENTS_PITCHER');
    validate_if_present('GAME_SUMMARY', 'FK_024_SUMMARY_GAME');
    validate_if_present('GAME_SUMMARY', 'FK_024_SUMMARY_PLAYER');
    validate_if_present('GAME_PLAY_BY_PLAY', 'FK_024_PBP_GAME');
END;
/
