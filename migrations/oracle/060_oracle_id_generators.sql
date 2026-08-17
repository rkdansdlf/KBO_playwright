-- Add Oracle-side ID generation for ORM baseline tables.
--
-- SQLAlchemy's Oracle metadata bootstrap creates NUMBER primary keys but does
-- not add identity clauses for the existing ORM baseline. Bulk sync remains
-- free to supply source IDs; these generators only fill NULL IDs on inserts
-- made by ORM repositories and crawlers.
DECLARE
    TYPE table_names IS TABLE OF VARCHAR2(30);
    v_tables table_names := table_names(
        'AWARDS',
        'CHEER_CHANTS',
        'CHEER_SONGS',
        'CORRECTION_AUDIT_TRAIL',
        'CRAWL_EVIDENCE',
        'CRAWL_RUNS',
        'DATA_SOURCES',
        'EXTERNAL_SEASON_STATS',
        'FA_CONTRACTS',
        'FOREIGN_PLAYER_CHANGES',
        'FUTURES_GAME_SCHEDULES',
        'FUTURES_TEAM_STANDINGS',
        'GAME',
        'GAME_BATTING_STATS',
        'GAME_BROADCASTS',
        'GAME_EVENTS',
        'GAME_HIGHLIGHTS',
        'GAME_INNING_SCORES',
        'GAME_LINEUPS',
        'GAME_MVPS',
        'GAME_PITCHING_STATS',
        'GAME_PLAY_BY_PLAY',
        'GAME_SUMMARY',
        'GAME_VALIDATION_METRICS',
        'INJURY_ENTRIES',
        'KBO_PRESS_RELEASES',
        'MANAGER_CHANGES',
        'MATCHUP_BATTER_HOME_AWAY',
        'MATCHUP_BATTER_SPLITS',
        'MATCHUP_BATTER_STADIUM_SPLIT',
        'MATCHUP_BATTER_TEAM_SPLIT',
        'MATCHUP_BATTER_VS_STARTER',
        'MATCHUP_BVP',
        'MATCHUP_PITCHER_HOME_AWAY',
        'MATCHUP_PITCHER_SPLITS',
        'MATCHUP_PITCHER_TEAM_SPLIT',
        'PARKING_FEE_RULES',
        'PARKING_LOTS',
        'PLAYER_DRAFT_HISTORIES',
        'PLAYER_GAME_BATTING',
        'PLAYER_GAME_PITCHING',
        'PLAYER_IDENTITIES',
        'PLAYER_MILESTONES',
        'PLAYER_MOVEMENTS',
        'PLAYER_PROJECTIONS',
        'PLAYER_SEASON_BASERUNNING',
        'PLAYER_SEASON_BATTING',
        'PLAYER_SEASON_FIELDING',
        'PLAYER_SEASON_PITCHING',
        'PLAYER_SPLITS_STATS',
        'PLAYERS',
        'QUARANTINED_RECORDS',
        'RAG_CHUNKS',
        'RAW_SOURCE_SNAPSHOTS',
        'ROSTER_TRANSACTIONS',
        'SLA_METRICS',
        'STADIUM_CONGESTION',
        'STADIUM_FOOD_MENU_ITEMS',
        'STADIUM_FOOD_VENDORS',
        'STADIUM_FOODS',
        'STADIUM_OPERATION_NOTICES',
        'STADIUM_REGULATIONS',
        'STADIUM_SEAT_SECTIONS',
        'STADIUM_TRANSIT_TIMES',
        'STAT_RANKINGS',
        'TEAM_CODE_MAP',
        'TEAM_DAILY_ROSTER',
        'TEAM_EVENTS',
        'TEAM_FRANCHISES',
        'TEAM_HISTORY',
        'TEAM_RIVALRIES',
        'TEAM_SEASON_BASERUNNING',
        'TEAM_SEASON_BATTING',
        'TEAM_SEASON_FIELDING',
        'TEAM_SEASON_PITCHING',
        'TEAM_STANDINGS_DAILY',
        'TICKET_OPEN_RULES',
        'TICKET_PRICES',
        'TICKET_SCHEDULES'
    );
    v_table_exists NUMBER;
    v_identity_exists NUMBER;
    v_sequence_exists NUMBER;
    v_next_id NUMBER;
    v_sequence_name VARCHAR2(30);
    v_trigger_name VARCHAR2(30);
BEGIN
    FOR i IN 1..v_tables.COUNT LOOP
        SELECT COUNT(*)
          INTO v_table_exists
          FROM user_tables
         WHERE table_name = v_tables(i);

        IF v_table_exists = 1 THEN
            SELECT COUNT(*)
              INTO v_identity_exists
              FROM user_tab_identity_cols
             WHERE table_name = v_tables(i)
               AND column_name = 'ID';

            IF v_identity_exists = 0 THEN
                v_sequence_name := 'KBO_AI_SQ_' || LPAD(TO_CHAR(i), 3, '0');
                v_trigger_name := 'KBO_AI_TR_' || LPAD(TO_CHAR(i), 3, '0');

                EXECUTE IMMEDIATE
                    'SELECT NVL(MAX(ID), 0) + 1 FROM "' || v_tables(i) || '"'
                    INTO v_next_id;

                SELECT COUNT(*)
                  INTO v_sequence_exists
                  FROM user_sequences
                 WHERE sequence_name = v_sequence_name;

                IF v_sequence_exists = 0 THEN
                    EXECUTE IMMEDIATE
                        'CREATE SEQUENCE ' || v_sequence_name
                        || ' START WITH '
                        || TO_CHAR(v_next_id, 'FM99999999999999999999999999999999999999')
                        || ' INCREMENT BY 1 NOCACHE';
                END IF;

                EXECUTE IMMEDIATE
                    'CREATE OR REPLACE TRIGGER ' || v_trigger_name
                    || ' BEFORE INSERT ON "' || v_tables(i) || '"'
                    || ' FOR EACH ROW WHEN (new.ID IS NULL)'
                    || ' BEGIN SELECT ' || v_sequence_name
                    || '.NEXTVAL INTO :new.ID FROM dual; END;';
            END IF;
        END IF;
    END LOOP;
END;
/
