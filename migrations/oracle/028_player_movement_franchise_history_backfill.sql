-- Oracle port of 028_player_movement_franchise_history_backfill.sql.
-- Apply only after the 024 identity-resolution review is approved.
DECLARE
    v_columns NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_columns
      FROM user_tab_columns
     WHERE table_name = 'PLAYER_MOVEMENTS'
       AND column_name IN ('PLAYER_BASIC_ID', 'RESOLUTION_STATUS', 'CANONICAL_TEAM_ID');
    IF v_columns != 3 THEN
        RAISE_APPLICATION_ERROR(
            -20028,
            '028 requires 024 player movement resolution columns'
        );
    END IF;

    EXECUTE IMMEDIATE q'[
        MERGE INTO PLAYER_MOVEMENTS target
        USING (
            SELECT ID, MIN(PLAYER_ID) AS PLAYER_BASIC_ID
              FROM (
                  SELECT movement_scope.ID, player_season_batting.PLAYER_ID
                    FROM (
                        SELECT pm.ID,
                               pm.CANONICAL_TEAM_ID,
                               EXTRACT(YEAR FROM pm.MOVEMENT_DATE) AS MOVEMENT_YEAR,
                               REGEXP_REPLACE(NVL(pm.PLAYER_NAME, ''), '\s*\([^)]*\)\s*$', '') AS NORMALIZED_NAME
                          FROM PLAYER_MOVEMENTS pm
                         WHERE pm.PLAYER_BASIC_ID IS NULL
                           AND pm.RESOLUTION_STATUS IN ('unresolved', 'unresolved_player')
                           AND pm.CANONICAL_TEAM_ID IS NOT NULL
                    ) movement_scope
                    JOIN PLAYER_BASIC
                      ON PLAYER_BASIC.NAME = movement_scope.NORMALIZED_NAME
                    JOIN (
                        SELECT movement_team.TEAM_ID, franchise_team.TEAM_ID AS FRANCHISE_TEAM_ID, movement_team.FRANCHISE_ID
                          FROM TEAMS movement_team
                          JOIN TEAMS franchise_team
                            ON franchise_team.FRANCHISE_ID = movement_team.FRANCHISE_ID
                         WHERE movement_team.FRANCHISE_ID IS NOT NULL
                    ) franchise_teams
                      ON franchise_teams.TEAM_ID = movement_scope.CANONICAL_TEAM_ID
                    JOIN PLAYER_SEASON_BATTING player_season_batting
                      ON player_season_batting.PLAYER_ID = PLAYER_BASIC.PLAYER_ID
                     AND player_season_batting.TEAM_CODE = franchise_teams.FRANCHISE_TEAM_ID
                     AND player_season_batting.SEASON IN (movement_scope.MOVEMENT_YEAR - 1, movement_scope.MOVEMENT_YEAR)
                  UNION ALL
                  SELECT movement_scope.ID, player_season_pitching.PLAYER_ID
                    FROM (
                        SELECT pm.ID,
                               pm.CANONICAL_TEAM_ID,
                               EXTRACT(YEAR FROM pm.MOVEMENT_DATE) AS MOVEMENT_YEAR,
                               REGEXP_REPLACE(NVL(pm.PLAYER_NAME, ''), '\s*\([^)]*\)\s*$', '') AS NORMALIZED_NAME
                          FROM PLAYER_MOVEMENTS pm
                         WHERE pm.PLAYER_BASIC_ID IS NULL
                           AND pm.RESOLUTION_STATUS IN ('unresolved', 'unresolved_player')
                           AND pm.CANONICAL_TEAM_ID IS NOT NULL
                    ) movement_scope
                    JOIN PLAYER_BASIC
                      ON PLAYER_BASIC.NAME = movement_scope.NORMALIZED_NAME
                    JOIN (
                        SELECT movement_team.TEAM_ID, franchise_team.TEAM_ID AS FRANCHISE_TEAM_ID, movement_team.FRANCHISE_ID
                          FROM TEAMS movement_team
                          JOIN TEAMS franchise_team
                            ON franchise_team.FRANCHISE_ID = movement_team.FRANCHISE_ID
                         WHERE movement_team.FRANCHISE_ID IS NOT NULL
                    ) franchise_teams
                      ON franchise_teams.TEAM_ID = movement_scope.CANONICAL_TEAM_ID
                    JOIN PLAYER_SEASON_PITCHING player_season_pitching
                      ON player_season_pitching.PLAYER_ID = PLAYER_BASIC.PLAYER_ID
                     AND player_season_pitching.TEAM_CODE = franchise_teams.FRANCHISE_TEAM_ID
                     AND player_season_pitching.SEASON IN (movement_scope.MOVEMENT_YEAR - 1, movement_scope.MOVEMENT_YEAR)
              ) candidates
             GROUP BY ID
            HAVING COUNT(DISTINCT PLAYER_ID) = 1
        ) resolved
           ON (target.ID = resolved.ID)
        WHEN MATCHED THEN UPDATE SET
            target.PLAYER_BASIC_ID = resolved.PLAYER_BASIC_ID,
            target.RESOLUTION_STATUS = 'resolved'
    ]';
END;
/
