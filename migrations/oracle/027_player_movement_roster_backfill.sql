-- Oracle port of 027_player_movement_roster_backfill.sql.
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
            -20027,
            '027 requires 024 player movement resolution columns'
        );
    END IF;

        EXECUTE IMMEDIATE q'[
            MERGE /*+ NO_PARALLEL */ INTO PLAYER_MOVEMENTS target
        USING (
            SELECT movement_scope.ID, MIN(team_daily_roster.PLAYER_BASIC_ID) AS PLAYER_BASIC_ID
              FROM (
                  SELECT pm.ID,
                         pm.MOVEMENT_DATE,
                         pm.CANONICAL_TEAM_ID,
                         REGEXP_REPLACE(NVL(pm.PLAYER_NAME, ''), '\s*\([^)]*\)\s*$', '') AS NORMALIZED_NAME
                    FROM PLAYER_MOVEMENTS pm
                   WHERE pm.PLAYER_BASIC_ID IS NULL
                     AND pm.RESOLUTION_STATUS IN ('unresolved', 'unresolved_player')
                     AND pm.CANONICAL_TEAM_ID IS NOT NULL
              ) movement_scope
              JOIN TEAM_DAILY_ROSTER team_daily_roster
                ON team_daily_roster.TEAM_CODE = movement_scope.CANONICAL_TEAM_ID
               AND team_daily_roster.PLAYER_NAME = movement_scope.NORMALIZED_NAME
               AND EXTRACT(YEAR FROM team_daily_roster.ROSTER_DATE) = EXTRACT(YEAR FROM movement_scope.MOVEMENT_DATE)
               AND team_daily_roster.PERSON_TYPE = 'player'
               AND team_daily_roster.PLAYER_BASIC_ID IS NOT NULL
             GROUP BY movement_scope.ID
            HAVING COUNT(DISTINCT team_daily_roster.PLAYER_BASIC_ID) = 1
        ) resolved
           ON (target.ID = resolved.ID)
        WHEN MATCHED THEN UPDATE SET
            target.PLAYER_BASIC_ID = resolved.PLAYER_BASIC_ID,
            target.RESOLUTION_STATUS = 'resolved'
    ]';
END;
/
