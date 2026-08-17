-- Oracle port of 026_player_movement_profile_mirror_backfill.sql.
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
            -20026,
            '026 requires 024 player movement resolution columns'
        );
    END IF;

        EXECUTE IMMEDIATE q'[
            MERGE /*+ NO_PARALLEL */ INTO PLAYER_MOVEMENTS target
        USING (
            SELECT candidate_scope.ID, MIN(candidate_scope.PLAYER_ID) AS PLAYER_BASIC_ID
              FROM (
                  SELECT movement_scope.ID,
                         player_basic.PLAYER_ID,
                         player_basic.POSITION,
                         movement_scope.RAW_POSITION,
                         MAX(CASE WHEN player_basic.POSITION = movement_scope.RAW_POSITION THEN 1 ELSE 0 END)
                             OVER (PARTITION BY movement_scope.ID) AS HAS_POSITION_CANDIDATES
                    FROM (
                        SELECT pm.ID,
                               REGEXP_REPLACE(NVL(pm.PLAYER_NAME, ''), '\s*\([^)]*\)\s*$', '') AS NORMALIZED_NAME,
                               REGEXP_SUBSTR(pm.PLAYER_NAME, '\(([^)]*)\)', 1, 1, NULL, 1) AS RAW_POSITION
                          FROM PLAYER_MOVEMENTS pm
                         WHERE pm.PLAYER_BASIC_ID IS NULL
                           AND pm.RESOLUTION_STATUS IN ('unresolved', 'unresolved_player')
                           AND pm.CANONICAL_TEAM_ID IS NOT NULL
                    ) movement_scope
                    JOIN PLAYER_BASIC player_basic
                      ON player_basic.NAME = movement_scope.NORMALIZED_NAME
              ) candidate_scope
              JOIN PLAYERS players
                ON players.PLAYER_BASIC_ID = candidate_scope.PLAYER_ID
             WHERE candidate_scope.RAW_POSITION IS NULL
                OR candidate_scope.RAW_POSITION = ''
                OR candidate_scope.HAS_POSITION_CANDIDATES = 0
                OR candidate_scope.POSITION = candidate_scope.RAW_POSITION
             GROUP BY candidate_scope.ID
            HAVING COUNT(DISTINCT candidate_scope.PLAYER_ID) = 1
        ) resolved
           ON (target.ID = resolved.ID)
        WHEN MATCHED THEN UPDATE SET
            target.PLAYER_BASIC_ID = resolved.PLAYER_BASIC_ID,
            target.RESOLUTION_STATUS = 'resolved'
    ]';
END;
/
