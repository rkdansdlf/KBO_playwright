-- Oracle port of 025_player_movement_position_backfill.sql.
--
-- This backfill depends on columns introduced by the design-only 024
-- integrity migration. It is intentionally not applied until that migration
-- and its identity-resolution review are approved.
DECLARE
    v_columns NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_columns
      FROM user_tab_columns
     WHERE table_name = 'PLAYER_MOVEMENTS'
       AND column_name IN ('PLAYER_BASIC_ID', 'RESOLUTION_STATUS', 'CANONICAL_TEAM_ID');
    IF v_columns != 3 THEN
        RAISE_APPLICATION_ERROR(
            -20025,
            '025 requires 024 player movement resolution columns'
        );
    END IF;

    EXECUTE IMMEDIATE q'[
        MERGE INTO PLAYER_MOVEMENTS target
        USING (
            SELECT unresolved.ID, MIN(player_basic.PLAYER_ID) AS PLAYER_BASIC_ID
              FROM (
                  SELECT pm.ID,
                         REGEXP_REPLACE(NVL(pm.PLAYER_NAME, ''), '\s*\([^)]*\)\s*$', '') AS NORMALIZED_NAME,
                         REGEXP_SUBSTR(pm.PLAYER_NAME, '\(([^)]*)\)', 1, 1, NULL, 1) AS RAW_POSITION
                    FROM PLAYER_MOVEMENTS pm
                   WHERE pm.PLAYER_BASIC_ID IS NULL
                     AND pm.RESOLUTION_STATUS IN ('unresolved', 'unresolved_player')
              ) unresolved
              JOIN PLAYER_BASIC player_basic
                ON player_basic.NAME = unresolved.NORMALIZED_NAME
               AND player_basic.POSITION = unresolved.RAW_POSITION
             WHERE unresolved.RAW_POSITION IS NOT NULL
               AND unresolved.RAW_POSITION <> ''
             GROUP BY unresolved.ID
            HAVING COUNT(DISTINCT player_basic.PLAYER_ID) = 1
        ) resolved
           ON (target.ID = resolved.ID)
        WHEN MATCHED THEN UPDATE SET
            target.PLAYER_BASIC_ID = resolved.PLAYER_BASIC_ID,
            target.RESOLUTION_STATUS = 'resolved'
    ]';
END;
/
