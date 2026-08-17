-- Oracle-only schema-drift repair for PLAYER_SEASON_BATTING.
--
-- The OCI batting table predates the model column rename and stores the same
-- concept as LEAGUE_LEVEL. The current model and PLAYER_SEASON_PITCHING use a
-- quoted Oracle column named "level", which is required by migration 048.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_tab_columns
     WHERE table_name = 'PLAYER_SEASON_BATTING'
       AND column_name = 'level';
    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE PLAYER_SEASON_BATTING '
            || 'ADD ("level" VARCHAR2(50) DEFAULT ''KBO1'' NOT NULL)';
    END IF;
    SELECT COUNT(*)
      INTO v_exists
      FROM user_tab_columns
     WHERE table_name = 'PLAYER_SEASON_BATTING'
       AND column_name = 'LEAGUE_LEVEL';
    IF v_exists > 0 THEN
        EXECUTE IMMEDIATE q'[
            UPDATE PLAYER_SEASON_BATTING
               SET "level" = CASE
                   WHEN TRIM(LEAGUE_LEVEL) IN ('1', '1군') THEN 'KBO1'
                   ELSE TRIM(LEAGUE_LEVEL)
               END
             WHERE "level" IS NULL
                OR "level" != CASE
                   WHEN TRIM(LEAGUE_LEVEL) IN ('1', '1군') THEN 'KBO1'
                   ELSE TRIM(LEAGUE_LEVEL)
               END
        ]';
    END IF;
END;
/
