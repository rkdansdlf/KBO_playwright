-- Oracle port of 036_phase1_team_event_unique.sql.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_exists
      FROM user_constraints
     WHERE table_name = 'TEAM_EVENTS'
       AND constraint_name = 'UQ_TEAM_EVENT';
    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE TEAM_EVENTS ADD CONSTRAINT UQ_TEAM_EVENT '
            || 'UNIQUE (TEAM_ID, TITLE, SOURCE_URL)';
    END IF;
END;
/
