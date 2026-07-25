-- Oracle port of 029_add_team_profiles_indexes.sql.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_exists
      FROM user_tables
     WHERE table_name = 'TEAM_PROFILES';
    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE q'[
            CREATE TABLE TEAM_PROFILES (
                TEAM_ID VARCHAR2(10) NOT NULL,
                PROFILE VARCHAR2(64) NOT NULL,
                CONSTRAINT PK_TEAM_PROFILES PRIMARY KEY (TEAM_ID, PROFILE)
            )
        ]';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_exists
      FROM user_indexes
     WHERE index_name = 'UQ_TEAM_PROFILES_TEAM_ID_PROFILE';
    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'CREATE UNIQUE INDEX UQ_TEAM_PROFILES_TEAM_ID_PROFILE '
            || 'ON TEAM_PROFILES (TEAM_ID, PROFILE)';
    END IF;

    SELECT COUNT(*) INTO v_exists
      FROM user_indexes
     WHERE index_name = 'IDX_TEAM_PROFILES_PROFILE';
    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE 'CREATE INDEX IDX_TEAM_PROFILES_PROFILE ON TEAM_PROFILES (PROFILE)';
    END IF;
END;
/
