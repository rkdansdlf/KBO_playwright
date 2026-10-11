-- 080_stadiums_dimensions.sql
-- Promote free-text geometry out of STADIUMS.NOTES: right fence distance,
-- dome flag, and the validity window. Idempotent via user_tab_columns guards.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_tab_columns
     WHERE table_name = 'STADIUMS' AND column_name = 'RIGHT_FENCE_M';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE STADIUMS ADD (RIGHT_FENCE_M NUMBER)';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_tab_columns
     WHERE table_name = 'STADIUMS' AND column_name = 'IS_DOME';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE STADIUMS ADD (IS_DOME NUMBER(1) DEFAULT 0 NOT NULL)';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_tab_columns
     WHERE table_name = 'STADIUMS' AND column_name = 'VALID_FROM_YEAR';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE STADIUMS ADD (VALID_FROM_YEAR NUMBER(10))';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_tab_columns
     WHERE table_name = 'STADIUMS' AND column_name = 'VALID_TO_YEAR';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE STADIUMS ADD (VALID_TO_YEAR NUMBER(10))';
    END IF;
END;
/
