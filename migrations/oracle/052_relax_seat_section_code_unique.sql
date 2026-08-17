-- Section codes are optional. Oracle treats repeated NULL values in this
-- composite constraint as duplicates when the stadium ID is the same; the
-- section-name uniqueness constraint remains the canonical fallback.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_constraints
     WHERE table_name = 'STADIUM_SEAT_SECTIONS'
       AND constraint_name = 'UQ_SEAT_SECTION_CODE';
    IF v_exists > 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE STADIUM_SEAT_SECTIONS DROP CONSTRAINT UQ_SEAT_SECTION_CODE';
    END IF;
END;
/
