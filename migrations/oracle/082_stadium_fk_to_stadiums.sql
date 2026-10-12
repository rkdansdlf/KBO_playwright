-- 081_stadium_fk_to_stadiums.sql
-- Convergence phase 2: repoint child FKs from STADIUM_INFO to STADIUMS.
-- Values are identical codes, so only the constraint target changes.
-- Constraint names are kept. Idempotent via user_constraints guards.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_constraints
     WHERE table_name = 'STADIUM_SEAT_SECTIONS'
       AND constraint_name = 'FK_SEAT_SECTIONS_STADIUM';

    IF v_exists > 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE STADIUM_SEAT_SECTIONS DROP CONSTRAINT FK_SEAT_SECTIONS_STADIUM';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_constraints
     WHERE table_name = 'STADIUM_SEAT_SECTIONS'
       AND constraint_name = 'FK_SEAT_SECTIONS_STADIUM';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE STADIUM_SEAT_SECTIONS ADD CONSTRAINT FK_SEAT_SECTIONS_STADIUM '
            || 'FOREIGN KEY (STADIUM_ID) REFERENCES STADIUMS (STADIUM_ID) ON DELETE CASCADE';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_constraints
     WHERE table_name = 'PARKING_LOTS'
       AND constraint_name = 'FK_PARKING_LOTS_STADIUM';

    IF v_exists > 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE PARKING_LOTS DROP CONSTRAINT FK_PARKING_LOTS_STADIUM';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_constraints
     WHERE table_name = 'PARKING_LOTS'
       AND constraint_name = 'FK_PARKING_LOTS_STADIUM';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE PARKING_LOTS ADD CONSTRAINT FK_PARKING_LOTS_STADIUM '
            || 'FOREIGN KEY (STADIUM_ID) REFERENCES STADIUMS (STADIUM_ID) ON DELETE CASCADE';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_constraints
     WHERE table_name = 'STADIUM_FOOD_VENDORS'
       AND constraint_name = 'FK_FOOD_VENDORS_STADIUM';

    IF v_exists > 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE STADIUM_FOOD_VENDORS DROP CONSTRAINT FK_FOOD_VENDORS_STADIUM';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_constraints
     WHERE table_name = 'STADIUM_FOOD_VENDORS'
       AND constraint_name = 'FK_FOOD_VENDORS_STADIUM';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE STADIUM_FOOD_VENDORS ADD CONSTRAINT FK_FOOD_VENDORS_STADIUM '
            || 'FOREIGN KEY (STADIUM_ID) REFERENCES STADIUMS (STADIUM_ID) ON DELETE CASCADE';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_constraints
     WHERE table_name = 'STADIUM_TRANSIT_TIMES'
       AND constraint_name = 'FK_STT_STADIUM';

    IF v_exists > 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE STADIUM_TRANSIT_TIMES DROP CONSTRAINT FK_STT_STADIUM';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_constraints
     WHERE table_name = 'STADIUM_TRANSIT_TIMES'
       AND constraint_name = 'FK_STT_STADIUM';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE STADIUM_TRANSIT_TIMES ADD CONSTRAINT FK_STT_STADIUM '
            || 'FOREIGN KEY (STADIUM_CODE) REFERENCES STADIUMS (STADIUM_ID) ON DELETE CASCADE';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_constraints
     WHERE table_name = 'STADIUM_CONGESTION'
       AND constraint_name = 'FK_SC_STADIUM';

    IF v_exists > 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE STADIUM_CONGESTION DROP CONSTRAINT FK_SC_STADIUM';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_constraints
     WHERE table_name = 'STADIUM_CONGESTION'
       AND constraint_name = 'FK_SC_STADIUM';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE STADIUM_CONGESTION ADD CONSTRAINT FK_SC_STADIUM '
            || 'FOREIGN KEY (STADIUM_CODE) REFERENCES STADIUMS (STADIUM_ID) ON DELETE CASCADE';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_constraints
     WHERE table_name = 'STADIUM_OPERATION_NOTICES'
       AND constraint_name = 'FK_SON_STADIUM';

    IF v_exists > 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE STADIUM_OPERATION_NOTICES DROP CONSTRAINT FK_SON_STADIUM';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_constraints
     WHERE table_name = 'STADIUM_OPERATION_NOTICES'
       AND constraint_name = 'FK_SON_STADIUM';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE STADIUM_OPERATION_NOTICES ADD CONSTRAINT FK_SON_STADIUM '
            || 'FOREIGN KEY (STADIUM_CODE) REFERENCES STADIUMS (STADIUM_ID) ON DELETE CASCADE';
    END IF;
END;
/
