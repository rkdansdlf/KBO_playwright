-- Oracle port of 021_game_summary_hash_index.sql.
--
-- Oracle cannot index a CLOB directly. Store a trigger-maintained MD5 hash in
-- a bounded VARCHAR2 column and index that value instead. This requires
-- EXECUTE privilege on DBMS_CRYPTO in the target schema.
--
-- Unlike the PostgreSQL source migration, this port does not silently delete
-- duplicates. It raises an error so duplicate cleanup can be reviewed before
-- Oracle's DDL auto-commit boundary.

DECLARE
    PROCEDURE drop_constraint_if_exists(p_name VARCHAR2) IS
        v_exists NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_exists
          FROM user_constraints
         WHERE table_name = 'GAME_SUMMARY'
           AND constraint_name = UPPER(p_name);
        IF v_exists = 1 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE GAME_SUMMARY DROP CONSTRAINT ' || UPPER(p_name);
        END IF;
    END;

    PROCEDURE drop_index_if_exists(p_name VARCHAR2) IS
        v_exists NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_exists
          FROM user_indexes
         WHERE index_name = UPPER(p_name);
        IF v_exists = 1 THEN
            EXECUTE IMMEDIATE 'DROP INDEX ' || UPPER(p_name);
        END IF;
    END;
BEGIN
    drop_constraint_if_exists('UQ_GAME_SUMMARY');
    drop_index_if_exists('UQ_GAME_SUMMARY');
    drop_constraint_if_exists('UQ_GAME_SUMMARY_ENTRY');
    drop_index_if_exists('UQ_GAME_SUMMARY_ENTRY');
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_exists
      FROM user_tab_columns
     WHERE table_name = 'GAME_SUMMARY'
       AND column_name = 'DETAIL_TEXT_HASH';
    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE GAME_SUMMARY ADD (DETAIL_TEXT_HASH VARCHAR2(32))';
    END IF;
END;
/

BEGIN
    UPDATE GAME_SUMMARY
       SET DETAIL_TEXT_HASH = CASE
           WHEN DETAIL_TEXT IS NULL THEN 'D41D8CD98F00B204E9800998ECF8427E'
           ELSE RAWTOHEX(DBMS_CRYPTO.HASH(DETAIL_TEXT, DBMS_CRYPTO.HASH_MD5))
       END
     WHERE DETAIL_TEXT_HASH IS NULL;
END;
/

DECLARE
    v_duplicate_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_duplicate_count
      FROM (
          SELECT GAME_ID, SUMMARY_TYPE, NVL(PLAYER_ID, -1), NVL(PLAYER_NAME, ' '), DETAIL_TEXT_HASH
            FROM GAME_SUMMARY
           GROUP BY GAME_ID, SUMMARY_TYPE, NVL(PLAYER_ID, -1), NVL(PLAYER_NAME, ' '), DETAIL_TEXT_HASH
          HAVING COUNT(*) > 1
      );
    IF v_duplicate_count > 0 THEN
        RAISE_APPLICATION_ERROR(
            -20021,
            'GAME_SUMMARY duplicate hash groups require reviewed cleanup: ' || v_duplicate_count
        );
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_exists
      FROM user_triggers
     WHERE trigger_name = 'TRG_GAME_SUMMARY_DETAIL_HASH';
    IF v_exists = 1 THEN
        EXECUTE IMMEDIATE 'DROP TRIGGER TRG_GAME_SUMMARY_DETAIL_HASH';
    END IF;

    EXECUTE IMMEDIATE q'[
        CREATE OR REPLACE TRIGGER TRG_GAME_SUMMARY_DETAIL_HASH
        BEFORE INSERT OR UPDATE ON GAME_SUMMARY
        FOR EACH ROW
        BEGIN
            :NEW.DETAIL_TEXT_HASH := CASE
                WHEN :NEW.DETAIL_TEXT IS NULL THEN 'D41D8CD98F00B204E9800998ECF8427E'
                ELSE RAWTOHEX(DBMS_CRYPTO.HASH(:NEW.DETAIL_TEXT, DBMS_CRYPTO.HASH_MD5))
            END;
        END;
    ]';
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_exists
      FROM user_indexes
     WHERE index_name = 'UQ_GAME_SUMMARY_HASH';
    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE q'[
            CREATE UNIQUE INDEX UQ_GAME_SUMMARY_HASH ON GAME_SUMMARY (
                GAME_ID,
                SUMMARY_TYPE,
                NVL(PLAYER_ID, -1),
                NVL(PLAYER_NAME, ' '),
                DETAIL_TEXT_HASH
            )
        ]';
    END IF;

    SELECT COUNT(*) INTO v_exists
      FROM user_indexes
     WHERE index_name = 'IDX_GAME_SUMMARY_LOOKUP';
    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'CREATE INDEX IDX_GAME_SUMMARY_LOOKUP '
            || 'ON GAME_SUMMARY (GAME_ID, SUMMARY_TYPE, PLAYER_NAME)';
    END IF;
END;
/
