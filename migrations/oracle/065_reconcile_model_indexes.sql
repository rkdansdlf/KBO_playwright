-- Restore model-declared indexes that were absent from an existing Oracle baseline.
DECLARE
    v_exists NUMBER;

    PROCEDURE create_index_if_missing(p_index_name VARCHAR2, p_ddl VARCHAR2) IS
    BEGIN
        SELECT COUNT(*)
          INTO v_exists
          FROM user_indexes
         WHERE index_name = UPPER(p_index_name);
        IF v_exists = 0 THEN
            BEGIN
                EXECUTE IMMEDIATE p_ddl;
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1408 THEN
                        RAISE;
                    END IF;
            END;
        END IF;
    END;
BEGIN
    create_index_if_missing(
        'IX_KBO_PRESS_RELEASES_PUBLISHED_DATE',
        'CREATE INDEX IX_KBO_PRESS_RELEASES_PUBLISHED_DATE '
        || 'ON KBO_PRESS_RELEASES (PUBLISHED_DATE)'
    );
    create_index_if_missing(
        'IDX_SC_STADIUM_MEASURED',
        'CREATE INDEX IDX_SC_STADIUM_MEASURED '
        || 'ON STADIUM_CONGESTION (STADIUM_CODE, MEASURED_AT)'
    );
    create_index_if_missing(
        'IDX_SC_STADIUM_GAME_DATE',
        'CREATE INDEX IDX_SC_STADIUM_GAME_DATE '
        || 'ON STADIUM_CONGESTION (STADIUM_CODE, GAME_DATE)'
    );
END;
/
