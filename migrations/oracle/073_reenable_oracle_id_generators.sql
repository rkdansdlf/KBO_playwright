-- Re-enable Oracle-side ID generators that may have been disabled during a
-- bulk-load or operational repair. The triggers only assign IDs when the
-- incoming value is NULL, so explicit source IDs remain valid.
DECLARE
    v_sql VARCHAR2(200);
BEGIN
    FOR generator_row IN (
        SELECT trigger_name
          FROM user_triggers
         WHERE trigger_name LIKE 'KBO_AI_TR_%'
           AND status = 'DISABLED'
    ) LOOP
        v_sql := 'ALTER TRIGGER "' || generator_row.trigger_name || '" ENABLE';
        EXECUTE IMMEDIATE v_sql;
    END LOOP;
END;
/
