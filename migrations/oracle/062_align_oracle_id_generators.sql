-- Align NULL-ID trigger sequences after explicit source IDs were bulk loaded.
DECLARE
    v_sequence_name VARCHAR2(30);
    v_max_id NUMBER;
    v_next_value NUMBER;
    v_increment NUMBER;
    v_advanced_value NUMBER;
BEGIN
    FOR generator_row IN (
        SELECT table_name, trigger_body
          FROM user_triggers
         WHERE trigger_name LIKE 'KBO_AI_TR_%'
    ) LOOP
        v_sequence_name := REGEXP_SUBSTR(generator_row.trigger_body, 'KBO_AI_SQ_[0-9]+');
        IF v_sequence_name IS NOT NULL THEN
            EXECUTE IMMEDIATE
                'SELECT NVL(MAX(ID), 0) FROM "' || generator_row.table_name || '"'
                INTO v_max_id;
            SELECT last_number
              INTO v_next_value
              FROM user_sequences
             WHERE sequence_name = v_sequence_name;

            v_increment := v_max_id + 1 - v_next_value;
            IF v_increment > 0 THEN
                EXECUTE IMMEDIATE
                    'ALTER SEQUENCE "' || v_sequence_name || '" INCREMENT BY '
                    || TO_CHAR(v_increment);
                EXECUTE IMMEDIATE 'SELECT "' || v_sequence_name || '".NEXTVAL FROM dual'
                    INTO v_advanced_value;
                EXECUTE IMMEDIATE 'ALTER SEQUENCE "' || v_sequence_name || '" INCREMENT BY 1';
            END IF;
        END IF;
    END LOOP;
END;
/
