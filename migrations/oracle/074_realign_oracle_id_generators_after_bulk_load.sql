-- Re-align ORM ID generators after explicit-ID bulk loads.
--
-- The original alignment migrations ran before later bulk loads and cannot
-- protect a subsequent NULL-ID insert when a sequence falls behind MAX(ID).
-- This migration is idempotent: a generator already ahead of MAX(ID) is left
-- unchanged, while a stale generator is advanced to the next safe value.
DECLARE
    v_sequence_name VARCHAR2(30);
    v_sequence_exists NUMBER;
    v_current_value NUMBER;
    v_max_id NUMBER;
    v_increment NUMBER;
    v_advanced_value NUMBER;
BEGIN
    FOR generator_row IN (
        SELECT table_name, trigger_name
          FROM user_triggers
         WHERE trigger_name LIKE 'KBO_AI_TR_%'
    ) LOOP
        v_sequence_name := 'KBO_AI_SQ_' || REGEXP_SUBSTR(generator_row.trigger_name, '[0-9]+$');

        SELECT COUNT(*)
          INTO v_sequence_exists
          FROM user_sequences
         WHERE sequence_name = v_sequence_name;

        IF v_sequence_exists = 1 THEN
            EXECUTE IMMEDIATE 'SELECT "' || v_sequence_name || '".NEXTVAL FROM dual'
                INTO v_current_value;
            EXECUTE IMMEDIATE
                'SELECT NVL(MAX(ID), 0) FROM "' || generator_row.table_name || '"'
                INTO v_max_id;

            IF v_current_value <= v_max_id THEN
                v_increment := v_max_id + 1 - v_current_value;
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
