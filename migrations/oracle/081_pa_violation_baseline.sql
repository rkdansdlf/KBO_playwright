-- 081_pa_violation_baseline.sql
-- Record the season PA violations that are known and unfixable, per check.
-- See migrations/postgresql/064_pa_violation_baseline.sql for the reasoning.
-- Idempotent via user_tables / user_constraints guards.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_exists FROM user_tables WHERE table_name = 'PA_VIOLATION_BASELINE';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE pa_violation_baseline (
                check_id     VARCHAR2(64) NOT NULL,
                scope_season NUMBER       DEFAULT 0 NOT NULL,
                player_id    NUMBER       NOT NULL,
                recorded_at  TIMESTAMP    NOT NULL,
                CONSTRAINT pk_pa_violation_baseline
                    PRIMARY KEY (check_id, scope_season, player_id)
            )';
        EXECUTE IMMEDIATE
            'CREATE INDEX idx_pa_violation_baseline_recorded ON pa_violation_baseline (recorded_at)';
    END IF;

    SELECT COUNT(*) INTO v_exists FROM user_tables WHERE table_name = 'PA_UNRECOVERABLE';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE pa_unrecoverable (
                player_id   NUMBER       NOT NULL,
                season      NUMBER,
                league      VARCHAR2(16),
                source      VARCHAR2(20),
                reason      VARCHAR2(32) NOT NULL,
                detail      VARCHAR2(400),
                recorded_at TIMESTAMP    NOT NULL
            )';
        EXECUTE IMMEDIATE
            'CREATE INDEX idx_pa_unrecoverable_recorded ON pa_unrecoverable (recorded_at)';
    END IF;
END;
/
