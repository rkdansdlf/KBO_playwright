-- 069_pa_violation_baseline.sql
-- Record the season PA violations that are known and unfixable, per check.
-- See migrations/postgresql/064_pa_violation_baseline.sql for the reasoning.
-- Applied once via version tracking; plain CREATE TABLE IF NOT EXISTS suffices.

CREATE TABLE IF NOT EXISTS pa_violation_baseline (
    check_id TEXT NOT NULL,
    scope_season INTEGER NOT NULL DEFAULT 0,
    player_id INTEGER NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_pa_violation_baseline PRIMARY KEY (check_id, scope_season, player_id)
);

CREATE INDEX IF NOT EXISTS idx_pa_violation_baseline_recorded
    ON pa_violation_baseline (recorded_at);
