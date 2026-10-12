-- 069_pa_violation_baseline.sql
-- Record the season PA violations that are known and unfixable, per check.
-- See migrations/postgresql/064_pa_violation_baseline.sql for the reasoning.
-- Applied once via version tracking; plain CREATE TABLE IF NOT EXISTS suffices.

CREATE TABLE IF NOT EXISTS pa_violation_baseline (
    check_id TEXT NOT NULL,
    scope_season INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_pa_violation_baseline PRIMARY KEY (check_id, scope_season, player_id)
);

CREATE INDEX IF NOT EXISTS idx_pa_violation_baseline_recorded
    ON pa_violation_baseline (recorded_at);

-- Rows whose counting columns were lost and cannot be recovered. Kept apart from
-- the baseline above because the question asked of them is different.
CREATE TABLE IF NOT EXISTS pa_unrecoverable (
    player_id INTEGER NOT NULL,
    season INTEGER,
    league TEXT,
    source TEXT,
    reason TEXT NOT NULL,
    detail TEXT,
    recorded_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pa_unrecoverable_recorded
    ON pa_unrecoverable (recorded_at);
