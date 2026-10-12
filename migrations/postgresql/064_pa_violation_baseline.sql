-- 064_pa_violation_baseline.sql
-- Record the season PA violations that are known and unfixable, per check.
--
-- These rows cannot be repaired from the pipeline: the page they came from is
-- disallowed by robots.txt and the game-level rows these seasons would be
-- re-derived from are absent. Recomputing the missing counts from the PA
-- identity would fabricate baseball statistics, so they are recorded instead.
--
-- Stored rather than kept on one machine's filesystem because the scheduler does
-- not run where the repository is checked out. A file would make the comparison
-- silently inert on the host that actually runs the daily gate.
--
-- scope_season is part of the key because 452 of the recorded players fail in
-- more than one season: a whole-history record read by a single-season run would
-- report most of the backlog as repaired on every night. Zero stands for "no
-- season filter" rather than a NULL, since a NULL cannot appear in a primary key
-- and every KBO season is four digits.

CREATE TABLE IF NOT EXISTS pa_violation_baseline (
    check_id VARCHAR(64) NOT NULL,
    scope_season INTEGER NOT NULL DEFAULT 0,
    player_id INTEGER NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_pa_violation_baseline PRIMARY KEY (check_id, scope_season, player_id)
);

CREATE INDEX IF NOT EXISTS idx_pa_violation_baseline_recorded
    ON pa_violation_baseline (recorded_at);

COMMENT ON TABLE pa_violation_baseline IS
    'Season PA violations recorded as a known backlog, so a later run can report new damage separately';

-- Rows whose counting columns were lost and cannot be recovered. Kept apart from
-- the baseline above because the question asked of them is different: not "did
-- this get worse" but "is this still the same set of rows nobody can fix".
CREATE TABLE IF NOT EXISTS pa_unrecoverable (
    player_id INTEGER NOT NULL,
    season INTEGER,
    league VARCHAR(16),
    source VARCHAR(20),
    reason VARCHAR(32) NOT NULL,
    detail VARCHAR(400),
    recorded_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pa_unrecoverable_recorded
    ON pa_unrecoverable (recorded_at);

COMMENT ON TABLE pa_unrecoverable IS
    'Season rows missing a count that no lawful source can supply; recorded so they are not mistaken for repaired';
