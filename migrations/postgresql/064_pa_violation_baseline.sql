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
-- scope_season is the season the row was found in, not the scope of the run that
-- recorded it. 452 of the recorded players fail in more than one season, so a
-- record holding bare player ids cannot tell a --year run which rows are its
-- own: every row it does not name would read as repaired, 714 of them, every
-- night. Storing the season with each row is what lets a season run select
-- exactly the rows it is compared against.
--
-- It is NOT NULL, so a whole-history recording writes one row per season rather
-- than a single row meaning "everything". Zero therefore means a row whose own
-- season is genuinely unknown, which cannot happen for these tables.

CREATE TABLE IF NOT EXISTS pa_violation_baseline (
    check_id VARCHAR(64) NOT NULL,
    scope_season INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_pa_violation_baseline PRIMARY KEY (check_id, scope_season, player_id)
);

CREATE INDEX IF NOT EXISTS idx_pa_violation_baseline_recorded
    ON pa_violation_baseline (recorded_at);

-- How much of a run's own scope is covered by the record. Read before trusting a
-- whole-history comparison: a record taken for one season would report every
-- other season's violations as new damage, and those counts are deducted from
-- the quality score.
CREATE INDEX IF NOT EXISTS idx_pa_violation_baseline_season
    ON pa_violation_baseline (scope_season);

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
