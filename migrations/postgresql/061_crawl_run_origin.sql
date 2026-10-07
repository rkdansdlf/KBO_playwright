-- 061_crawl_run_origin.sql
-- Which subsystem started a crawl execution.
--
-- The ledger recorded what ran and whether it worked, but not who asked for it.
-- A day of `schedule` runs at a two-minute cadence was therefore
-- indistinguishable from the daily pipeline's single run, and the ledger is the
-- projection every crawl alert reads -- so the gap was not just an attribution
-- convenience, it was an observability one.
--
-- `checkpoint` was rejected for this: it is written only on terminal paths, so
-- a run stranded in `running` by a dead process (the case most worth
-- attributing) would carry nothing, and a later progress payload would
-- overwrite it.
--
-- Nullable and not backfilled on purpose: rows written before this existed
-- never recorded a caller, and NULL says exactly that. Idempotent: safe when
-- SQLAlchemy create_all already created the ORM column.

ALTER TABLE crawl_execution_runs
    ADD COLUMN IF NOT EXISTS origin VARCHAR(32);

-- A stranded `running` row is whose caller decides who sweeps it. A dead process
-- never writes the terminal columns that would otherwise narrow the search, so
-- the index is on the caller alone.
CREATE INDEX IF NOT EXISTS idx_crawl_execution_runs_origin
    ON crawl_execution_runs(origin);
