-- 066_crawl_run_origin.sql
-- Which subsystem started a crawl execution.
-- See migrations/postgresql/061_crawl_run_origin.sql for the full reasoning: the
-- ledger records what ran and whether it worked, not who asked for it, and the
-- ledger is the projection every crawl alert reads. `checkpoint` was rejected
-- because it is written only on terminal paths -- a run stranded in `running`
-- by a dead process would carry nothing -- and because it is the crawler's own
-- progress channel, so a later payload would overwrite it.
--
-- Nullable and not backfilled on purpose: a row that predates this never
-- recorded a caller, and NULL says exactly that. Idempotent: safe when
-- SQLAlchemy create_all already created the ORM column.
--
-- SQLite has no `ADD COLUMN IF NOT EXISTS`, so re-running against an
-- already-migrated database raises. This chain is applied once by version
-- tracking, which is what makes the missing guard acceptable here.

ALTER TABLE crawl_execution_runs ADD COLUMN origin VARCHAR(32);

-- A stranded `running` row is whose caller decides who sweeps it. A dead process
-- never writes the terminal columns that would otherwise narrow the search, so
-- the index is on the caller alone.
CREATE INDEX IF NOT EXISTS idx_crawl_execution_runs_origin
    ON crawl_execution_runs(origin);
