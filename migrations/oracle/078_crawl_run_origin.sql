-- 078_crawl_run_origin.sql
-- Which subsystem started a crawl execution.
-- See migrations/postgresql/061_crawl_run_origin.sql for the full reasoning: the
-- ledger records what ran and whether it worked, not who asked for it, and the
-- ledger is the projection every crawl alert reads. `checkpoint` was rejected
-- because it is written only on terminal paths -- a run stranded in `running`
-- by a dead process would carry nothing -- and because it is the crawler's own
-- progress channel, so a later payload would overwrite it.
--
-- Nullable and not backfilled on purpose: a row that predates this never
-- recorded a caller, and NULL says exactly that.
--
-- Oracle has no `ADD COLUMN IF NOT EXISTS`, so existence is checked in
-- user_tab_columns first -- the same guard shape as the rest of this chain.

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_tab_columns
     WHERE table_name = 'CRAWL_EXECUTION_RUNS'
       AND column_name = 'ORIGIN';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE CRAWL_EXECUTION_RUNS ADD (ORIGIN VARCHAR2(32 CHAR))';
    END IF;
END;
/

-- A stranded `running` row is whose caller decides who sweeps it. A dead process
-- never writes the terminal columns that would otherwise narrow the search, so
-- the index is on the caller alone.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_indexes
     WHERE index_name = 'IDX_CRAWL_EXECUTION_RUNS_ORIGIN';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE 'CREATE INDEX IDX_CRAWL_EXECUTION_RUNS_ORIGIN ON CRAWL_EXECUTION_RUNS (ORIGIN)';
    END IF;
END;
/
