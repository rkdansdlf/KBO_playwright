-- Oracle port of 042_crawl_runs_unique.sql.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_exists
      FROM user_indexes
     WHERE index_name = 'UQ_CRAWL_RUNS_LABEL_STARTED_AT';
    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE
            'CREATE UNIQUE INDEX UQ_CRAWL_RUNS_LABEL_STARTED_AT '
            || 'ON CRAWL_RUNS (LABEL, STARTED_AT)';
    END IF;
END;
/
