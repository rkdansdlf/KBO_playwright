CREATE UNIQUE INDEX IF NOT EXISTS uq_crawl_runs_label_started_at
    ON crawl_runs (label, started_at);
