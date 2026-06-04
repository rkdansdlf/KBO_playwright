-- Prevent duplicate crawl run entries with same label and start time.

CREATE UNIQUE INDEX IF NOT EXISTS uq_crawl_runs_label_started_at
    ON crawl_runs (label, started_at);
