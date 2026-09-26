-- 058_crawl_dead_letters_status_updated_index.sql
-- Supports stale-retrying recovery scans.

CREATE INDEX IF NOT EXISTS idx_crawl_dead_letters_status_updated
    ON crawl_dead_letters (status, updated_at);
