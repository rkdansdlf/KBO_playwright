-- 057_crawl_dead_letters.sql
-- Replayable crawl failure queue keyed by dlq_id and scoped per original run.

CREATE TABLE IF NOT EXISTS crawl_dead_letters (
    id SERIAL PRIMARY KEY,
    dlq_id VARCHAR(36) NOT NULL UNIQUE,
    original_run_id VARCHAR(36) NOT NULL,
    crawler VARCHAR(64) NOT NULL,
    target_type VARCHAR(32) NOT NULL,
    target_id VARCHAR(128),
    season INTEGER,
    game_id VARCHAR(20),
    failure_stage VARCHAR(16) NOT NULL,
    error_code VARCHAR(64) NOT NULL,
    error_message TEXT,
    error_type VARCHAR(128),
    source_url VARCHAR(1000),
    payload_ref VARCHAR(1000),
    snapshot_id INTEGER,
    evidence_id INTEGER,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 5,
    next_retry_at TIMESTAMP,
    status VARCHAR(16) NOT NULL DEFAULT 'pending',
    replay_run_id VARCHAR(36),
    resolved_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_crawl_dead_letters_incident
        UNIQUE (crawler, target_type, target_id, original_run_id)
);

CREATE INDEX IF NOT EXISTS idx_crawl_dead_letters_status_retry
    ON crawl_dead_letters (status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_crawl_dead_letters_original_run
    ON crawl_dead_letters (original_run_id);
CREATE INDEX IF NOT EXISTS idx_crawl_dead_letters_crawler_status
    ON crawl_dead_letters (crawler, status);
CREATE INDEX IF NOT EXISTS idx_crawl_dead_letters_error_code
    ON crawl_dead_letters (error_code);
CREATE INDEX IF NOT EXISTS idx_crawl_dead_letters_next_retry
    ON crawl_dead_letters (next_retry_at);
