-- 055_crawl_execution_runs.sql
-- Generic crawl execution run ledger keyed by run_id.

CREATE TABLE IF NOT EXISTS crawl_execution_runs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(36) NOT NULL UNIQUE,
    crawler VARCHAR(64) NOT NULL,
    target_type VARCHAR(32) NOT NULL,
    target_id VARCHAR(128),
    season INTEGER,
    game_id VARCHAR(20),
    status VARCHAR(16) NOT NULL DEFAULT 'running',
    attempt INTEGER NOT NULL DEFAULT 1,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    records_read INTEGER NOT NULL DEFAULT 0,
    records_written INTEGER NOT NULL DEFAULT 0,
    records_failed INTEGER NOT NULL DEFAULT 0,
    error_code VARCHAR(64),
    error_message TEXT,
    checkpoint JSON,
    source_url VARCHAR(1000),
    parser_version VARCHAR(64),
    snapshot_id INTEGER,
    evidence_id INTEGER,
    parent_run_id VARCHAR(36),
    replay_of_run_id VARCHAR(36),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_crawl_execution_runs_crawler
    ON crawl_execution_runs (crawler, started_at);
CREATE INDEX IF NOT EXISTS idx_crawl_execution_runs_status
    ON crawl_execution_runs (status);
CREATE INDEX IF NOT EXISTS idx_crawl_execution_runs_game
    ON crawl_execution_runs (game_id);
CREATE INDEX IF NOT EXISTS idx_crawl_execution_runs_parent
    ON crawl_execution_runs (parent_run_id);
CREATE INDEX IF NOT EXISTS idx_crawl_execution_runs_replay_of
    ON crawl_execution_runs (replay_of_run_id);
