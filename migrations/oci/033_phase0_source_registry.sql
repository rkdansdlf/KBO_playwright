-- 033_phase0_source_registry.sql
-- OCI migration: Phase 0 - Source Registry

CREATE TABLE IF NOT EXISTS data_sources (
    id SERIAL PRIMARY KEY,
    source_key VARCHAR(100) NOT NULL UNIQUE,
    source_type VARCHAR(30) NOT NULL,
    team_id VARCHAR(10),
    stadium_id VARCHAR(10),
    target_domain VARCHAR(30) NOT NULL,
    reliability VARCHAR(10) NOT NULL DEFAULT 'medium',
    parser_name VARCHAR(100),
    crawl_frequency VARCHAR(30),
    base_url VARCHAR(500),
    last_success_at TIMESTAMP,
    last_content_hash VARCHAR(64),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ds_target_domain ON data_sources (target_domain);
CREATE INDEX IF NOT EXISTS idx_ds_team ON data_sources (team_id);
CREATE INDEX IF NOT EXISTS idx_ds_stadium ON data_sources (stadium_id);

CREATE TABLE IF NOT EXISTS raw_source_snapshots (
    id SERIAL PRIMARY KEY,
    data_source_id INTEGER NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    fetched_at TIMESTAMP NOT NULL,
    content_hash VARCHAR(64),
    raw_html_or_json_path VARCHAR(500),
    status_code INTEGER,
    parse_status VARCHAR(20) NOT NULL DEFAULT 'pending',
    parser_version VARCHAR(30),
    reprocess_status VARCHAR(20),
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(data_source_id, content_hash)
);

CREATE INDEX IF NOT EXISTS idx_rss_data_source ON raw_source_snapshots (data_source_id);
CREATE INDEX IF NOT EXISTS idx_rss_fetched_at ON raw_source_snapshots (fetched_at);
CREATE INDEX IF NOT EXISTS idx_rss_parse_status ON raw_source_snapshots (parse_status);
