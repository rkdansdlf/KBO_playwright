-- 052_crawl_evidence.sql
-- Immutable source artifacts and source-to-database lineage.

CREATE TABLE IF NOT EXISTS crawl_evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type VARCHAR(32) NOT NULL,
    entity_id VARCHAR(128) NOT NULL,
    dataset VARCHAR(64) NOT NULL,
    source_name VARCHAR(128) NOT NULL,
    source_url VARCHAR(1000),
    captured_at DATETIME NOT NULL,
    raw_artifact_path VARCHAR(1000),
    parsed_payload_path VARCHAR(1000),
    normalized_payload_path VARCHAR(1000),
    raw_hash VARCHAR(64),
    parsed_hash VARCHAR(64),
    normalized_hash VARCHAR(64),
    db_projection_hash VARCHAR(64),
    parser_version VARCHAR(64),
    normalization_version VARCHAR(64),
    validation_status VARCHAR(32) NOT NULL DEFAULT 'captured',
    diff_summary JSON,
    capture_metadata JSON,
    error_message TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_crawl_evidence_entity
    ON crawl_evidence(entity_type, entity_id, dataset);
CREATE INDEX IF NOT EXISTS idx_crawl_evidence_status
    ON crawl_evidence(validation_status);
CREATE INDEX IF NOT EXISTS idx_crawl_evidence_raw_hash
    ON crawl_evidence(raw_hash);
