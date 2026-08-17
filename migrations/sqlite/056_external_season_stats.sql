-- 056_external_season_stats.sql
-- Provider-specific season statistics with canonical-player lineage.
-- Idempotent: safe when SQLAlchemy create_all already created the ORM table.

CREATE TABLE IF NOT EXISTS external_season_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_record_key VARCHAR(64) NOT NULL UNIQUE,
    provider VARCHAR(32) NOT NULL,
    source_key VARCHAR(100) NOT NULL,
    stat_type VARCHAR(16) NOT NULL,
    season INTEGER NOT NULL,
    league VARCHAR(16) NOT NULL DEFAULT 'REGULAR',
    level VARCHAR(16) NOT NULL DEFAULT 'KBO1',
    external_player_id VARCHAR(64),
    player_id INTEGER REFERENCES player_basic(player_id) ON DELETE RESTRICT,
    player_name VARCHAR(100) NOT NULL,
    team_name VARCHAR(100),
    team_code VARCHAR(10),
    metrics JSON NOT NULL,
    metric_metadata JSON,
    source_url VARCHAR(1000) NOT NULL,
    content_hash VARCHAR(64),
    fetched_at DATETIME NOT NULL,
    parser_version VARCHAR(32) NOT NULL,
    resolution_status VARCHAR(24) NOT NULL DEFAULT 'unresolved',
    resolution_note TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_external_stats_provider_season
    ON external_season_stats(provider, season, stat_type);
CREATE INDEX IF NOT EXISTS idx_external_stats_player
    ON external_season_stats(player_id, season, stat_type);
CREATE INDEX IF NOT EXISTS idx_external_stats_resolution
    ON external_season_stats(resolution_status);
