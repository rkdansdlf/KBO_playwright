-- 049_quality_gate_and_projection_tables.sql
-- Quality-gate quarantine, correction audit trail, and player projection storage.
-- Idempotent: safe when SQLAlchemy create_all already created the ORM tables.

CREATE TABLE IF NOT EXISTS quarantined_records (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(20),
    entity_type VARCHAR(32) NOT NULL,
    entity_id VARCHAR(64),
    rule_id VARCHAR(32) NOT NULL,
    severity VARCHAR(16) NOT NULL,
    failure_reason TEXT NOT NULL,
    raw_payload JSON NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'kbo_official',
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    retry_count INTEGER NOT NULL DEFAULT 0,
    resolved_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_quarantined_records_game_id ON quarantined_records(game_id);
CREATE INDEX IF NOT EXISTS ix_quarantined_records_entity_type ON quarantined_records(entity_type);
CREATE INDEX IF NOT EXISTS ix_quarantined_records_rule_id ON quarantined_records(rule_id);
CREATE INDEX IF NOT EXISTS ix_quarantined_records_status ON quarantined_records(status);

CREATE TABLE IF NOT EXISTS correction_audit_trail (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(20),
    entity_type VARCHAR(32) NOT NULL,
    entity_id VARCHAR(64),
    field_name VARCHAR(64) NOT NULL,
    raw_value TEXT,
    raw_source VARCHAR(64) NOT NULL,
    corrected_value TEXT,
    corrected_source VARCHAR(64) NOT NULL,
    correction_reason TEXT NOT NULL,
    confidence FLOAT NOT NULL DEFAULT 1.0,
    extra_metadata JSON,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_correction_audit_trail_game_id ON correction_audit_trail(game_id);
CREATE INDEX IF NOT EXISTS ix_correction_audit_trail_entity_type ON correction_audit_trail(entity_type);

CREATE TABLE IF NOT EXISTS player_projections (
    id SERIAL PRIMARY KEY,
    target_season INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    player_name VARCHAR(64) NOT NULL,
    team_code VARCHAR(10),
    position_type VARCHAR(16) NOT NULL,
    age INTEGER,
    projected_pa FLOAT,
    projected_ip NUMERIC(6, 2),
    projected_avg FLOAT,
    projected_obp FLOAT,
    projected_slg FLOAT,
    projected_ops FLOAT,
    projected_woba FLOAT,
    projected_era FLOAT,
    projected_fip FLOAT,
    projected_whip FLOAT,
    projected_stats JSON NOT NULL,
    weights_used JSON NOT NULL,
    regression_params JSON NOT NULL,
    version VARCHAR(32) NOT NULL DEFAULT 'marcel-v1',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_player_projection UNIQUE (target_season, player_id, position_type, version)
);
CREATE INDEX IF NOT EXISTS ix_player_projections_target_season ON player_projections(target_season);
CREATE INDEX IF NOT EXISTS ix_player_projections_player_id ON player_projections(player_id);
