-- 056_notification_incidents.sql
-- Durable incident ledger for the in-process notification alert manager.
-- Idempotent: safe when SQLAlchemy create_all already created the ORM table.

CREATE TABLE IF NOT EXISTS notification_incidents (
    id SERIAL PRIMARY KEY,
    incident_key VARCHAR(255) NOT NULL UNIQUE,
    source VARCHAR(64) NOT NULL,
    component VARCHAR(128) NOT NULL DEFAULT '',
    severity VARCHAR(16) NOT NULL,
    state VARCHAR(16) NOT NULL DEFAULT 'OPEN',
    title VARCHAR(255) NOT NULL DEFAULT '',
    message TEXT NOT NULL DEFAULT '',
    details_hash VARCHAR(64) NOT NULL DEFAULT '',
    occurrence_count INTEGER NOT NULL DEFAULT 1,
    notification_count INTEGER NOT NULL DEFAULT 0,
    first_opened_at TIMESTAMP NOT NULL,
    last_seen_at TIMESTAMP NOT NULL,
    last_notified_at TIMESTAMP,
    resolved_at TIMESTAMP,
    metadata JSON,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_notification_incidents_state_severity
    ON notification_incidents(state, severity);
CREATE INDEX IF NOT EXISTS idx_notification_incidents_source_last_seen
    ON notification_incidents(source, last_seen_at);
