-- 064_notification_deliveries.sql
-- Append-only delivery audit ledger for the notification subsystem.
-- `incident_id` is a soft reference (no FK) so incident retention and delivery
-- retention stay independent. Idempotent: safe when SQLAlchemy create_all
-- already created the ORM table.

CREATE TABLE IF NOT EXISTS notification_deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id INTEGER,
    notification_type VARCHAR(32) NOT NULL DEFAULT 'notification',
    batch_id VARCHAR(36) NOT NULL DEFAULT '',
    channel VARCHAR(32) NOT NULL,
    destination VARCHAR(255),
    status VARCHAR(32) NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 1,
    dispatched_at DATETIME NOT NULL,
    completed_at DATETIME,
    latency_ms INTEGER,
    error_code VARCHAR(64),
    error_message TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_notification_deliveries_incident
    ON notification_deliveries(incident_id, dispatched_at);
CREATE INDEX IF NOT EXISTS idx_notification_deliveries_channel_status
    ON notification_deliveries(channel, status, dispatched_at);
CREATE INDEX IF NOT EXISTS idx_notification_deliveries_batch
    ON notification_deliveries(batch_id);
