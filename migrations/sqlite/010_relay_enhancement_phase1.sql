-- 010_relay_enhancement_phase1.sql
-- Phase 1: Relay enhancement — dedup, mapping keys, validation state, lifecycle state

-- 1. GameEvent: add dedup / mapping columns + at-bat / pitch count columns
ALTER TABLE game_events ADD COLUMN provider_log_id VARCHAR(64);
ALTER TABLE game_events ADD COLUMN source_row_index INTEGER;
ALTER TABLE game_events ADD COLUMN at_bat_seq INTEGER;
ALTER TABLE game_events ADD COLUMN at_bat_event_role VARCHAR(16);
ALTER TABLE game_events ADD COLUMN at_bat_confidence VARCHAR(16);
ALTER TABLE game_events ADD COLUMN balls INTEGER DEFAULT 0;
ALTER TABLE game_events ADD COLUMN strikes INTEGER DEFAULT 0;

-- 2. GamePlayByPlay: add player_id FK (nullable) + resolver metadata
ALTER TABLE game_play_by_play ADD COLUMN player_id INTEGER REFERENCES player_basic(player_id) ON DELETE RESTRICT;
ALTER TABLE game_play_by_play ADD COLUMN resolver_confidence VARCHAR(16);
ALTER TABLE game_play_by_play ADD COLUMN resolver_reason VARCHAR(64);
ALTER TABLE game_play_by_play ADD COLUMN unresolved_player_name VARCHAR(64);
ALTER TABLE game_play_by_play ADD COLUMN provider_log_id VARCHAR(64);
ALTER TABLE game_play_by_play ADD COLUMN source_row_index INTEGER;
ALTER TABLE game_play_by_play ADD COLUMN source_name VARCHAR(32);

-- 3. Game: add lifecycle state column
ALTER TABLE game ADD COLUMN game_lifecycle_state VARCHAR(32);

-- 4. GameValidationMetrics: new tracking table
CREATE TABLE IF NOT EXISTS game_validation_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id VARCHAR(20) NOT NULL REFERENCES game(game_id) ON DELETE CASCADE,
    validation_status VARCHAR(32) NOT NULL DEFAULT 'pending_live',
    previous_status VARCHAR(32),
    source_used VARCHAR(16),
    fallback_trigger_count INTEGER DEFAULT 0,
    fallback_trigger_reason VARCHAR(64),
    last_fallback_at DATETIME,
    duplicate_event_count INTEGER DEFAULT 0,
    unclassified_event_count INTEGER DEFAULT 0,
    finish_mismatch_count INTEGER DEFAULT 0,
    last_successful_event_at DATETIME,
    parser_version VARCHAR(32),
    source_schema_version VARCHAR(32),
    payload_hash VARCHAR(16),
    evidence_json JSON,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(game_id)
);

-- 5. Performance indexes
CREATE INDEX IF NOT EXISTS idx_game_events_provider_log
    ON game_events(game_id, provider_log_id);
CREATE INDEX IF NOT EXISTS idx_game_events_at_bat
    ON game_events(game_id, at_bat_seq);
CREATE INDEX IF NOT EXISTS idx_game_play_by_play_event_type
    ON game_play_by_play(game_id, event_type);
CREATE INDEX IF NOT EXISTS idx_game_play_by_play_provider_log
    ON game_play_by_play(game_id, provider_log_id);
CREATE INDEX IF NOT EXISTS idx_game_validation_metrics_game
    ON game_validation_metrics(game_id);
CREATE INDEX IF NOT EXISTS idx_game_validation_metrics_status
    ON game_validation_metrics(validation_status);
CREATE INDEX IF NOT EXISTS idx_game_lifecycle_state
    ON game(game_lifecycle_state);
