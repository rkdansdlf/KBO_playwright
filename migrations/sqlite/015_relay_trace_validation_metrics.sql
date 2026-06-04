-- 015_relay_trace_validation_metrics.sql
-- Delta migration for existing DBs that already applied 010 before raw PBP
-- trace columns and validation evidence were introduced.

ALTER TABLE game_play_by_play ADD COLUMN provider_log_id VARCHAR(64);
ALTER TABLE game_play_by_play ADD COLUMN source_row_index INTEGER;
ALTER TABLE game_play_by_play ADD COLUMN source_name VARCHAR(32);

ALTER TABLE game_validation_metrics ADD COLUMN evidence_json JSON;

CREATE INDEX IF NOT EXISTS idx_game_play_by_play_provider_log
    ON game_play_by_play(game_id, provider_log_id);
CREATE INDEX IF NOT EXISTS idx_game_validation_metrics_status
    ON game_validation_metrics(validation_status);
