-- 053_relay_source_capture_columns.sql
-- Add columns that were previously repaired only during SQLite startup.
-- Apply once to databases created from the pre-lineage schema.

ALTER TABLE game_validation_metrics ADD COLUMN payload_hash_full VARCHAR(64);

ALTER TABLE raw_source_snapshots ADD COLUMN source_url VARCHAR(1000);
ALTER TABLE raw_source_snapshots ADD COLUMN content_type VARCHAR(100);
ALTER TABLE raw_source_snapshots ADD COLUMN raw_size INTEGER;
ALTER TABLE raw_source_snapshots ADD COLUMN capture_metadata JSON;
