-- 036_phase1_team_event_unique.sql
-- OCI migration: Add UniqueConstraint(team_id, title, source_url) to team_events

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_team_event'
    ) THEN
        ALTER TABLE team_events ADD CONSTRAINT uq_team_event UNIQUE (team_id, title, source_url);
    END IF;
END $$;
