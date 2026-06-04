-- 014_phase1_team_event_unique.sql
-- Add UniqueConstraint(team_id, title, source_url) to team_events
-- SQLite requires table recreation for constraint changes

PRAGMA foreign_keys=OFF;

CREATE TABLE team_events_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_scope VARCHAR(10) NOT NULL DEFAULT 'team',
    team_id VARCHAR(10),
    game_id VARCHAR(20) REFERENCES game(game_id) ON DELETE SET NULL,
    stadium_id VARCHAR(10),
    title VARCHAR(300) NOT NULL,
    description TEXT,
    event_type VARCHAR(30),
    event_start_at DATETIME,
    event_end_at DATETIME,
    apply_start_at DATETIME,
    apply_end_at DATETIME,
    location_text VARCHAR(200),
    target_audience VARCHAR(200),
    benefit_text VARCHAR(500),
    image_url VARCHAR(500),
    source_id INTEGER REFERENCES data_sources(id) ON DELETE SET NULL,
    source_url VARCHAR(500),
    published_at DATETIME,
    last_seen_at DATETIME NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'unknown',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(team_id, title, source_url)
);

INSERT INTO team_events_new SELECT * FROM team_events;

DROP TABLE team_events;

ALTER TABLE team_events_new RENAME TO team_events;

CREATE INDEX IF NOT EXISTS idx_team_event_scope ON team_events (event_scope);
CREATE INDEX IF NOT EXISTS idx_team_event_type ON team_events (event_type);
CREATE INDEX IF NOT EXISTS idx_team_event_status ON team_events (status);
CREATE INDEX IF NOT EXISTS idx_team_event_published ON team_events (published_at);
CREATE INDEX IF NOT EXISTS idx_team_event_team ON team_events (team_id);
CREATE INDEX IF NOT EXISTS idx_team_event_game ON team_events (game_id);

PRAGMA foreign_keys=ON;
