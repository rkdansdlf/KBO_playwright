-- 034_phase1_p0_models.sql
-- OCI migration: Phase 1 (P0) - Team events, roster transactions, ticket prices, ticket open rules

CREATE TABLE IF NOT EXISTS team_events (
    id SERIAL PRIMARY KEY,
    event_scope VARCHAR(10) NOT NULL DEFAULT 'team',
    team_id VARCHAR(10),
    game_id VARCHAR(20) REFERENCES game(game_id) ON DELETE SET NULL,
    stadium_id VARCHAR(10),
    title VARCHAR(300) NOT NULL,
    description TEXT,
    event_type VARCHAR(30),
    event_start_at TIMESTAMP,
    event_end_at TIMESTAMP,
    apply_start_at TIMESTAMP,
    apply_end_at TIMESTAMP,
    location_text VARCHAR(200),
    target_audience VARCHAR(200),
    benefit_text VARCHAR(500),
    image_url VARCHAR(500),
    source_id INTEGER REFERENCES data_sources(id) ON DELETE SET NULL,
    source_url VARCHAR(500),
    published_at TIMESTAMP,
    last_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
    status VARCHAR(20) NOT NULL DEFAULT 'unknown',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_team_event_scope ON team_events (event_scope);
CREATE INDEX IF NOT EXISTS idx_team_event_team ON team_events (team_id);
CREATE INDEX IF NOT EXISTS idx_team_event_game ON team_events (game_id);
CREATE INDEX IF NOT EXISTS idx_team_event_type ON team_events (event_type);
CREATE INDEX IF NOT EXISTS idx_team_event_status ON team_events (status);
CREATE INDEX IF NOT EXISTS idx_team_event_published ON team_events (published_at);

CREATE TABLE IF NOT EXISTS roster_transactions (
    id SERIAL PRIMARY KEY,
    transaction_date DATE NOT NULL,
    team_id VARCHAR(10) NOT NULL,
    player_id INTEGER REFERENCES player_basic(player_id) ON DELETE RESTRICT,
    player_name VARCHAR(100) NOT NULL,
    action VARCHAR(20) NOT NULL,
    roster_level VARCHAR(20) NOT NULL DEFAULT 'first_team',
    inferred_to_level VARCHAR(20),
    source_type VARCHAR(30) NOT NULL DEFAULT 'kbo_today_page',
    source_id INTEGER REFERENCES data_sources(id) ON DELETE SET NULL,
    before_snapshot_id INTEGER REFERENCES raw_source_snapshots(id) ON DELETE SET NULL,
    after_snapshot_id INTEGER REFERENCES raw_source_snapshots(id) ON DELETE SET NULL,
    confidence VARCHAR(10) NOT NULL DEFAULT 'high',
    dedupe_key VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rt_date ON roster_transactions (transaction_date);
CREATE INDEX IF NOT EXISTS idx_rt_team ON roster_transactions (team_id);
CREATE INDEX IF NOT EXISTS idx_rt_date_team ON roster_transactions (transaction_date, team_id);
CREATE INDEX IF NOT EXISTS idx_rt_action ON roster_transactions (action);
CREATE INDEX IF NOT EXISTS idx_rt_player ON roster_transactions (player_id);

CREATE TABLE IF NOT EXISTS ticket_prices (
    id SERIAL PRIMARY KEY,
    team_id VARCHAR(10) NOT NULL,
    stadium_id VARCHAR(10) NOT NULL,
    season INTEGER NOT NULL,
    seat_grade VARCHAR(50) NOT NULL,
    day_type VARCHAR(20) NOT NULL DEFAULT 'weekday',
    audience_type VARCHAR(30),
    price INTEGER NOT NULL,
    currency VARCHAR(10) NOT NULL DEFAULT 'KRW',
    source_id INTEGER REFERENCES data_sources(id) ON DELETE SET NULL,
    source_url VARCHAR(500),
    effective_from DATE,
    effective_to DATE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(team_id, season, seat_grade, day_type, audience_type)
);

CREATE INDEX IF NOT EXISTS idx_tp_team ON ticket_prices (team_id);
CREATE INDEX IF NOT EXISTS idx_tp_stadium ON ticket_prices (stadium_id);
CREATE INDEX IF NOT EXISTS idx_tp_season ON ticket_prices (season);

CREATE TABLE IF NOT EXISTS ticket_open_rules (
    id SERIAL PRIMARY KEY,
    team_id VARCHAR(10) NOT NULL,
    platform VARCHAR(50) NOT NULL,
    open_offset_days INTEGER NOT NULL,
    open_time TIME NOT NULL,
    sales_close_rule TEXT,
    max_tickets_per_user INTEGER,
    fee_rule TEXT,
    cancel_rule TEXT,
    source_id INTEGER REFERENCES data_sources(id) ON DELETE SET NULL,
    note VARCHAR(500),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(team_id, platform, open_offset_days, open_time)
);

CREATE INDEX IF NOT EXISTS idx_tor_team ON ticket_open_rules (team_id);
CREATE INDEX IF NOT EXISTS idx_tor_platform ON ticket_open_rules (platform);
