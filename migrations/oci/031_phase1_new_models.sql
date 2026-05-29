-- 031_phase1_new_models.sql
-- OCI migration: Phase 1 new tables

CREATE TABLE IF NOT EXISTS game_broadcasts (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL,
    broadcaster VARCHAR(50) NOT NULL,
    channel_name VARCHAR(50),
    streaming_platform VARCHAR(50),
    casters JSONB,
    source VARCHAR(20) NOT NULL DEFAULT 'KBO',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(game_id, broadcaster)
);

CREATE INDEX IF NOT EXISTS idx_broadcast_game ON game_broadcasts (game_id);

CREATE TABLE IF NOT EXISTS stadium_info (
    stadium_code VARCHAR(10) PRIMARY KEY,
    name_kr VARCHAR(100) NOT NULL,
    name_en VARCHAR(100),
    home_team_id VARCHAR(10),
    capacity INTEGER,
    opened_year INTEGER,
    location VARCHAR(200),
    address VARCHAR(300),
    parking_info TEXT,
    public_transport JSONB,
    facilities JSONB,
    seat_map_url VARCHAR(500),
    latitude REAL,
    longitude REAL,
    is_dome BOOLEAN NOT NULL DEFAULT FALSE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stadium_regulations (
    id SERIAL PRIMARY KEY,
    stadium_code VARCHAR(10) NOT NULL,
    regulation_type VARCHAR(50) NOT NULL,
    title VARCHAR(200) NOT NULL,
    description TEXT NOT NULL,
    source VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sreg_stadium ON stadium_regulations (stadium_code);

CREATE TABLE IF NOT EXISTS game_mvps (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL,
    player_id INTEGER REFERENCES player_basic(player_id) ON DELETE RESTRICT,
    player_name VARCHAR(100) NOT NULL,
    team_id VARCHAR(10),
    mvp_type VARCHAR(20) NOT NULL DEFAULT 'GAME',
    reason TEXT,
    award_source VARCHAR(20) NOT NULL DEFAULT 'NAVER',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(game_id, mvp_type, player_name)
);

CREATE INDEX IF NOT EXISTS idx_mvp_game ON game_mvps (game_id);
CREATE INDEX IF NOT EXISTS idx_mvp_player ON game_mvps (player_id);

CREATE TABLE IF NOT EXISTS injury_entries (
    id SERIAL PRIMARY KEY,
    player_id INTEGER REFERENCES player_basic(player_id) ON DELETE RESTRICT,
    player_name VARCHAR(100) NOT NULL,
    team_id VARCHAR(10) NOT NULL,
    body_part VARCHAR(50),
    injury_type VARCHAR(100),
    injury_date DATE,
    il_placement_date DATE,
    expected_return_date DATE,
    actual_return_date DATE,
    severity VARCHAR(20),
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    note TEXT,
    source_url VARCHAR(500),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(player_id, il_placement_date)
);

CREATE INDEX IF NOT EXISTS idx_injury_status ON injury_entries (status);
CREATE INDEX IF NOT EXISTS idx_injury_team ON injury_entries (team_id);
CREATE INDEX IF NOT EXISTS idx_injury_player ON injury_entries (player_id);

CREATE TABLE IF NOT EXISTS foreign_player_changes (
    id SERIAL PRIMARY KEY,
    player_id INTEGER REFERENCES player_basic(player_id) ON DELETE RESTRICT,
    player_name VARCHAR(100) NOT NULL,
    team_id VARCHAR(10) NOT NULL,
    season INTEGER NOT NULL,
    change_type VARCHAR(20) NOT NULL,
    previous_team VARCHAR(100),
    replacement_reason VARCHAR(50),
    announcement_date DATE,
    contract_amount VARCHAR(100),
    stats_before_change JSONB,
    note TEXT,
    source_url VARCHAR(500),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(player_name, team_id, season, change_type)
);

CREATE INDEX IF NOT EXISTS idx_fp_team_season ON foreign_player_changes (team_id, season);
CREATE INDEX IF NOT EXISTS idx_fp_player ON foreign_player_changes (player_id);

CREATE TABLE IF NOT EXISTS manager_changes (
    id SERIAL PRIMARY KEY,
    team_id VARCHAR(10) NOT NULL,
    season INTEGER NOT NULL,
    previous_manager VARCHAR(100),
    new_manager VARCHAR(100) NOT NULL,
    change_date DATE,
    change_reason VARCHAR(30),
    note TEXT,
    source_url VARCHAR(500),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(team_id, season, new_manager)
);

CREATE INDEX IF NOT EXISTS idx_mgr_team_season ON manager_changes (team_id, season);

CREATE TABLE IF NOT EXISTS team_rivalries (
    id SERIAL PRIMARY KEY,
    team_id_a VARCHAR(10) NOT NULL,
    team_id_b VARCHAR(10) NOT NULL,
    rivalry_name VARCHAR(100) NOT NULL,
    description TEXT,
    intensity VARCHAR(10) NOT NULL DEFAULT 'MEDIUM',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(team_id_a, team_id_b)
);

CREATE TABLE IF NOT EXISTS cheer_songs (
    id SERIAL PRIMARY KEY,
    team_id VARCHAR(10) NOT NULL,
    player_id INTEGER REFERENCES player_basic(player_id) ON DELETE SET NULL,
    song_name VARCHAR(200) NOT NULL,
    song_type VARCHAR(20) NOT NULL,
    lyrics TEXT,
    description TEXT,
    video_url VARCHAR(500),
    introduction_year INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(team_id, song_name, song_type)
);

CREATE INDEX IF NOT EXISTS idx_cheer_song_team ON cheer_songs (team_id);

CREATE TABLE IF NOT EXISTS cheer_chants (
    id SERIAL PRIMARY KEY,
    team_id VARCHAR(10) NOT NULL,
    chant_text VARCHAR(500) NOT NULL,
    situation VARCHAR(100),
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(team_id, chant_text)
);

CREATE INDEX IF NOT EXISTS idx_cheer_chant_team ON cheer_chants (team_id);
