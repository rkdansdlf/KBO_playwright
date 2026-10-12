-- 069_stadium_fk_to_stadiums.sql
-- Convergence phase 2: repoint child FKs from stadium_info to stadiums.
-- SQLite cannot ALTER a foreign key, so each table is rebuilt (copy out,
-- recreate, copy back). Values are identical codes (verified: no orphans).
-- Applied once via version tracking.

-- ── stadium_seat_sections ──
CREATE TABLE stadium_seat_sections_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stadium_id VARCHAR(10) NOT NULL REFERENCES stadiums(stadium_id) ON DELETE CASCADE,
    section_code VARCHAR(50),
    section_name VARCHAR(100) NOT NULL,
    seat_grade VARCHAR(50),
    base_side VARCHAR(20),
    floor_level VARCHAR(50),
    gate_info VARCHAR(100),
    is_home_cheering BOOLEAN NOT NULL DEFAULT 0,
    is_away_cheering BOOLEAN NOT NULL DEFAULT 0,
    is_table_seat BOOLEAN NOT NULL DEFAULT 0,
    is_family_seat BOOLEAN NOT NULL DEFAULT 0,
    is_wheelchair_accessible BOOLEAN NOT NULL DEFAULT 0,
    price_grade_key VARCHAR(50),
    seat_map_url VARCHAR(500),
    geometry_json TEXT,
    source_id INTEGER REFERENCES data_sources(id) ON DELETE SET NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stadium_id, section_code),
    UNIQUE(stadium_id, section_name)
);
INSERT INTO stadium_seat_sections_new SELECT * FROM stadium_seat_sections;
DROP TABLE stadium_seat_sections;
ALTER TABLE stadium_seat_sections_new RENAME TO stadium_seat_sections;
UPDATE sqlite_sequence SET seq = (SELECT MAX(id) FROM stadium_seat_sections) WHERE name = 'stadium_seat_sections';
CREATE INDEX IF NOT EXISTS idx_ss_stadium ON stadium_seat_sections (stadium_id);
CREATE INDEX IF NOT EXISTS idx_ss_grade ON stadium_seat_sections (seat_grade);
CREATE INDEX IF NOT EXISTS idx_ss_side ON stadium_seat_sections (base_side);

-- ── parking_lots ──
CREATE TABLE parking_lots_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stadium_id VARCHAR(10) NOT NULL REFERENCES stadiums(stadium_id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    lot_type VARCHAR(20) NOT NULL DEFAULT 'official',
    address VARCHAR(300),
    latitude REAL,
    longitude REAL,
    capacity INTEGER,
    walking_minutes INTEGER,
    is_event_day_available BOOLEAN NOT NULL DEFAULT 1,
    reservation_required BOOLEAN NOT NULL DEFAULT 0,
    operating_hours VARCHAR(200),
    source_id INTEGER REFERENCES data_sources(id) ON DELETE SET NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stadium_id, name)
);
INSERT INTO parking_lots_new SELECT * FROM parking_lots;
DROP TABLE parking_lots;
ALTER TABLE parking_lots_new RENAME TO parking_lots;
UPDATE sqlite_sequence SET seq = (SELECT MAX(id) FROM parking_lots) WHERE name = 'parking_lots';
CREATE INDEX IF NOT EXISTS idx_pl_stadium ON parking_lots (stadium_id);
CREATE INDEX IF NOT EXISTS idx_pl_type ON parking_lots (lot_type);

-- ── stadium_food_vendors ──
CREATE TABLE stadium_food_vendors_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stadium_id VARCHAR(10) NOT NULL REFERENCES stadiums(stadium_id) ON DELETE CASCADE,
    vendor_name VARCHAR(100) NOT NULL,
    location_text VARCHAR(200),
    floor_level VARCHAR(50),
    base_side VARCHAR(20),
    gate_info VARCHAR(100),
    order_method VARCHAR(20) DEFAULT 'onsite',
    source_id INTEGER REFERENCES data_sources(id) ON DELETE SET NULL,
    confidence VARCHAR(10) NOT NULL DEFAULT 'medium',
    last_verified_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stadium_id, vendor_name)
);
INSERT INTO stadium_food_vendors_new SELECT * FROM stadium_food_vendors;
DROP TABLE stadium_food_vendors;
ALTER TABLE stadium_food_vendors_new RENAME TO stadium_food_vendors;
UPDATE sqlite_sequence SET seq = (SELECT MAX(id) FROM stadium_food_vendors) WHERE name = 'stadium_food_vendors';
CREATE INDEX IF NOT EXISTS idx_sfv_stadium ON stadium_food_vendors (stadium_id);
CREATE INDEX IF NOT EXISTS idx_sfv_confidence ON stadium_food_vendors (confidence);

-- ── stadium_transit_times ──
CREATE TABLE stadium_transit_times_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stadium_code VARCHAR(10) NOT NULL REFERENCES stadiums(stadium_id) ON DELETE CASCADE,
    origin_label VARCHAR(100) NOT NULL,
    origin_lat REAL,
    origin_lng REAL,
    transport_mode VARCHAR(20) NOT NULL,
    measured_at DATETIME NOT NULL,
    game_date DATE NOT NULL,
    duration_minutes INTEGER NOT NULL,
    distance_meters INTEGER,
    congestion_factor REAL,
    source_api VARCHAR(30) NOT NULL,
    raw_response TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stadium_code, origin_label, transport_mode, measured_at)
);
INSERT INTO stadium_transit_times_new SELECT * FROM stadium_transit_times;
DROP TABLE stadium_transit_times;
ALTER TABLE stadium_transit_times_new RENAME TO stadium_transit_times;
UPDATE sqlite_sequence SET seq = (SELECT MAX(id) FROM stadium_transit_times) WHERE name = 'stadium_transit_times';
CREATE INDEX IF NOT EXISTS idx_stt_stadium ON stadium_transit_times (stadium_code);
CREATE INDEX IF NOT EXISTS idx_stt_game_date ON stadium_transit_times (game_date);
CREATE INDEX IF NOT EXISTS idx_stt_measured_at ON stadium_transit_times (measured_at);
CREATE INDEX IF NOT EXISTS idx_stt_origin ON stadium_transit_times (origin_label);
CREATE INDEX IF NOT EXISTS idx_stt_mode ON stadium_transit_times (transport_mode);

-- ── stadium_congestion ──
CREATE TABLE stadium_congestion_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stadium_code VARCHAR(10) NOT NULL REFERENCES stadiums(stadium_id) ON DELETE CASCADE,
    location_type VARCHAR(30) NOT NULL,
    location_label VARCHAR(100) NOT NULL,
    measured_at DATETIME NOT NULL,
    game_date DATE NOT NULL,
    congestion_level VARCHAR(20) NOT NULL,
    congestion_index REAL,
    people_count INTEGER,
    source VARCHAR(50) NOT NULL,
    raw_data TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stadium_code, location_label, measured_at)
);
INSERT INTO stadium_congestion_new SELECT * FROM stadium_congestion;
DROP TABLE stadium_congestion;
ALTER TABLE stadium_congestion_new RENAME TO stadium_congestion;
UPDATE sqlite_sequence SET seq = (SELECT MAX(id) FROM stadium_congestion) WHERE name = 'stadium_congestion';
CREATE INDEX IF NOT EXISTS idx_sc_stadium ON stadium_congestion (stadium_code);
CREATE INDEX IF NOT EXISTS idx_sc_game_date ON stadium_congestion (game_date);
CREATE INDEX IF NOT EXISTS idx_sc_measured_at ON stadium_congestion (measured_at);
CREATE INDEX IF NOT EXISTS idx_sc_location_type ON stadium_congestion (location_type);
CREATE INDEX IF NOT EXISTS idx_sc_level ON stadium_congestion (congestion_level);

-- ── stadium_operation_notices ──
CREATE TABLE stadium_operation_notices_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stadium_code VARCHAR(10) NOT NULL REFERENCES stadiums(stadium_id) ON DELETE CASCADE,
    notice_type VARCHAR(30) NOT NULL,
    title VARCHAR(500) NOT NULL,
    content TEXT,
    published_at DATETIME,
    game_date DATE,
    source_name VARCHAR(100) NOT NULL,
    source_url VARCHAR(500),
    external_id VARCHAR(200),
    is_urgent BOOLEAN NOT NULL DEFAULT 0,
    is_confirmed BOOLEAN NOT NULL DEFAULT 1,
    raw_snapshot TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stadium_code, source_name, external_id),
    UNIQUE(stadium_code, source_name, title, published_at)
);
INSERT INTO stadium_operation_notices_new SELECT * FROM stadium_operation_notices;
DROP TABLE stadium_operation_notices;
ALTER TABLE stadium_operation_notices_new RENAME TO stadium_operation_notices;
UPDATE sqlite_sequence SET seq = (SELECT MAX(id) FROM stadium_operation_notices) WHERE name = 'stadium_operation_notices';
CREATE INDEX IF NOT EXISTS idx_son_stadium ON stadium_operation_notices (stadium_code);
CREATE INDEX IF NOT EXISTS idx_son_game_date ON stadium_operation_notices (game_date);
CREATE INDEX IF NOT EXISTS idx_son_published_at ON stadium_operation_notices (published_at);
CREATE INDEX IF NOT EXISTS idx_son_notice_type ON stadium_operation_notices (notice_type);
CREATE INDEX IF NOT EXISTS idx_son_source ON stadium_operation_notices (source_name);
CREATE INDEX IF NOT EXISTS idx_son_urgent ON stadium_operation_notices (is_urgent);
