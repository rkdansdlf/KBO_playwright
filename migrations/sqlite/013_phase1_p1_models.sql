-- 013_phase1_p1_models.sql
-- Phase 1 (P1): Stadium seat sections, parking lots, parking fee rules, food vendors, food menu items

CREATE TABLE IF NOT EXISTS stadium_seat_sections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stadium_id VARCHAR(10) NOT NULL REFERENCES stadium_info(stadium_code) ON DELETE CASCADE,
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

CREATE INDEX IF NOT EXISTS idx_ss_stadium ON stadium_seat_sections (stadium_id);
CREATE INDEX IF NOT EXISTS idx_ss_grade ON stadium_seat_sections (seat_grade);
CREATE INDEX IF NOT EXISTS idx_ss_side ON stadium_seat_sections (base_side);

CREATE TABLE IF NOT EXISTS parking_lots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stadium_id VARCHAR(10) NOT NULL REFERENCES stadium_info(stadium_code) ON DELETE CASCADE,
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

CREATE INDEX IF NOT EXISTS idx_pl_stadium ON parking_lots (stadium_id);
CREATE INDEX IF NOT EXISTS idx_pl_type ON parking_lots (lot_type);

CREATE TABLE IF NOT EXISTS parking_fee_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parking_lot_id INTEGER NOT NULL REFERENCES parking_lots(id) ON DELETE CASCADE,
    vehicle_type VARCHAR(20) NOT NULL,
    base_fee INTEGER NOT NULL,
    base_minutes INTEGER NOT NULL,
    additional_fee INTEGER,
    additional_minutes INTEGER,
    daily_max_fee INTEGER,
    event_flat_fee INTEGER,
    discount_json TEXT,
    free_exit_minutes INTEGER,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(parking_lot_id, vehicle_type)
);

CREATE INDEX IF NOT EXISTS idx_pfr_lot ON parking_fee_rules (parking_lot_id);

CREATE TABLE IF NOT EXISTS stadium_food_vendors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stadium_id VARCHAR(10) NOT NULL REFERENCES stadium_info(stadium_code) ON DELETE CASCADE,
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

CREATE INDEX IF NOT EXISTS idx_sfv_stadium ON stadium_food_vendors (stadium_id);
CREATE INDEX IF NOT EXISTS idx_sfv_confidence ON stadium_food_vendors (confidence);

CREATE TABLE IF NOT EXISTS stadium_food_menu_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_id INTEGER NOT NULL REFERENCES stadium_food_vendors(id) ON DELETE CASCADE,
    menu_name VARCHAR(200) NOT NULL,
    price INTEGER,
    category VARCHAR(30),
    is_signature BOOLEAN NOT NULL DEFAULT 0,
    tags_json TEXT,
    source_id INTEGER REFERENCES data_sources(id) ON DELETE SET NULL,
    effective_from DATE,
    effective_to DATE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(vendor_id, menu_name)
);

CREATE INDEX IF NOT EXISTS idx_sfmi_vendor ON stadium_food_menu_items (vendor_id);
CREATE INDEX IF NOT EXISTS idx_sfmi_category ON stadium_food_menu_items (category);
