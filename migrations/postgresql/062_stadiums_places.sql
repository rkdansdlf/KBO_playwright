-- 062_stadiums_places.sql
-- Requested master tables: stadiums (dimensions/turf/bullpen/park factor/phone)
-- and places (in-stadium amenities). Mirrors the production table shapes.
-- Idempotent: safe when SQLAlchemy create_all already created the ORM tables.

CREATE TABLE IF NOT EXISTS stadiums (
    stadium_id VARCHAR(10) PRIMARY KEY,
    stadium_name VARCHAR(100),
    city VARCHAR(100),
    team VARCHAR(20),
    capacity INTEGER,
    seating_capacity INTEGER,
    open_year INTEGER,
    left_fence_m DOUBLE PRECISION,
    center_fence_m DOUBLE PRECISION,
    fence_height_m DOUBLE PRECISION,
    turf_type VARCHAR(20),
    bullpen_type VARCHAR(20),
    homerun_park_factor DOUBLE PRECISION,
    notes VARCHAR(500),
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    address VARCHAR(300),
    phone VARCHAR(30),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS places (
    id SERIAL PRIMARY KEY,
    stadium_id VARCHAR(10) NOT NULL REFERENCES stadiums(stadium_id) ON DELETE CASCADE,
    category VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    description VARCHAR(500),
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    address VARCHAR(300),
    phone VARCHAR(30),
    rating NUMERIC,
    open_time VARCHAR(50),
    close_time VARCHAR(50),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_places_stadium ON places(stadium_id);
CREATE INDEX IF NOT EXISTS idx_places_category ON places(category);
