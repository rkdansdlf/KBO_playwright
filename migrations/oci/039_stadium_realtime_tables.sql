-- 039_stadium_realtime_tables.sql
-- OCI PostgreSQL migration: Jamsil stadium real-time data pipeline
-- Tables: stadium_transit_times, stadium_congestion, stadium_operation_notices

-- ─────────────────────────────────────────────
-- 1. stadium_transit_times
--    Measured travel duration from transit hubs to the stadium on game days.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stadium_transit_times (
    id                 SERIAL       PRIMARY KEY,
    stadium_code       VARCHAR(10)  NOT NULL REFERENCES stadium_info(stadium_code) ON DELETE CASCADE,
    origin_label       VARCHAR(100) NOT NULL,
    origin_lat         REAL,
    origin_lng         REAL,
    transport_mode     VARCHAR(20)  NOT NULL,   -- subway / bus / walk / car / mixed
    measured_at        TIMESTAMP    NOT NULL,
    game_date          DATE         NOT NULL,
    duration_minutes   INTEGER      NOT NULL,
    distance_meters    INTEGER,
    congestion_factor  REAL,                    -- 1.0 = normal
    source_api         VARCHAR(30)  NOT NULL,   -- kakao / naver / tmap / google
    raw_response       JSONB,
    created_at         TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMP    NOT NULL DEFAULT NOW(),
    UNIQUE(stadium_code, origin_label, transport_mode, measured_at)
);

CREATE INDEX IF NOT EXISTS idx_stt_stadium     ON stadium_transit_times (stadium_code);
CREATE INDEX IF NOT EXISTS idx_stt_game_date   ON stadium_transit_times (game_date);
CREATE INDEX IF NOT EXISTS idx_stt_measured_at ON stadium_transit_times (measured_at);
CREATE INDEX IF NOT EXISTS idx_stt_origin      ON stadium_transit_times (origin_label);
CREATE INDEX IF NOT EXISTS idx_stt_mode        ON stadium_transit_times (transport_mode);

-- ─────────────────────────────────────────────
-- 2. stadium_congestion
--    Real-time congestion measurements at gate / subway / road / parking.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stadium_congestion (
    id                 SERIAL       PRIMARY KEY,
    stadium_code       VARCHAR(10)  NOT NULL REFERENCES stadium_info(stadium_code) ON DELETE CASCADE,
    location_type      VARCHAR(30)  NOT NULL,   -- gate / subway_station / road / parking / area
    location_label     VARCHAR(100) NOT NULL,
    measured_at        TIMESTAMP    NOT NULL,
    game_date          DATE         NOT NULL,
    congestion_level   VARCHAR(20)  NOT NULL,   -- low / normal / high / very_high
    congestion_index   REAL,                    -- 0~100
    people_count       INTEGER,
    source             VARCHAR(50)  NOT NULL,   -- seoul_open_api / sdot / kakao / naver / manual
    raw_data           JSONB,
    created_at         TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMP    NOT NULL DEFAULT NOW(),
    UNIQUE(stadium_code, location_label, measured_at)
);

CREATE INDEX IF NOT EXISTS idx_sc_stadium       ON stadium_congestion (stadium_code);
CREATE INDEX IF NOT EXISTS idx_sc_game_date     ON stadium_congestion (game_date);
CREATE INDEX IF NOT EXISTS idx_sc_measured_at   ON stadium_congestion (measured_at);
CREATE INDEX IF NOT EXISTS idx_sc_location_type ON stadium_congestion (location_type);
CREATE INDEX IF NOT EXISTS idx_sc_level         ON stadium_congestion (congestion_level);

-- ─────────────────────────────────────────────
-- 3. stadium_operation_notices
--    Official game-day notices: gate changes, cancellations, entry rules, etc.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stadium_operation_notices (
    id              SERIAL       PRIMARY KEY,
    stadium_code    VARCHAR(10)  NOT NULL REFERENCES stadium_info(stadium_code) ON DELETE CASCADE,
    notice_type     VARCHAR(30)  NOT NULL,   -- GATE_CHANGE / CANCEL / DELAY / ENTRY_RULE / WEATHER / EVENT / PARKING / GENERAL
    title           VARCHAR(500) NOT NULL,
    content         TEXT,
    published_at    TIMESTAMP,
    game_date       DATE,
    source_name     VARCHAR(100) NOT NULL,   -- LG트윈스공식 / 두산베어스공식 / KBO
    source_url      VARCHAR(500),
    external_id     VARCHAR(200),
    is_urgent       BOOLEAN NOT NULL DEFAULT FALSE,
    is_confirmed    BOOLEAN NOT NULL DEFAULT TRUE,
    raw_snapshot    JSONB,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(stadium_code, source_name, external_id),
    UNIQUE(stadium_code, source_name, title, published_at)
);

CREATE INDEX IF NOT EXISTS idx_son_stadium      ON stadium_operation_notices (stadium_code);
CREATE INDEX IF NOT EXISTS idx_son_game_date    ON stadium_operation_notices (game_date);
CREATE INDEX IF NOT EXISTS idx_son_published_at ON stadium_operation_notices (published_at);
CREATE INDEX IF NOT EXISTS idx_son_notice_type  ON stadium_operation_notices (notice_type);
CREATE INDEX IF NOT EXISTS idx_son_source       ON stadium_operation_notices (source_name);
CREATE INDEX IF NOT EXISTS idx_son_urgent       ON stadium_operation_notices (is_urgent);
