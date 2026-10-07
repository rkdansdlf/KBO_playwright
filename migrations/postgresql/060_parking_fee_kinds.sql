-- 060_parking_fee_kinds.sql
-- Fee kinds printed on stadium parking pages (기본/추가/일일/행사/경기/무료).
-- `parking_fee_rules` is keyed by vehicle class, so these kinds never had a
-- table of their own; they lived only in the raw snapshot text. Idempotent:
-- safe when SQLAlchemy create_all already created the ORM table.

CREATE TABLE IF NOT EXISTS parking_fee_kinds (
    id SERIAL PRIMARY KEY,
    parking_lot_id INTEGER NOT NULL REFERENCES parking_lots(id) ON DELETE CASCADE,
    fee_kind VARCHAR(16) NOT NULL,
    amount_krw INTEGER NOT NULL,
    source_url VARCHAR(500),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(parking_lot_id, fee_kind)
);
CREATE INDEX IF NOT EXISTS idx_parking_fee_kinds_lot
    ON parking_fee_kinds(parking_lot_id);
