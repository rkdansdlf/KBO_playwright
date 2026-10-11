-- 063_stadiums_dimensions.sql
-- Promote free-text geometry out of stadiums.notes: right fence distance,
-- dome flag, and the validity window (e.g. Jamsil rebuild, Sajik re-fence).

ALTER TABLE stadiums ADD COLUMN IF NOT EXISTS right_fence_m DOUBLE PRECISION;
ALTER TABLE stadiums ADD COLUMN IF NOT EXISTS is_dome BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE stadiums ADD COLUMN IF NOT EXISTS valid_from_year INTEGER;
ALTER TABLE stadiums ADD COLUMN IF NOT EXISTS valid_to_year INTEGER;
