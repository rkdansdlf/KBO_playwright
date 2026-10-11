-- 068_stadiums_dimensions.sql
-- Promote free-text geometry out of stadiums.notes: right fence distance,
-- dome flag, and the validity window (e.g. Jamsil rebuild, Sajik re-fence).
-- Applied once via version tracking; plain ADD COLUMN is sufficient.

ALTER TABLE stadiums ADD COLUMN right_fence_m REAL;
ALTER TABLE stadiums ADD COLUMN is_dome BOOLEAN NOT NULL DEFAULT 0;
ALTER TABLE stadiums ADD COLUMN valid_from_year INTEGER;
ALTER TABLE stadiums ADD COLUMN valid_to_year INTEGER;
