-- 064_stadium_fk_to_stadiums.sql
-- Convergence phase 2: repoint child FKs from stadium_info to stadiums.
-- Values are identical codes (verified: no orphans), so only the constraint
-- target changes. Constraint names are kept.

ALTER TABLE parking_lots
    DROP CONSTRAINT IF EXISTS parking_lots_stadium_id_fkey,
    ADD CONSTRAINT parking_lots_stadium_id_fkey
    FOREIGN KEY (stadium_id) REFERENCES stadiums(stadium_id) ON DELETE CASCADE;

ALTER TABLE stadium_food_vendors
    DROP CONSTRAINT IF EXISTS stadium_food_vendors_stadium_id_fkey,
    ADD CONSTRAINT stadium_food_vendors_stadium_id_fkey
    FOREIGN KEY (stadium_id) REFERENCES stadiums(stadium_id) ON DELETE CASCADE;

ALTER TABLE stadium_seat_sections
    DROP CONSTRAINT IF EXISTS stadium_seat_sections_stadium_id_fkey,
    ADD CONSTRAINT stadium_seat_sections_stadium_id_fkey
    FOREIGN KEY (stadium_id) REFERENCES stadiums(stadium_id) ON DELETE CASCADE;

ALTER TABLE stadium_congestion
    DROP CONSTRAINT IF EXISTS stadium_congestion_stadium_code_fkey,
    ADD CONSTRAINT stadium_congestion_stadium_code_fkey
    FOREIGN KEY (stadium_code) REFERENCES stadiums(stadium_id) ON DELETE CASCADE;

ALTER TABLE stadium_transit_times
    DROP CONSTRAINT IF EXISTS stadium_transit_times_stadium_code_fkey,
    ADD CONSTRAINT stadium_transit_times_stadium_code_fkey
    FOREIGN KEY (stadium_code) REFERENCES stadiums(stadium_id) ON DELETE CASCADE;

ALTER TABLE stadium_operation_notices
    DROP CONSTRAINT IF EXISTS stadium_operation_notices_stadium_code_fkey,
    ADD CONSTRAINT stadium_operation_notices_stadium_code_fkey
    FOREIGN KEY (stadium_code) REFERENCES stadiums(stadium_id) ON DELETE CASCADE;
