-- 054: Widen columns whose SQLite production values exceed the ORM limits.
-- players.salary_amount stores KRW salaries above int4 range (observed 4.2B).
-- player_season_batting/pitching.source stores values like FINAL_VERIFICATION (18 chars).
-- game_play_by_play.batter_name occasionally holds a full play description (observed 80 chars).
ALTER TABLE players ALTER COLUMN salary_amount TYPE BIGINT;
ALTER TABLE player_basic ALTER COLUMN salary_amount TYPE BIGINT;
ALTER TABLE player_season_batting ALTER COLUMN source TYPE VARCHAR(32);
ALTER TABLE player_season_pitching ALTER COLUMN source TYPE VARCHAR(32);
ALTER TABLE game_play_by_play ALTER COLUMN batter_name TYPE VARCHAR(100);
