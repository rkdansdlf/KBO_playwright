-- Migration 032: Fix float columns wrongly typed as INTEGER in team_season_fielding and team_season_baserunning
-- Affected columns:
--   team_season_fielding:   def_innings, fielding_pct, range_factor_per_game
--   team_season_baserunning: sb_success_rate

-- team_season_fielding: fix def_innings, fielding_pct, range_factor_per_game
DO $$
BEGIN
    -- def_innings
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'team_season_fielding' AND column_name = 'def_innings'
          AND data_type IN ('integer', 'bigint', 'smallint')
    ) THEN
        ALTER TABLE team_season_fielding ALTER COLUMN def_innings TYPE DOUBLE PRECISION USING def_innings::DOUBLE PRECISION;
        RAISE NOTICE 'team_season_fielding.def_innings -> DOUBLE PRECISION';
    END IF;

    -- fielding_pct
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'team_season_fielding' AND column_name = 'fielding_pct'
          AND data_type IN ('integer', 'bigint', 'smallint')
    ) THEN
        ALTER TABLE team_season_fielding ALTER COLUMN fielding_pct TYPE DOUBLE PRECISION USING fielding_pct::DOUBLE PRECISION;
        RAISE NOTICE 'team_season_fielding.fielding_pct -> DOUBLE PRECISION';
    END IF;

    -- range_factor_per_game
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'team_season_fielding' AND column_name = 'range_factor_per_game'
          AND data_type IN ('integer', 'bigint', 'smallint')
    ) THEN
        ALTER TABLE team_season_fielding ALTER COLUMN range_factor_per_game TYPE DOUBLE PRECISION USING range_factor_per_game::DOUBLE PRECISION;
        RAISE NOTICE 'team_season_fielding.range_factor_per_game -> DOUBLE PRECISION';
    END IF;
END $$;

-- team_season_baserunning: fix sb_success_rate
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'team_season_baserunning' AND column_name = 'sb_success_rate'
          AND data_type IN ('integer', 'bigint', 'smallint')
    ) THEN
        ALTER TABLE team_season_baserunning ALTER COLUMN sb_success_rate TYPE DOUBLE PRECISION USING sb_success_rate::DOUBLE PRECISION;
        RAISE NOTICE 'team_season_baserunning.sb_success_rate -> DOUBLE PRECISION';
    END IF;
END $$;
