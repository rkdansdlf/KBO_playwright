-- Phase 105A Gate 2 Sample Selection Queries

-- Batting Sample Selection
SELECT * FROM player_season_batting
WHERE (level = '1군' OR level = 'KBO1' OR level IS NULL)
  AND (league = 'REGULAR' OR league IS NULL)
  AND (source != 'ROLLUP' OR source IS NULL)
  AND at_bats IS NOT NULL
ORDER BY season DESC, plate_appearances DESC
LIMIT 500;

-- Pitching Sample Selection
SELECT * FROM player_season_pitching
WHERE (level = '1군' OR level = 'KBO1' OR level IS NULL)
  AND (league = 'REGULAR' OR league IS NULL)
  AND (source != 'ROLLUP' OR source IS NULL)
  AND (innings_outs > 0 OR innings_pitched > 0)
ORDER BY season DESC, innings_outs DESC
LIMIT 500;
