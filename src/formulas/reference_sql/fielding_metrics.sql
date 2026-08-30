-- Reference SQL Formulas for Fielding Sabermetrics (2 Canonical Metrics)
-- Dialect: SQLite / Standard ANSI SQL
-- Natural Primary Key: (year, player_id, team_id, position_id)

SELECT
    year AS season,
    player_id,
    team_id AS team_code,
    'REGULAR' AS league,
    'KBO1' AS level,
    position_id,
    -- 1. FPCT = (PO + A) / (PO + A + E)
    CASE WHEN (COALESCE(putouts, 0) + COALESCE(assists, 0) + COALESCE(errors, 0)) > 0
         THEN ROUND(CAST(COALESCE(putouts, 0) + COALESCE(assists, 0) AS REAL) / (COALESCE(putouts, 0) + COALESCE(assists, 0) + COALESCE(errors, 0)), 3)
         ELSE NULL END AS sql_fpct,
    -- 2. RF_9 = (PO + A) * 9 / Innings
    CASE WHEN innings > 0
         THEN ROUND((CAST(COALESCE(putouts, 0) + COALESCE(assists, 0) AS REAL) * 9.0) / innings, 2)
         ELSE NULL END AS sql_rf_9
FROM player_season_fielding
ORDER BY year DESC, games DESC;
