-- Reference SQL Formulas for Fielding and Baserunning Sabermetrics
-- Dialect: SQLite / Standard ANSI SQL
-- Natural Primary Key: (season, player_id, team_id, level, league)

-- Baserunning: SB% = SB / (SB + CS) from player_season_baserunning
-- (Metric Category: Baserunning, Source Table: player_season_baserunning)
SELECT
    year AS season,
    player_id,
    team_id,
    'KBO' AS league,
    'regular' AS level,
    CASE WHEN (stolen_bases + caught_stealing) > 0
         THEN ROUND(CAST(stolen_bases AS REAL) / (stolen_bases + caught_stealing), 3)
         ELSE NULL END AS sql_sb_pct
FROM player_season_baserunning;

-- Fielding: FPCT = (PO + A) / (PO + A + E) from player_season_fielding
-- (Metric Category: Fielding, Source Table: player_season_fielding)
SELECT
    year AS season,
    player_id,
    team_id,
    'KBO' AS league,
    'regular' AS level,
    position_id,
    CASE WHEN (putouts + assists + errors) > 0
         THEN ROUND(CAST(putouts + assists AS REAL) / (putouts + assists + errors), 3)
         ELSE NULL END AS sql_fpct
FROM player_season_fielding;
