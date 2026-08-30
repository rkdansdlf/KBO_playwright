-- Reference SQL Formulas for Pitching Sabermetrics
-- Dialect: SQLite / Standard ANSI SQL
-- Natural Primary Key: (season, player_id, team_id, level, league)

SELECT
    season,
    player_id,
    team_id,
    'KBO' AS league,
    'regular' AS level,
    -- Basic Pitching
    CASE WHEN innings_pitched_outs > 0
         THEN ROUND((CAST(earned_runs AS REAL) * 27.0) / innings_pitched_outs, 2)
         ELSE NULL END AS sql_era,
    CASE WHEN innings_pitched_outs > 0
         THEN ROUND((CAST(walks + hits AS REAL) * 3.0) / innings_pitched_outs, 2)
         ELSE NULL END AS sql_whip,
    -- K/9 = (SO * 27) / IP_outs
    CASE WHEN innings_pitched_outs > 0
         THEN ROUND((CAST(strike_outs AS REAL) * 27.0) / innings_pitched_outs, 2)
         ELSE NULL END AS sql_k_9,
    -- BB/9 = (BB * 27) / IP_outs
    CASE WHEN innings_pitched_outs > 0
         THEN ROUND((CAST(walks AS REAL) * 27.0) / innings_pitched_outs, 2)
         ELSE NULL END AS sql_bb_9,
    -- K/BB
    CASE WHEN walks > 0
         THEN ROUND(CAST(strike_outs AS REAL) / walks, 2)
         ELSE NULL END AS sql_k_bb,
    -- H/9 = (H * 27) / IP_outs
    CASE WHEN innings_pitched_outs > 0
         THEN ROUND((CAST(hits AS REAL) * 27.0) / innings_pitched_outs, 2)
         ELSE NULL END AS sql_h_9,
    -- HR/9 = (HR * 27) / IP_outs
    CASE WHEN innings_pitched_outs > 0
         THEN ROUND((CAST(home_runs AS REAL) * 27.0) / innings_pitched_outs, 2)
         ELSE NULL END AS sql_hr_9
FROM player_season_pitching
ORDER BY season, player_id;
