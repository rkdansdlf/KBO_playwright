-- Reference SQL Formulas for Pitching Sabermetrics (13 Canonical Metrics)
-- Dialect: SQLite / Standard ANSI SQL
-- Natural Primary Key: (season, player_id, team_code, league, level)

SELECT
    season,
    player_id,
    team_code,
    league,
    level,
    -- 1. ERA = (ER * 27) / IP_outs
    CASE WHEN innings_outs > 0
         THEN ROUND((CAST(earned_runs AS REAL) * 27.0) / innings_outs, 2)
         ELSE NULL END AS sql_era,
    -- 2. WHIP = (BB + H) * 3 / IP_outs
    CASE WHEN innings_outs > 0
         THEN ROUND((CAST(walks_allowed + hits_allowed AS REAL) * 3.0) / innings_outs, 2)
         ELSE NULL END AS sql_whip,
    -- 3. K/9 = (SO * 27) / IP_outs
    CASE WHEN innings_outs > 0
         THEN ROUND((CAST(strikeouts AS REAL) * 27.0) / innings_outs, 2)
         ELSE NULL END AS sql_k_9,
    -- 4. BB/9 = (BB * 27) / IP_outs
    CASE WHEN innings_outs > 0
         THEN ROUND((CAST(walks_allowed AS REAL) * 27.0) / innings_outs, 2)
         ELSE NULL END AS sql_bb_9,
    -- 5. HR/9 = (HR * 27) / IP_outs
    CASE WHEN innings_outs > 0
         THEN ROUND((CAST(home_runs_allowed AS REAL) * 27.0) / innings_outs, 2)
         ELSE NULL END AS sql_hr_9,
    -- 6. K_PCT_PIT = SO / TBF
    CASE WHEN tbf > 0
         THEN ROUND(CAST(strikeouts AS REAL) / tbf, 3)
         ELSE NULL END AS sql_k_pct_pit,
    -- 7. BB_PCT_PIT = BB / TBF
    CASE WHEN tbf > 0
         THEN ROUND(CAST(walks_allowed AS REAL) / tbf, 3)
         ELSE NULL END AS sql_bb_pct_pit,
    -- 8. K_BB_PIT = (SO - BB) / TBF
    CASE WHEN tbf > 0
         THEN ROUND(CAST(strikeouts - walks_allowed AS REAL) / tbf, 3)
         ELSE NULL END AS sql_k_bb_pit,
    -- 9. BABIP_PIT = (H - HR) / (TBF - SO - HR + SF)
    CASE WHEN (tbf - strikeouts - home_runs_allowed + COALESCE(sacrifice_flies_allowed, 0)) > 0
         THEN ROUND(CAST(hits_allowed - home_runs_allowed AS REAL) / (tbf - strikeouts - home_runs_allowed + COALESCE(sacrifice_flies_allowed, 0)), 3)
         ELSE NULL END AS sql_babip_pit,
    -- 10. LOB_PCT = (H + BB + HBP - R) / (H + BB + HBP - 1.4*HR)
    CASE WHEN (hits_allowed + walks_allowed + COALESCE(hit_batters, 0) - (1.4 * home_runs_allowed)) > 0
         THEN ROUND(CAST(hits_allowed + walks_allowed + COALESCE(hit_batters, 0) - runs_allowed AS REAL) / (hits_allowed + walks_allowed + COALESCE(hit_batters, 0) - (1.4 * home_runs_allowed)), 3)
         ELSE NULL END AS sql_lob_pct,
    -- 11. DICE = 3.00 + ((13*HR + 3*(BB + HBP) - 2*SO) * 3 / IP_outs)
    CASE WHEN innings_outs > 0
         THEN ROUND(3.00 + ((13.0 * home_runs_allowed + 3.0 * (walks_allowed + COALESCE(hit_batters, 0)) - 2.0 * strikeouts) * 3.0 / innings_outs), 2)
         ELSE NULL END AS sql_dice
FROM player_season_pitching
ORDER BY season DESC, innings_outs DESC;
