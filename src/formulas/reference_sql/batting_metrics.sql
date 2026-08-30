-- Reference SQL Formulas for Batting and Baserunning Sabermetrics (18 Canonical Metrics)
-- Dialect: SQLite / Standard ANSI SQL
-- Natural Primary Key: (season, player_id, team_code, league, level)

SELECT
    season,
    player_id,
    team_code,
    league,
    level,
    -- 1. AVG = H / AB
    CASE WHEN at_bats > 0 THEN ROUND(CAST(hits AS REAL) / at_bats, 3) ELSE NULL END AS sql_avg,
    -- 2. OBP = (H + BB + HBP) / (AB + BB + HBP + SF)
    CASE WHEN (at_bats + walks + COALESCE(hbp, 0) + COALESCE(sacrifice_flies, 0)) > 0
         THEN ROUND(CAST(hits + walks + COALESCE(hbp, 0) AS REAL) / (at_bats + walks + COALESCE(hbp, 0) + COALESCE(sacrifice_flies, 0)), 3)
         ELSE NULL END AS sql_obp,
    -- 3. SLG = TB / AB
    CASE WHEN at_bats > 0
         THEN ROUND(CAST((hits - COALESCE(doubles,0) - COALESCE(triples,0) - COALESCE(home_runs,0)) + 2*COALESCE(doubles,0) + 3*COALESCE(triples,0) + 4*COALESCE(home_runs,0) AS REAL) / at_bats, 3)
         ELSE NULL END AS sql_slg,
    -- 4. OPS = OBP + SLG
    CASE WHEN at_bats > 0 AND (at_bats + walks + COALESCE(hbp, 0) + COALESCE(sacrifice_flies, 0)) > 0
         THEN ROUND(
            (CAST(hits + walks + COALESCE(hbp, 0) AS REAL) / (at_bats + walks + COALESCE(hbp, 0) + COALESCE(sacrifice_flies, 0))) +
            (CAST((hits - COALESCE(doubles,0) - COALESCE(triples,0) - COALESCE(home_runs,0)) + 2*COALESCE(doubles,0) + 3*COALESCE(triples,0) + 4*COALESCE(home_runs,0) AS REAL) / at_bats), 3)
         ELSE NULL END AS sql_ops,
    -- 5. ISO = SLG - AVG
    CASE WHEN at_bats > 0
         THEN ROUND((CAST((hits - COALESCE(doubles,0) - COALESCE(triples,0) - COALESCE(home_runs,0)) + 2*COALESCE(doubles,0) + 3*COALESCE(triples,0) + 4*COALESCE(home_runs,0) AS REAL) / at_bats) - (CAST(hits AS REAL) / at_bats), 3)
         ELSE NULL END AS sql_iso,
    -- 6. BABIP_BAT = (H - HR) / (AB - SO - HR + SF)
    CASE WHEN (at_bats - strikeouts - home_runs + COALESCE(sacrifice_flies, 0)) > 0
         THEN ROUND(CAST(hits - home_runs AS REAL) / (at_bats - strikeouts - home_runs + COALESCE(sacrifice_flies, 0)), 3)
         ELSE NULL END AS sql_babip_bat,
    -- 7. BB_PCT_BAT = BB / PA
    CASE WHEN plate_appearances > 0
         THEN ROUND(CAST(walks AS REAL) / plate_appearances, 3)
         ELSE NULL END AS sql_bb_pct_bat,
    -- 8. K_PCT_BAT = SO / PA
    CASE WHEN plate_appearances > 0
         THEN ROUND(CAST(strikeouts AS REAL) / plate_appearances, 3)
         ELSE NULL END AS sql_k_pct_bat,
    -- 9. BB_TO_K_BAT = BB / SO
    CASE WHEN strikeouts > 0
         THEN ROUND(CAST(walks AS REAL) / strikeouts, 2)
         ELSE NULL END AS sql_bb_to_k_bat,
    -- 10. GPA = (1.8 * OBP + SLG) / 4.0
    CASE WHEN at_bats > 0 AND (at_bats + walks + COALESCE(hbp, 0) + COALESCE(sacrifice_flies, 0)) > 0
         THEN ROUND((1.8 * (CAST(hits + walks + COALESCE(hbp, 0) AS REAL) / (at_bats + walks + COALESCE(hbp, 0) + COALESCE(sacrifice_flies, 0))) +
                     (CAST((hits - COALESCE(doubles,0) - COALESCE(triples,0) - COALESCE(home_runs,0)) + 2*COALESCE(doubles,0) + 3*COALESCE(triples,0) + 4*COALESCE(home_runs,0) AS REAL) / at_bats)) / 4.0, 3)
         ELSE NULL END AS sql_gpa,
    -- 11. SecA = (BB + (TB - H) + (SB - CS)) / AB
    CASE WHEN at_bats > 0
         THEN ROUND(CAST(walks + (((hits - COALESCE(doubles,0) - COALESCE(triples,0) - COALESCE(home_runs,0)) + 2*COALESCE(doubles,0) + 3*COALESCE(triples,0) + 4*COALESCE(home_runs,0)) - hits) + (stolen_bases - COALESCE(caught_stealing, 0)) AS REAL) / at_bats, 3)
         ELSE NULL END AS sql_seca,
    -- 12. RC = (H + BB - CS + HBP - GDP) * (TB + 0.26*(BB - IBB + HBP) + 0.52*(SH + SF + SB)) / (AB + BB + HBP + SH + SF)
    CASE WHEN (at_bats + walks + COALESCE(hbp, 0) + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)) > 0
         THEN ROUND(CAST(hits + walks - COALESCE(caught_stealing, 0) + COALESCE(hbp, 0) - COALESCE(gdp, 0) AS REAL) *
                    (((hits - COALESCE(doubles,0) - COALESCE(triples,0) - COALESCE(home_runs,0)) + 2*COALESCE(doubles,0) + 3*COALESCE(triples,0) + 4*COALESCE(home_runs,0)) +
                     0.26 * (walks - COALESCE(intentional_walks, 0) + COALESCE(hbp, 0)) +
                     0.52 * (COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0) + COALESCE(stolen_bases, 0))) /
                    (at_bats + walks + COALESCE(hbp, 0) + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)), 2)
         ELSE NULL END AS sql_rc,
    -- 13. SB_PCT = SB / (SB + CS)
    CASE WHEN (stolen_bases + COALESCE(caught_stealing, 0)) > 0
         THEN ROUND(CAST(stolen_bases AS REAL) / (stolen_bases + COALESCE(caught_stealing, 0)), 3)
         ELSE NULL END AS sql_sb_pct
FROM player_season_batting
ORDER BY season DESC, plate_appearances DESC;
