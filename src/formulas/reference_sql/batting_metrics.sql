-- Reference SQL Formulas for Batting Sabermetrics
-- Dialect: SQLite / Standard ANSI SQL
-- Natural Primary Key: (season, player_id, team_id, level, league)

SELECT
    season,
    player_id,
    team_id,
    'KBO' AS league,
    'regular' AS level,
    -- Basic Batting
    CASE WHEN at_bats > 0 THEN ROUND(CAST(hits AS REAL) / at_bats, 3) ELSE NULL END AS sql_avg,
    CASE WHEN (at_bats + walks + COALESCE(hit_by_pitch, 0) + COALESCE(sacrifice_flies, 0)) > 0
         THEN ROUND(CAST(hits + walks + COALESCE(hit_by_pitch, 0) AS REAL) / (at_bats + walks + COALESCE(hit_by_pitch, 0) + COALESCE(sacrifice_flies, 0)), 3)
         ELSE NULL END AS sql_obp,
    CASE WHEN at_bats > 0
         THEN ROUND(CAST(hits + doubles + 2 * triples + 3 * home_runs AS REAL) / at_bats, 3)
         ELSE NULL END AS sql_slg,
    -- OPS = OBP + SLG
    CASE WHEN at_bats > 0 AND (at_bats + walks + COALESCE(hit_by_pitch, 0) + COALESCE(sacrifice_flies, 0)) > 0
         THEN ROUND(
            (CAST(hits + walks + COALESCE(hit_by_pitch, 0) AS REAL) / (at_bats + walks + COALESCE(hit_by_pitch, 0) + COALESCE(sacrifice_flies, 0))) +
            (CAST(hits + doubles + 2 * triples + 3 * home_runs AS REAL) / at_bats), 3)
         ELSE NULL END AS sql_ops,
    -- ISO = SLG - AVG
    CASE WHEN at_bats > 0
         THEN ROUND(CAST(doubles + 2 * triples + 3 * home_runs AS REAL) / at_bats, 3)
         ELSE NULL END AS sql_iso,
    -- BABIP = (H - HR) / (AB - SO - HR + SF)
    CASE WHEN (at_bats - strike_outs - home_runs + COALESCE(sacrifice_flies, 0)) > 0
         THEN ROUND(CAST(hits - home_runs AS REAL) / (at_bats - strike_outs - home_runs + COALESCE(sacrifice_flies, 0)), 3)
         ELSE NULL END AS sql_babip,
    -- BB% = BB / PA
    CASE WHEN (at_bats + walks + COALESCE(hit_by_pitch, 0) + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)) > 0
         THEN ROUND(CAST(walks AS REAL) / (at_bats + walks + COALESCE(hit_by_pitch, 0) + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)), 3)
         ELSE NULL END AS sql_bb_pct,
    -- K% = SO / PA
    CASE WHEN (at_bats + walks + COALESCE(hit_by_pitch, 0) + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)) > 0
         THEN ROUND(CAST(strike_outs AS REAL) / (at_bats + walks + COALESCE(hit_by_pitch, 0) + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)), 3)
         ELSE NULL END AS sql_k_pct,
    -- BB / K
    CASE WHEN strike_outs > 0
         THEN ROUND(CAST(walks AS REAL) / strike_outs, 2)
         ELSE NULL END AS sql_bb_k
FROM player_season_batting
ORDER BY season, player_id;
