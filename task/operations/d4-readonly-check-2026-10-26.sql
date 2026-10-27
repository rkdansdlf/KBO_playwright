-- D4 READ-ONLY DATABASE CHECK RECONSTRUCTION
-- Source file: d4-readonly-check-2026-10-26.sql (original ABSENT; reconstructed)
-- Mode: READ-ONLY (default_transaction_read_only=on, rollback after)
-- Filter: KIA (team=HT -> KIA code) / Season 2026 (season_id=2026) / Pitcher filter team_code='KIA'
-- No raw row dumps; aggregate statistics only; no player names; no connection details in output.

-- ============================================================
-- Q1. 2025 / 2026 시즌 매핑 (kbo_seasons.season_id <-> game.season_id)
-- ============================================================
SELECT 'Q1-A' AS query_part,
       s.season_id AS kbo_seasons_season_id,
       s.year,
       s.league_type_code,
       s.league_type_name,
       s.start_date,
       s.end_date
FROM kbo_seasons s
WHERE s.season_id IN (2025, 259, 202600, 202601, 202602, 202603, 202604, 202605, 2026)
ORDER BY s.season_id;

SELECT 'Q1-B' AS query_part,
       g.season_id AS game_season_id,
       g.game_status,
       COUNT(*) AS cnt
FROM game g
WHERE g.season_id IN (2025, 259, 202600, 202601, 202602, 202603, 202604, 202605, 2026)
GROUP BY g.season_id, g.game_status
ORDER BY g.season_id, g.game_status;

-- ============================================================
-- Q2. 2026 KIA 투수 NULL 수 (시즌 필터 game.season_id = 2026 포함)
-- ============================================================
SELECT 'Q2-A' AS query_part,
       COUNT(*) AS total_kia_pitcher_rows,
       COUNT(DISTINCT gps.game_id) AS distinct_games_covered
FROM game_pitching_stats gps
WHERE gps.team_code = 'KIA'
  AND gps.game_id IN (
        SELECT DISTINCT g.game_id FROM game g
        WHERE g.season_id = 2026 AND (g.home_team = 'KIA' OR g.away_team = 'KIA')
      );

SELECT 'Q2-B' AS query_part,
       gps.team_code,
       gps.is_starting,
       gps.innings_outs,
       COUNT(*) AS null_row_count
FROM game_pitching_stats gps
WHERE gps.team_code = 'KIA'
  AND (gps.team_code IS NULL OR gps.innings_outs IS NULL)
  AND gps.game_id IN (
        SELECT DISTINCT g.game_id FROM game g
        WHERE g.season_id = 2026 AND (g.home_team = 'KIA' OR g.away_team = 'KIA')
      )
GROUP BY gps.team_code, gps.is_starting, gps.innings_outs
ORDER BY null_row_count DESC;

-- ============================================================
-- Q3. 2026 팀 코드 분포 (KIA + 상대팀 포함 전체)
-- ============================================================
SELECT 'Q3' AS query_part,
       gps.team_code,
       gps.team_side,
       COUNT(*) AS row_count
FROM game_pitching_stats gps
WHERE gps.game_id IN (
        SELECT DISTINCT g.game_id FROM game g
        WHERE g.season_id = 2026
      )
GROUP BY gps.team_code, gps.team_side
ORDER BY row_count DESC;

-- ============================================================
-- Q4. 취소된 경기(CANCELLED)에 투수 행이 존재하는지
-- ============================================================
SELECT 'Q4-A' AS query_part,
       g.game_id,
       g.game_status,
       g.home_team,
       g.away_team,
       g.game_date
FROM game g
WHERE g.season_id = 2026
  AND g.game_status = 'CANCELLED'
ORDER BY g.game_id;

SELECT 'Q4-B' AS query_part,
       gps.game_id,
       g.game_status,
       gps.team_code,
       gps.is_starting,
       gps.innings_outs,
       gps.extra_stats
FROM game_pitching_stats gps
JOIN game g ON gps.game_id = g.game_id
WHERE g.season_id = 2026
  AND g.game_status = 'CANCELLED';

SELECT 'Q4-C' AS query_part,
       COUNT(DISTINCT gps.game_id) AS cancelled_games_with_pitcher_rows,
       COUNT(*) AS total_pitcher_rows_for_cancelled
FROM game_pitching_stats gps
JOIN game g ON gps.game_id = g.game_id
WHERE g.season_id = 2026
  AND g.game_status = 'CANCELLED';

-- ============================================================
-- Q5. 2026 game_lifecycle_state 분포 (TODO-3: season repair 확인용)
-- ============================================================
SELECT 'Q5-A' AS query_part,
       g.game_id,
       g.game_date,
       g.home_team,
       g.away_team,
       g.game_status,
       g.season_id,
       g.is_primary
FROM game g
WHERE g.season_id = 2026
ORDER BY g.game_date, g.game_id;

SELECT 'Q5-B' AS query_part,
       g.game_status,
       g.is_primary,
       COUNT(*) AS cnt,
       COUNT(DISTINCT g.game_id) AS distinct_games
FROM game g
WHERE g.season_id = 2026
GROUP BY g.game_status, g.is_primary
ORDER BY g.game_status, g.is_primary;

SELECT 'Q5-C' AS query_part,
       g.season_id,
       g.game_status,
       COUNT(*) AS cnt
FROM game g
WHERE g.season_id IN (259, 2026)
GROUP BY g.season_id, g.game_status
ORDER BY g.season_id, g.game_status;