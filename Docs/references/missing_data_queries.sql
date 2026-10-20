-- 근거 쿼리: 시즌×컬럼 결측 매트릭스 (bega_prod, PostgreSQL)
-- 판정은 안타 대비 컬럼 합계 비율 휴리스틱(의심 목록).

-- Q1. 시즌별 경기 수 (game)
SELECT EXTRACT(YEAR FROM game_date)::int AS season, COUNT(*) AS games
FROM game
GROUP BY season
ORDER BY season;

-- Q2. player_season_batting (REGULAR) 시즌별 컬럼 합계
SELECT season,
       COUNT(*) AS rows,
       COALESCE(SUM(hits),0) AS hits,
       COALESCE(SUM(home_runs),0) AS home_runs,
       COALESCE(SUM(doubles),0) AS doubles,
       COALESCE(SUM(triples),0) AS triples,
       COALESCE(SUM(stolen_bases),0) AS stolen_bases,
       COALESCE(SUM(walks),0) AS walks,
       COALESCE(SUM(strikeouts),0) AS strikeouts,
       COALESCE(SUM(hbp),0) AS hbp,
       COALESCE(SUM(runs),0) AS runs,
       COALESCE(SUM(plate_appearances),0) AS pa,
       COALESCE(SUM(at_bats),0) AS ab
FROM player_season_batting
WHERE league = 'REGULAR'
GROUP BY season
ORDER BY season;

-- Q3. game_batting_stats 시즌별 컬럼 합계 (게임 단위 팀 타격)
SELECT SUBSTRING(game_id,1,4)::int AS season,
       COUNT(*) AS rows,
       COALESCE(SUM(hits),0) AS hits,
       COALESCE(SUM(home_runs),0) AS home_runs,
       COALESCE(SUM(doubles),0) AS doubles,
       COALESCE(SUM(triples),0) AS triples,
       COALESCE(SUM(stolen_bases),0) AS stolen_bases,
       COALESCE(SUM(walks),0) AS walks,
       COALESCE(SUM(strikeouts),0) AS strikeouts,
       COALESCE(SUM(hbp),0) AS hbp,
       COALESCE(SUM(runs),0) AS runs
FROM game_batting_stats
GROUP BY season
ORDER BY season;

-- Q4. 비율 휴리스틱: 안타 대비 컬럼 비율이 0.5%(희귀 컬럼) 또는 5%(볼넷/삼진) 미만이면 의심
-- (애플리케이션/스프레드시트에서 Q2/Q3 결과로 계산)

-- Q5. 순위표 자격 기준 영향: 시즌별 완료 경기 수
SELECT season, games_played FROM (
  SELECT EXTRACT(YEAR FROM game_date)::int AS season,
         COUNT(DISTINCT game_date) FILTER (WHERE game_status IN ('COMPLETED','DRAW')) AS games_played
  FROM game GROUP BY season
) t ORDER BY season;

-- Q6. 2009/2010 record row counts (game_batting_stats)
SELECT SUBSTRING(game_id,1,4)::int AS season, COUNT(*) AS rows
FROM game_batting_stats
WHERE SUBSTRING(game_id,1,4) IN ('2009','2010')
GROUP BY season;
