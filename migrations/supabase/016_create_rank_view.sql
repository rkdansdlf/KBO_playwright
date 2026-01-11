
-- 016_create_rank_view.sql
-- KBO 시즌별 팀 순위 뷰 (v_team_rank_all) 생성
-- 정규시즌(league_type_code=1) && 경기 완료(점수 존재) 조건 적용

CREATE OR REPLACE VIEW public.v_team_rank_all AS
WITH season_games AS (
    SELECT
        s.season_year,
        g.game_id,
        g.game_date,
        g.home_team AS home_team_id,
        g.away_team AS away_team_id,
        g.winning_team,
        g.season_id
    FROM
        public.game g
        JOIN public.kbo_seasons s ON g.season_id = s.season_id
    WHERE
        s.league_type_code = 1 -- 정규시즌
        AND g.home_score IS NOT NULL
        AND g.away_score IS NOT NULL
),
team_game_results AS (
    -- 홈팀 관점
    SELECT
        g.season_year,
        g.home_team_id AS team_id,
        CASE
            WHEN g.winning_team = g.home_team_id THEN 1
            ELSE 0
        END AS wins,
        CASE
            WHEN g.winning_team IS NULL THEN 1 -- 무승부
            ELSE 0
        END AS draws,
        CASE
            WHEN g.winning_team IS NOT NULL AND g.winning_team <> g.home_team_id THEN 1
            ELSE 0
        END AS losses
    FROM
        season_games g
    
    UNION ALL
    
    -- 원정팀 관점
    SELECT
        g.season_year,
        g.away_team_id AS team_id,
        CASE
            WHEN g.winning_team = g.away_team_id THEN 1
            ELSE 0
        END AS wins,
        CASE
            WHEN g.winning_team IS NULL THEN 1 -- 무승부
            ELSE 0
        END AS draws,
        CASE
            WHEN g.winning_team IS NOT NULL AND g.winning_team <> g.away_team_id THEN 1
            ELSE 0
        END AS losses
    FROM
        season_games g
),
team_season_agg AS (
    SELECT
        r.season_year,
        r.team_id,
        SUM(r.wins) AS wins,
        SUM(r.losses) AS losses,
        SUM(r.draws) AS draws,
        SUM(r.wins + r.losses + r.draws) AS games_played,
        CASE
            WHEN SUM(r.wins + r.losses) > 0 THEN 
                ROUND(SUM(r.wins)::numeric / SUM(r.wins + r.losses)::numeric, 3)
            ELSE 0
        END AS win_pct
    FROM
        team_game_results r
    GROUP BY
        r.season_year,
        r.team_id
)
SELECT
    a.season_year,
    a.team_id,
    t.team_name,
    a.wins,
    a.losses,
    a.draws,
    a.games_played,
    a.win_pct,
    RANK() OVER (
        PARTITION BY a.season_year
        ORDER BY
            a.win_pct DESC,    -- 승률 우선
            a.wins DESC,       -- 다승
            a.draws DESC,      -- (KBO 방식은 승률->다승->승자승 등이나 여기선 단순 정렬)
            a.losses ASC,      -- 패 적은 순
            a.games_played DESC
    ) AS season_rank
FROM
    team_season_agg a
    LEFT JOIN public.teams t ON t.team_id = a.team_id
ORDER BY
    a.season_year DESC,
    season_rank ASC;
