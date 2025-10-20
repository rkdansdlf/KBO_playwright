-- ===================================================================
-- KBO 데이터베이스 외래키 제약조건 설정
-- 008_create_foreign_keys.sql
-- ===================================================================

-- 1. team_history 테이블 외래키
-- team_history.stadium_id → stadiums.stadium_id
ALTER TABLE public.team_history
ADD CONSTRAINT fk_team_history_stadium
FOREIGN KEY (stadium_id) REFERENCES public.stadiums(stadium_id);

-- ===================================================================

-- 2. player_season_batting 테이블 외래키 추가

-- player_season_batting.team_code → teams.team_id  
ALTER TABLE public.player_season_batting
ADD CONSTRAINT fk_player_season_batting_team
FOREIGN KEY (team_code) REFERENCES public.teams(team_id);

-- player_season_batting.player_id → player_basic.player_id
-- (player_basic 테이블이 존재한다고 가정)
ALTER TABLE public.player_season_batting
ADD CONSTRAINT fk_player_season_batting_player
FOREIGN KEY (player_id) REFERENCES public.player_basic(player_id);

-- ===================================================================

-- 3. 기존 player_season_batting에 season_id 컬럼 추가 및 FK 설정
-- 먼저 season_id 컬럼 추가
ALTER TABLE public.player_season_batting
ADD COLUMN season_id INTEGER;

-- season_id 값을 기존 데이터에 기반하여 설정
-- league와 season을 기반으로 kbo_seasons 테이블의 season_id를 매핑
UPDATE public.player_season_batting 
SET season_id = (
    SELECT s.season_id 
    FROM public.kbo_seasons s 
    WHERE s.season_year = player_season_batting.season 
    AND s.league_type_code = CASE 
        WHEN player_season_batting.league = 'REGULAR' THEN 0
        WHEN player_season_batting.league = 'EXHIBITION' THEN 1
        WHEN player_season_batting.league = 'WILDCARD' THEN 2
        WHEN player_season_batting.league = 'SEMI_PLAYOFF' THEN 3
        WHEN player_season_batting.league = 'PLAYOFF' THEN 4
        WHEN player_season_batting.league = 'KOREAN_SERIES' THEN 5
        ELSE 0
    END
    LIMIT 1
);

-- season_id 외래키 제약조건 추가
ALTER TABLE public.player_season_batting
ADD CONSTRAINT fk_player_season_batting_season
FOREIGN KEY (season_id) REFERENCES public.kbo_seasons(season_id);

-- ===================================================================

-- 4. 유니크 제약조건 업데이트
-- 기존 제약조건 제거 후 season_id 포함하여 재생성
ALTER TABLE public.player_season_batting
DROP CONSTRAINT IF EXISTS uq_player_season_batting;

-- 새 유니크 제약조건 (season_id 포함)
ALTER TABLE public.player_season_batting
ADD CONSTRAINT uq_player_season_batting_new
UNIQUE (player_id, season_id, level);

-- ===================================================================

-- 인덱스 업데이트
CREATE INDEX IF NOT EXISTS idx_psb_season ON public.player_season_batting (season_id);
CREATE INDEX IF NOT EXISTS idx_psb_team ON public.player_season_batting (team_code);