-- ===================================================================
-- KBO 데이터베이스 핵심 테이블 생성
-- 007_create_core_tables.sql
-- ===================================================================

-- 1. Teams 테이블 (KBO 10개 구단)
CREATE TABLE IF NOT EXISTS public.teams (
    team_id VARCHAR(10) PRIMARY KEY,
    team_name VARCHAR(50) NOT NULL,
    team_short_name VARCHAR(20) NOT NULL,
    city VARCHAR(30) NOT NULL,
    founded_year INTEGER,
    stadium_name VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Teams 테이블 코멘트
COMMENT ON TABLE public.teams IS 'KBO 구단 기본 정보';
COMMENT ON COLUMN public.teams.team_id IS '구단 고유 ID (LG, NC, SS 등)';
COMMENT ON COLUMN public.teams.team_name IS '구단 정식 명칭';
COMMENT ON COLUMN public.teams.team_short_name IS '구단 약칭';
COMMENT ON COLUMN public.teams.city IS '연고 도시';
COMMENT ON COLUMN public.teams.founded_year IS '창단 연도';
COMMENT ON COLUMN public.teams.stadium_name IS '홈 구장 명칭';

-- ===================================================================

-- 2. Stadiums 테이블 (9개 야구장)
CREATE TABLE IF NOT EXISTS public.stadiums (
    stadium_id VARCHAR(30) PRIMARY KEY,
    stadium_name VARCHAR(50) NOT NULL,
    city VARCHAR(30) NOT NULL,
    open_year INTEGER NOT NULL,
    capacity INTEGER NOT NULL,
    seating_capacity INTEGER NOT NULL,
    left_fence_m DOUBLE PRECISION NOT NULL,
    center_fence_m DOUBLE PRECISION NOT NULL,
    fence_height_m DOUBLE PRECISION,
    turf_type VARCHAR(20) NOT NULL,
    bullpen_type VARCHAR(50),
    homerun_park_factor DOUBLE PRECISION,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Stadiums 테이블 코멘트
COMMENT ON TABLE public.stadiums IS 'KBO 야구장 정보 및 파크팩터';
COMMENT ON COLUMN public.stadiums.stadium_id IS '구장 고유 식별 코드 (JAMSIL, GOCHEOK 등)';
COMMENT ON COLUMN public.stadiums.stadium_name IS '구장 정식 명칭';
COMMENT ON COLUMN public.stadiums.city IS '연고 도시';
COMMENT ON COLUMN public.stadiums.homerun_park_factor IS '홈런 파크 팩터 (1.0 기준)';
COMMENT ON COLUMN public.stadiums.notes IS '특이사항 (돔구장, 리모델링 등)';

-- ===================================================================

-- 3. Team History 테이블 (구단 변천사)
CREATE TABLE IF NOT EXISTS public.team_history (
    id SERIAL PRIMARY KEY,
    franchise_id INTEGER NOT NULL,
    team_name VARCHAR(50) NOT NULL,
    team_code VARCHAR(10) NOT NULL,
    city VARCHAR(30) NOT NULL,
    start_season INTEGER NOT NULL,
    end_season INTEGER,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    stadium_id VARCHAR(30),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Team History 테이블 코멘트  
COMMENT ON TABLE public.team_history IS 'KBO 구단 역사 및 변천사';
COMMENT ON COLUMN public.team_history.franchise_id IS '프랜차이즈 그룹 ID (1-10)';
COMMENT ON COLUMN public.team_history.team_name IS '당시 구단명';
COMMENT ON COLUMN public.team_history.team_code IS '당시 팀 코드';
COMMENT ON COLUMN public.team_history.start_season IS '시작 시즌';
COMMENT ON COLUMN public.team_history.end_season IS '종료 시즌 (NULL이면 현재)';
COMMENT ON COLUMN public.team_history.is_current IS '현재 명칭 여부';
COMMENT ON COLUMN public.team_history.stadium_id IS '사용 구장 ID';

-- ===================================================================

-- 4. KBO Seasons 테이블 (시즌 메타데이터)
CREATE TABLE IF NOT EXISTS public.kbo_seasons (
    season_id SERIAL PRIMARY KEY,
    season_year INTEGER NOT NULL,
    league_type_code INTEGER NOT NULL,
    league_type_name VARCHAR(50) NOT NULL,
    start_date DATE,
    end_date DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- KBO Seasons 테이블 코멘트
COMMENT ON TABLE public.kbo_seasons IS 'KBO 시즌 정보 (1982-2030)';
COMMENT ON COLUMN public.kbo_seasons.season_id IS '시즌 고유 ID';
COMMENT ON COLUMN public.kbo_seasons.season_year IS '시즌 연도';
COMMENT ON COLUMN public.kbo_seasons.league_type_code IS '시즌 종류 코드 (0:정규시즌, 1:시범경기, 2:와일드카드, 3:준플레이오프, 4:플레이오프, 5:한국시리즈)';
COMMENT ON COLUMN public.kbo_seasons.league_type_name IS '시즌 종류 명칭';

-- ===================================================================

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_team_history_franchise ON public.team_history (franchise_id);
CREATE INDEX IF NOT EXISTS idx_team_history_season ON public.team_history (start_season, end_season);
CREATE INDEX IF NOT EXISTS idx_kbo_seasons_year_type ON public.kbo_seasons (season_year, league_type_code);

-- ===================================================================

-- RLS 활성화
ALTER TABLE public.teams ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.stadiums ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.team_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.kbo_seasons ENABLE ROW LEVEL SECURITY;

-- RLS 정책 생성 (모든 작업 허용)
CREATE POLICY "Allow all operations on teams" ON public.teams FOR ALL USING (true);
CREATE POLICY "Allow all operations on stadiums" ON public.stadiums FOR ALL USING (true);
CREATE POLICY "Allow all operations on team_history" ON public.team_history FOR ALL USING (true);
CREATE POLICY "Allow all operations on kbo_seasons" ON public.kbo_seasons FOR ALL USING (true);