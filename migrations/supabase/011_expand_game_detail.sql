-- ===================================================================
-- 확장된 경기 상세 데이터 저장소
-- 011_expand_game_detail.sql
-- 기존 game/box_score 테이블에 메타데이터 컬럼을 추가하고
-- 라인업/타격/투구/이닝별 득점/정규화 이벤트 테이블을 생성한다.
-- ===================================================================

-- 1. game_summary 컬럼 명 정비 (category/content → summary_type/detail_text)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'game_summary'
          AND column_name = 'category'
    ) THEN
        ALTER TABLE public.game_summary
        RENAME COLUMN category TO summary_type;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'game_summary'
          AND column_name = 'content'
    ) THEN
        ALTER TABLE public.game_summary
        RENAME COLUMN content TO detail_text;
    END IF;
END $$;

-- 2. game_metadata
CREATE TABLE IF NOT EXISTS public.game_metadata (
    game_id VARCHAR(20) PRIMARY KEY REFERENCES public.game(game_id) ON DELETE CASCADE,
    stadium_code VARCHAR(30),
    stadium_name VARCHAR(64),
    attendance INTEGER,
    start_time TIME,
    end_time TIME,
    game_time_minutes INTEGER,
    weather VARCHAR(32),
    source_payload JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. game_inning_scores
CREATE TABLE IF NOT EXISTS public.game_inning_scores (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL REFERENCES public.game(game_id) ON DELETE CASCADE,
    team_side VARCHAR(5) NOT NULL,
    team_code VARCHAR(10),
    inning INTEGER NOT NULL,
    runs INTEGER DEFAULT 0,
    is_extra BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_game_inning_team UNIQUE (game_id, team_side, inning)
);

-- 4. game_lineups
CREATE TABLE IF NOT EXISTS public.game_lineups (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL REFERENCES public.game(game_id) ON DELETE CASCADE,
    team_side VARCHAR(5) NOT NULL,
    team_code VARCHAR(10),
    player_id INTEGER,
    player_name VARCHAR(64) NOT NULL,
    batting_order INTEGER,
    position VARCHAR(8),
    is_starter BOOLEAN DEFAULT FALSE,
    appearance_seq INTEGER NOT NULL,
    notes VARCHAR(64),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_game_lineup_entry UNIQUE (game_id, team_side, appearance_seq)
);

-- 5. game_batting_stats
CREATE TABLE IF NOT EXISTS public.game_batting_stats (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL REFERENCES public.game(game_id) ON DELETE CASCADE,
    team_side VARCHAR(5) NOT NULL,
    team_code VARCHAR(10),
    player_id INTEGER,
    player_name VARCHAR(64) NOT NULL,
    batting_order INTEGER,
    is_starter BOOLEAN DEFAULT FALSE,
    appearance_seq INTEGER NOT NULL,
    position VARCHAR(8),
    plate_appearances INTEGER DEFAULT 0,
    at_bats INTEGER DEFAULT 0,
    runs INTEGER DEFAULT 0,
    hits INTEGER DEFAULT 0,
    doubles INTEGER DEFAULT 0,
    triples INTEGER DEFAULT 0,
    home_runs INTEGER DEFAULT 0,
    rbi INTEGER DEFAULT 0,
    walks INTEGER DEFAULT 0,
    intentional_walks INTEGER DEFAULT 0,
    hbp INTEGER DEFAULT 0,
    strikeouts INTEGER DEFAULT 0,
    stolen_bases INTEGER DEFAULT 0,
    caught_stealing INTEGER DEFAULT 0,
    sacrifice_hits INTEGER DEFAULT 0,
    sacrifice_flies INTEGER DEFAULT 0,
    gdp INTEGER DEFAULT 0,
    avg DOUBLE PRECISION,
    obp DOUBLE PRECISION,
    slg DOUBLE PRECISION,
    ops DOUBLE PRECISION,
    iso DOUBLE PRECISION,
    babip DOUBLE PRECISION,
    extra_stats JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_game_batting_player UNIQUE (game_id, player_id, appearance_seq)
);

CREATE INDEX IF NOT EXISTS idx_game_batting_team ON public.game_batting_stats (game_id, team_side);

-- 6. game_pitching_stats
CREATE TABLE IF NOT EXISTS public.game_pitching_stats (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL REFERENCES public.game(game_id) ON DELETE CASCADE,
    team_side VARCHAR(5) NOT NULL,
    team_code VARCHAR(10),
    player_id INTEGER,
    player_name VARCHAR(64) NOT NULL,
    is_starting BOOLEAN DEFAULT FALSE,
    appearance_seq INTEGER NOT NULL,
    innings_outs INTEGER DEFAULT 0,
    innings_pitched NUMERIC(5,3),
    batters_faced INTEGER DEFAULT 0,
    pitches INTEGER DEFAULT 0,
    hits_allowed INTEGER DEFAULT 0,
    runs_allowed INTEGER DEFAULT 0,
    earned_runs INTEGER DEFAULT 0,
    home_runs_allowed INTEGER DEFAULT 0,
    walks_allowed INTEGER DEFAULT 0,
    strikeouts INTEGER DEFAULT 0,
    hit_batters INTEGER DEFAULT 0,
    wild_pitches INTEGER DEFAULT 0,
    balks INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    saves INTEGER DEFAULT 0,
    holds INTEGER DEFAULT 0,
    decision VARCHAR(2),
    era DOUBLE PRECISION,
    whip DOUBLE PRECISION,
    k_per_nine DOUBLE PRECISION,
    bb_per_nine DOUBLE PRECISION,
    kbb DOUBLE PRECISION,
    extra_stats JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_game_pitching_player UNIQUE (game_id, player_id, appearance_seq)
);

CREATE INDEX IF NOT EXISTS idx_game_pitching_team ON public.game_pitching_stats (game_id, team_side);

-- 7. game_events (정규화된 PBP)
CREATE TABLE IF NOT EXISTS public.game_events (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL REFERENCES public.game(game_id) ON DELETE CASCADE,
    event_seq INTEGER NOT NULL,
    inning INTEGER,
    inning_half VARCHAR(6),
    outs INTEGER,
    batter_id INTEGER,
    batter_name VARCHAR(64),
    pitcher_id INTEGER,
    pitcher_name VARCHAR(64),
    description TEXT,
    event_type VARCHAR(32),
    result_code VARCHAR(16),
    rbi INTEGER,
    bases_before VARCHAR(3),
    bases_after VARCHAR(3),
    extra_json JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_game_event_seq UNIQUE (game_id, event_seq)
);

CREATE INDEX IF NOT EXISTS idx_game_events_inning ON public.game_events (game_id, inning, inning_half);

-- 8. game_summary 인덱스
CREATE INDEX IF NOT EXISTS idx_game_summary_summary_type
ON public.game_summary (game_id, summary_type);

-- 9. RLS (열람/쓰기 모두 허용)
ALTER TABLE public.game_metadata ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.game_inning_scores ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.game_lineups ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.game_batting_stats ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.game_pitching_stats ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.game_events ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all on game_metadata" ON public.game_metadata FOR ALL USING (true);
CREATE POLICY "Allow all on game_inning_scores" ON public.game_inning_scores FOR ALL USING (true);
CREATE POLICY "Allow all on game_lineups" ON public.game_lineups FOR ALL USING (true);
CREATE POLICY "Allow all on game_batting_stats" ON public.game_batting_stats FOR ALL USING (true);
CREATE POLICY "Allow all on game_pitching_stats" ON public.game_pitching_stats FOR ALL USING (true);
CREATE POLICY "Allow all on game_events" ON public.game_events FOR ALL USING (true);
