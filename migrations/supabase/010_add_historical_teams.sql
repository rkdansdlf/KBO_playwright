-- ===================================================================
-- 과거 구단들을 teams 테이블에 추가하여 team_history와 FK 연결 가능하게 함
-- 010_add_historical_teams.sql
-- ===================================================================

-- 과거 구단 정보 추가
INSERT INTO public.teams (team_id, team_name, team_short_name, city, founded_year, stadium_name) VALUES
-- 과거 구단들 (현재는 존재하지 않음)
('MBC', 'MBC 청룡', 'MBC', '서울', 1982, '서울종합운동장 잠실 야구장'),
('BE', '빙그레 이글스', '빙그레', '대전', 1986, '대전구장'),
('CB', '청보 핀토스', '청보', '인천', 1985, '인천구장'),
('HU', '현대 유니콘스', '현대', '수원', 1996, '수원야구장'),
('SL', '쌍방울 레이더스', '쌍방울', '전주', 1990, '전주구장'),
('SM', '삼미 슈퍼스타즈', '삼미', '인천', 1982, '인천구장'),
('TP', '태평양 돌핀스', '태평양', '인천', 1988, '인천구장')
ON CONFLICT (team_id) DO NOTHING;

-- ===================================================================

-- team_history.team_code → teams.team_id 외래키 제약조건 추가
ALTER TABLE public.team_history
ADD CONSTRAINT fk_team_history_team
FOREIGN KEY (team_code) REFERENCES public.teams(team_id);

-- ===================================================================

-- 매핑 확인용 인덱스 추가
CREATE INDEX IF NOT EXISTS idx_team_history_team_code ON public.team_history (team_code);