-- ===================================================================
-- KBO 데이터베이스 초기 데이터 삽입
-- 009_insert_initial_data.sql
-- ===================================================================

-- 1. Teams 테이블 초기 데이터 (KBO 10개 구단)
INSERT INTO public.teams (team_id, team_name, team_short_name, city, founded_year, stadium_name) VALUES
('SS', '삼성 라이온즈', '삼성', '대구', 1982, '대구 삼성 라이온즈 파크'),
('LG', 'LG 트윈스', 'LG', '서울', 1990, '서울종합운동장 잠실 야구장'),
('NC', 'NC 다이노스', 'NC', '창원', 2011, '창원NC파크'),
('OB', '두산 베어스', '두산', '서울', 1982, '서울종합운동장 잠실 야구장'),
('KT', 'kt wiz', 'kt', '수원', 2013, '수원 kt wiz 파크'),
('HT', 'KIA 타이거즈', 'KIA', '광주', 2001, '광주-기아 챔피언스 필드'),
('LT', '롯데 자이언츠', '롯데', '부산', 1982, '부산 사직 야구장'),
('HH', '한화 이글스', '한화', '대전', 1986, '대전 한화생명 이글스 파크'),
('WO', '키움 히어로즈', '키움', '서울', 2008, '고척스카이돔'),
('SK', 'SSG 랜더스', 'SSG', '인천', 2021, '인천SSG랜더스필드')
ON CONFLICT (team_id) DO NOTHING;

-- ===================================================================

-- 2. Stadiums 테이블 초기 데이터 (9개 야구장)
INSERT INTO public.stadiums (stadium_id, stadium_name, city, open_year, capacity, seating_capacity, left_fence_m, center_fence_m, fence_height_m, turf_type, bullpen_type, homerun_park_factor, notes) VALUES
('JAMSIL', '잠실 야구장', '서울', 1982, 25000, 24411, 100.0, 125.0, NULL, '천연잔디', '분리형 불펜', 0.732, 'KBO 최대 규모, 투수 친화적'),
('GOCHEOK', '고척스카이돔', '서울', 2015, 22258, 16783, 99.0, 122.0, NULL, '인조잔디', '폐쇄형 지하불펜', 0.822, '국내 유일 돔구장'),
('SSGLANDERS', '인천SSG랜더스필드', '인천', 2002, 25000, 23000, 95.0, 120.0, 2.8, '천연잔디', '외야 펜스 분리형 불펜', 1.489, '홈런 2번째로 많음, 주차장 넓음'),
('KTWIZ', '수원 kt wiz 파크', '수원', 1989, 25000, 22067, 98.0, 120.0, NULL, '천연잔디', '외야 파울존 분리형 불펜', NULL, '2014년 리모델링'),
('LIONS', '대구 삼성 라이온즈 파크', '대구', 2016, 29178, 24331, 99.0, 122.0, 3.6, '천연잔디', NULL, 1.522, '국내 최초 팔각형 구장, 홈런 가장 많음'),
('CHAMPIONS', '광주-기아 챔피언스 필드', '광주', 2014, 27000, 20500, 99.0, 121.0, NULL, '천연잔디', '외야 펜스 분리형 불펜', 0.953, '국내 최초 개방형 구장'),
('EAGLES', '대전 한화생명 이글스 파크', '대전', 2025, 25000, 22000, 99.5, 122.0, NULL, '천연잔디', '외야 펜스 복층형 불펜', NULL, '2025년 신규 개장'),
('NCPARK', '창원NC파크', '창원', 2019, 22112, 22112, 101.0, 122.0, NULL, '천연잔디', '외야 펜스 분리형 불펜', 1.085, NULL),
('SAJIK', '부산 사직 야구장', '부산', 1985, 27500, 24500, 96.0, 121.0, 4.8, '천연잔디', '외야 파울존 분리형 불펜', 0.729, '2025년 펜스 높이 4.8m로 조정')
ON CONFLICT (stadium_id) DO NOTHING;

-- ===================================================================

-- 3. Team History 테이블 초기 데이터 (구단 변천사)
INSERT INTO public.team_history (id, franchise_id, team_name, team_code, city, start_season, end_season, is_current, stadium_id) VALUES
-- 1. 삼성 라이온즈 (ID: 1)
(1, 1, '삼성 라이온즈', 'SS', '대구', 1982, NULL, true, 'LIONS'),

-- 2. 롯데 자이언츠 (ID: 2)
(2, 2, '롯데 자이언츠', 'LT', '부산', 1982, NULL, true, 'SAJIK'),

-- 3. MBC 청룡 -> LG 트윈스 (ID: 3)
(3, 3, 'MBC 청룡', 'MBC', '서울', 1982, 1989, false, 'JAMSIL'),
(4, 3, 'LG 트윈스', 'LG', '서울', 1990, NULL, true, 'JAMSIL'),

-- 4. OB 베어스 -> 두산 베어스 (ID: 4)
(5, 4, 'OB 베어스', 'OB', '서울', 1982, 1995, false, 'JAMSIL'),
(6, 4, '두산 베어스', 'OB', '서울', 1996, NULL, true, 'JAMSIL'),

-- 5. 해태 타이거즈 -> KIA 타이거즈 (ID: 5)
(7, 5, '해태 타이거즈', 'HT', '광주', 1982, 2000, false, 'CHAMPIONS'),
(8, 5, 'KIA 타이거즈', 'HT', '광주', 2001, NULL, true, 'CHAMPIONS'),

-- 6. 삼미 -> 청보 -> 태평양 -> 현대 -> 히어로즈 프랜차이즈 (ID: 6)
(9, 6, '삼미 슈퍼스타즈', 'SM', '인천', 1982, 1985, false, 'SSGLANDERS'),
(10, 6, '청보 핀토스', 'CB', '인천', 1985, 1987, false, 'SSGLANDERS'),
(11, 6, '태평양 돌핀스', 'TP', '인천', 1988, 1995, false, 'SSGLANDERS'),
(12, 6, '현대 유니콘스', 'HU', '수원', 1996, 2007, false, 'KTWIZ'),
(13, 6, '우리 히어로즈', 'WO', '서울', 2008, 2009, false, 'GOCHEOK'),
(14, 6, '넥센 히어로즈', 'WO', '서울', 2010, 2018, false, 'GOCHEOK'),
(15, 6, '키움 히어로즈', 'WO', '서울', 2019, NULL, true, 'GOCHEOK'),

-- 7. 빙그레 이글스 -> 한화 이글스 (ID: 7)
(16, 7, '빙그레 이글스', 'BE', '대전', 1986, 1992, false, 'EAGLES'),
(17, 7, '한화 이글스', 'HH', '대전', 1993, NULL, true, 'EAGLES'),

-- 8. 쌍방울 레이더스 -> SK 와이번스 -> SSG 랜더스 (ID: 8)
(18, 8, '쌍방울 레이더스', 'SL', '전주', 1990, 1999, false, 'SSGLANDERS'),
(19, 8, 'SK 와이번스', 'SK', '인천', 2000, 2020, false, 'SSGLANDERS'),
(20, 8, 'SSG 랜더스', 'SK', '인천', 2021, NULL, true, 'SSGLANDERS'),

-- 9. NC 다이노스 (ID: 9)
(21, 9, 'NC 다이노스', 'NC', '창원', 2011, NULL, true, 'NCPARK'),

-- 10. KT 위즈 (ID: 10)
(22, 10, 'kt wiz', 'KT', '수원', 2013, NULL, true, 'KTWIZ')
ON CONFLICT (id) DO NOTHING;

-- ===================================================================

-- 4. KBO Seasons 테이블 초기 데이터 (1982-2030년)
-- PostgreSQL의 GENERATE_SERIES를 사용하여 자동 생성
WITH seasons_map AS (
    SELECT * FROM (VALUES
        (0, '정규시즌'),
        (1, '시범경기'),
        (2, '와일드카드'),
        (3, '준플레이오프'),
        (4, '플레이오프'),
        (5, '한국시리즈')
    ) AS t(code, name)
)
INSERT INTO public.kbo_seasons (season_year, league_type_code, league_type_name)
SELECT
    s.year AS season_year,
    sm.code AS league_type_code,
    sm.name AS league_type_name
FROM
    GENERATE_SERIES(1982, 2030) AS s(year)
CROSS JOIN
    seasons_map sm
ON CONFLICT DO NOTHING;