팀 변천·해체 정보와 현재 홈구장 목록을 기준으로, **이력(프랜차이즈)·명칭(브랜딩)·구장(볼파크)·할당(홈구장 기간)** 을 분리한 **SQLite 스키마 + 초기 데이터(seed)** 를 제안합니다.
— “프랜차이즈”는 법적·역사적 연속성을, “팀 명칭”은 시기별 브랜드/약칭/도시 표기를 담습니다. 홈구장은 시즌(또는 연도) 구간으로 매핑합니다.

---

# 1) DDL — teams 도메인 스키마 (SQLite)

```sql
-- 0) 공통: updated_at 자동 갱신용 트리거에서 쓸 함수 없음(SQLite) → 각 테이블별 트리거 제공
PRAGMA foreign_keys = ON;

-- 1) 프랜차이즈(역사적 동일성 단위)
CREATE TABLE franchises (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  key             TEXT UNIQUE NOT NULL,             -- 예: SAMSUNG, LOTTE, LG, KIA, DOOSAN, HEROES, HANWHA, NC, SSG, KT, SSANG
  canonical_name  TEXT NOT NULL,                    -- 현재 기준의 대표명(예: 삼성 라이온즈)
  first_season    INTEGER,                          -- 창단 시즌(알 수 없으면 NULL)
  last_season     INTEGER,                          -- 해체/이전 등으로 리그 이탈 시즌(NULL=활동중)
  status          TEXT NOT NULL DEFAULT 'ACTIVE',   -- ACTIVE | DISSOLVED
  notes           TEXT,
  created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TRIGGER franchises_updated_at
AFTER UPDATE ON franchises
FOR EACH ROW BEGIN
  UPDATE franchises SET updated_at=CURRENT_TIMESTAMP WHERE id=OLD.id;
END;

-- 2) 팀 명칭(브랜딩) 이력: 프랜차이즈별로 시기별 이름/약칭/도시 변화 기록
CREATE TABLE team_identities (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  franchise_id     INTEGER NOT NULL REFERENCES franchises(id) ON DELETE CASCADE,
  name_kor         TEXT NOT NULL,          -- 예: MBC 청룡, LG 트윈스, 해태 타이거즈, KIA 타이거즈 ...
  name_eng         TEXT,                   -- 선택
  short_code       TEXT,                   -- 예: SS, LOT, LG, HT, DOO, HERO, HHE, NC, SSG, KT ...
  city_kor         TEXT,                   -- 예: 서울, 인천, 수원, 대구...
  start_season     INTEGER,                -- 시작 시즌(모르면 NULL)
  end_season       INTEGER,                -- 종료 시즌(NULL=현재)
  is_current       INTEGER NOT NULL DEFAULT 0 CHECK (is_current IN (0,1)),
  notes            TEXT,
  created_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- 동일 프랜차이즈에서 겹치는 구간이 있더라도(부분 정보만 있을 수 있으므로) 허용.
CREATE INDEX idx_team_identities_franchise ON team_identities(franchise_id);
CREATE INDEX idx_team_identities_period    ON team_identities(franchise_id, start_season, end_season);
CREATE TRIGGER team_identities_updated_at
AFTER UPDATE ON team_identities
FOR EACH ROW BEGIN
  UPDATE team_identities SET updated_at=CURRENT_TIMESTAMP WHERE id=OLD.id;
END;

-- 3) 브랜드/소유·인수·개명 이벤트(선택): 연도 단위의 변천 기록
CREATE TABLE franchise_events (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  franchise_id     INTEGER NOT NULL REFERENCES franchises(id) ON DELETE CASCADE,
  event_year       INTEGER,                -- 연도(제공되지 않으면 NULL)
  event_type       TEXT NOT NULL,          -- RENAME | ACQUISITION | FOLD | RELOCATION | EXPANSION
  from_name        TEXT,                   -- MBC 청룡
  to_name          TEXT,                   -- LG 트윈스
  description      TEXT,                   -- 자유 기술
  created_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_franchise_events ON franchise_events(franchise_id, event_year);

-- 4) 볼파크(구장) 마스터
CREATE TABLE ballparks (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  name_kor         TEXT NOT NULL,          -- 예: 인천SSG랜더스필드
  city_kor         TEXT,                   -- 예: 인천, 수원, 서울, 대구, 고척(서울), 광주...
  opened_year      INTEGER,
  closed_year      INTEGER,
  is_dome          INTEGER CHECK (is_dome IN (0,1)),
  capacity         INTEGER,
  notes            TEXT,
  created_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (name_kor)
);
CREATE TRIGGER ballparks_updated_at
AFTER UPDATE ON ballparks
FOR EACH ROW BEGIN
  UPDATE ballparks SET updated_at=CURRENT_TIMESTAMP WHERE id=OLD.id;
END;

-- 5) 홈구장 할당(기간): 프랜차이즈 ↔ 볼파크
CREATE TABLE home_ballpark_assignments (
  franchise_id     INTEGER NOT NULL REFERENCES franchises(id) ON DELETE CASCADE,
  ballpark_id      INTEGER NOT NULL REFERENCES ballparks(id)   ON DELETE RESTRICT,
  start_season     INTEGER,                -- 시작 시즌(모르면 NULL)
  end_season       INTEGER,                -- 종료 시즌(NULL=현재)
  is_primary       INTEGER NOT NULL DEFAULT 1 CHECK (is_primary IN (0,1)),  -- 다중 홈구장 시 구분
  notes            TEXT,
  created_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (franchise_id, ballpark_id, COALESCE(start_season, -1))
);
CREATE INDEX idx_home_ballpark_period ON home_ballpark_assignments(franchise_id, start_season, end_season);
CREATE TRIGGER home_ballpark_assignments_updated_at
AFTER UPDATE ON home_ballpark_assignments
FOR EACH ROW BEGIN
  UPDATE home_ballpark_assignments SET updated_at=CURRENT_TIMESTAMP
  WHERE franchise_id=OLD.franchise_id AND ballpark_id=OLD.ballpark_id AND COALESCE(start_season,-1)=COALESCE(OLD.start_season,-1);
END;
```

> 설계 포인트
>
> * **franchises**: 역사적 연속성을 보존(해체 여부·활동 기간 관리).
> * **team_identities**: 같은 프랜차이즈의 **브랜드/명칭/도시** 변화 기록(기간 필드 NULL 허용으로 ‘연도 추후 보강’ 가능).
> * **franchise_events**: “청보→태평양→현대→…” 등 변천 스토리를 **연도 단위**로 빠르게 조회.
> * **ballparks + assignments**: 여러 팀이 **동일 구장 공유** 가능(예: 같은 시즌 LG/두산), 한 팀이 **다중 홈** 운영 시 `is_primary`로 구분.

---

# 2) Seed — 초기 데이터 입력 (사용자 제공 정보만 반영)

> ⚠️ 연도 정보가 **문장으로 제공된 부분만** 채웠습니다. 모르는 연도는 NULL로 두었습니다.
> ⚠️ 홈구장도 **제공된 7개**만 우선 매핑했습니다(나머지 3팀은 추후 보강).

```sql
-- A. 프랜차이즈 마스터 (활동/해체 상태)
INSERT INTO franchises (key, canonical_name, status, notes) VALUES
  ('SAMSUNG', '삼성 라이온즈',  'ACTIVE',  NULL),
  ('LOTTE',   '롯데 자이언츠',  'ACTIVE',  NULL),
  ('LG',      'LG 트윈스',      'ACTIVE',  'MBC 청룡의 전신'),
  ('KIA',     'KIA 타이거즈',   'ACTIVE',  '해태 타이거즈의 전신'),
  ('DOOSAN',  '두산 베어스',    'ACTIVE',  'OB 베어스의 전신'),
  ('HEROES',  '키움 히어로즈',  'ACTIVE',  '삼미→청보→태평양→현대→우리→넥센→키움'),
  ('HANWHA',  '한화 이글스',    'ACTIVE',  NULL),
  ('NC',      'NC 다이노스',    'ACTIVE',  NULL),
  ('SSG',     'SSG 랜더스',     'ACTIVE',  'SK 와이번스에서 변경'),
  ('KT',      'KT 위즈',        'ACTIVE',  NULL),
  ('SSANG',   '쌍방울 레이더스','DISSOLVED','1999년 해체');

-- B. 팀 명칭(브랜딩) 이력 — 제공 정보 기반, 연도 미상은 NULL
-- 삼성/롯데: 변천 없음(현재명 동일)
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season, is_current)
SELECT id, '삼성 라이온즈', 'SS', '대구', 1982, NULL, 1 FROM franchises WHERE key='SAMSUNG';
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season, is_current)
SELECT id, '롯데 자이언츠', 'LOT', '부산', 1982, NULL, 1 FROM franchises WHERE key='LOTTE';

-- LG 프랜차이즈: MBC 청룡 → (1990 인수) → LG 트윈스
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season)
SELECT id, 'MBC 청룡', 'MBC', '서울', 1982, 1990 FROM franchises WHERE key='LG';
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season, is_current)
SELECT id, 'LG 트윈스', 'LG', '서울', 1990, NULL, 1 FROM franchises WHERE key='LG';

-- KIA 프랜차이즈: 해태 타이거즈 → (2001 인수) → KIA 타이거즈
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season)
SELECT id, '해태 타이거즈', 'HAI', '광주', 1982, 2001 FROM franchises WHERE key='KIA';
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season, is_current)
SELECT id, 'KIA 타이거즈', 'KIA', '광주', 2001, NULL, 1 FROM franchises WHERE key='KIA';

-- DOOSAN 프랜차이즈: OB 베어스 → 두산 베어스 (연도 미상 → end/start NULL)
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season)
SELECT id, 'OB 베어스', 'OB', '서울', 1982, NULL FROM franchises WHERE key='DOOSAN';
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season, is_current)
SELECT id, '두산 베어스', 'DOO', '서울', NULL, NULL, 1 FROM franchises WHERE key='DOOSAN';

-- HEROES 프랜차이즈: 삼미→청보(1985)→태평양(1988)→현대(1995)→우리(2008)→넥센(2010)→키움(2019)
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season)
SELECT id, '삼미 슈퍼스타즈', 'SAM', NULL, 1982, 1985 FROM franchises WHERE key='HEROES';
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season)
SELECT id, '청보 핀토스',     'CB',  NULL, 1985, 1988 FROM franchises WHERE key='HEROES';
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season)
SELECT id, '태평양 돌핀스',   'TP',  NULL, 1988, 1995 FROM franchises WHERE key='HEROES';
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season)
SELECT id, '현대 유니콘스',   'HYU', NULL, 1995, 2008 FROM franchises WHERE key='HEROES';
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season)
SELECT id, '우리 히어로즈',   'WO',  '서울', 2008, 2010 FROM franchises WHERE key='HEROES';
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season)
SELECT id, '넥센 히어로즈',   'NEX', '서울', 2010, 2019 FROM franchises WHERE key='HEROES';
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season, is_current)
SELECT id, '키움 히어로즈',   'KIW', '서울', 2019, NULL, 1 FROM franchises WHERE key='HEROES';

-- HANWHA / NC / SSG / KT: 현재명만 우선
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season, is_current)
SELECT id, '한화 이글스', 'HHE', '대전', NULL, NULL, 1 FROM franchises WHERE key='HANWHA';
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season, is_current)
SELECT id, 'NC 다이노스', 'NC',  '창원', NULL, NULL, 1 FROM franchises WHERE key='NC';
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season)
SELECT id, 'SK 와이번스', 'SK',  '인천', NULL, 2021, 0 FROM franchises WHERE key='SSG';
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season, is_current)
SELECT id, 'SSG 랜더스', 'SSG', '인천', 2021, NULL, 1 FROM franchises WHERE key='SSG';
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season, is_current)
SELECT id, 'KT 위즈', 'KT', '수원', NULL, NULL, 1 FROM franchises WHERE key='KT';

-- 쌍방울 레이더스(해체)
INSERT INTO team_identities (franchise_id, name_kor, short_code, city_kor, start_season, end_season, is_current)
SELECT id, '쌍방울 레이더스', 'SSANG', NULL, NULL, 1999, 0 FROM franchises WHERE key='SSANG';

-- C. 변천 이벤트(연도 제공분만)
-- HEROES 체인
INSERT INTO franchise_events (franchise_id, event_year, event_type, from_name, to_name, description)
SELECT id, 1985, 'ACQUISITION', '삼미 슈퍼스타즈', '청보 핀토스', '청보식품 인수' FROM franchises WHERE key='HEROES';
INSERT INTO franchise_events (franchise_id, event_year, event_type, from_name, to_name, description)
SELECT id, 1988, 'ACQUISITION', '청보 핀토스', '태평양 돌핀스', '태평양화학 인수' FROM franchises WHERE key='HEROES';
INSERT INTO franchise_events (franchise_id, event_year, event_type, from_name, to_name, description)
SELECT id, 1995, 'ACQUISITION', '태평양 돌핀스', '현대 유니콘스', '현대그룹 인수' FROM franchises WHERE key='HEROES';
INSERT INTO franchise_events (franchise_id, event_year, event_type, from_name, to_name, description)
SELECT id, 2008, 'ACQUISITION', '현대 유니콘스', '우리 히어로즈', '넥센 인수(우리 히어로즈 명칭)' FROM franchises WHERE key='HEROES';
INSERT INTO franchise_events (franchise_id, event_year, event_type, from_name, to_name, description)
SELECT id, 2010, 'RENAME', '우리 히어로즈', '넥센 히어로즈', '네이밍 변경' FROM franchises WHERE key='HEROES';
INSERT INTO franchise_events (franchise_id, event_year, event_type, from_name, to_name, description)
SELECT id, 2019, 'RENAME', '넥센 히어로즈', '키움 히어로즈', '네이밍 변경' FROM franchises WHERE key='HEROES';

-- LG 체인
INSERT INTO franchise_events (franchise_id, event_year, event_type, from_name, to_name, description)
SELECT id, 1990, 'ACQUISITION', 'MBC 청룡', 'LG 트윈스', 'LG 인수' FROM franchises WHERE key='LG';

-- KIA 체인
INSERT INTO franchise_events (franchise_id, event_year, event_type, from_name, to_name, description)
SELECT id, 2001, 'ACQUISITION', '해태 타이거즈', 'KIA 타이거즈', '기아차 인수' FROM franchises WHERE key='KIA';

-- DOOSAN 체인(연도 미제공 → NULL)
INSERT INTO franchise_events (franchise_id, event_year, event_type, from_name, to_name, description)
SELECT id, NULL, 'RENAME', 'OB 베어스', '두산 베어스', '연도 미상(추후 보강)' FROM franchises WHERE key='DOOSAN';

-- SSG 체인
INSERT INTO franchise_events (franchise_id, event_year, event_type, from_name, to_name, description)
SELECT id, 2021, 'ACQUISITION', 'SK 와이번스', 'SSG 랜더스', '신세계그룹 인수' FROM franchises WHERE key='SSG';

-- SSANG 해체
INSERT INTO franchise_events (franchise_id, event_year, event_type, from_name, to_name, description)
SELECT id, 1999, 'FOLD', '쌍방울 레이더스', NULL, '팀 해체' FROM franchises WHERE key='SSANG';

-- D. 볼파크(제공된 7개)
INSERT INTO ballparks (name_kor, city_kor) VALUES
  ('인천SSG랜더스필드', '인천'),
  ('수원KT위즈파크',     '수원'),
  ('사직야구장',         '부산'),
  ('서울종합운동장 야구장', '서울'),
  ('대구삼성라이온즈파크', '대구'),
  ('고척스카이돔',       '서울'),
  ('광주기아챔피언스필드','광주');

-- E. 홈구장 할당(현재 기준, 제공 팀만 매핑)
-- SSG 랜더스 → 인천SSG랜더스필드
INSERT INTO home_ballpark_assignments (franchise_id, ballpark_id, start_season, end_season, is_primary)
SELECT f.id, b.id, NULL, NULL, 1
FROM franchises f JOIN ballparks b
ON f.key='SSG' AND b.name_kor='인천SSG랜더스필드';

-- KT 위즈 → 수원KT위즈파크
INSERT INTO home_ballpark_assignments (franchise_id, ballpark_id, start_season, end_season, is_primary)
SELECT f.id, b.id, NULL, NULL, 1
FROM franchises f JOIN ballparks b
ON f.key='KT' AND b.name_kor='수원KT위즈파크';

-- 롯데 → 사직
INSERT INTO home_ballpark_assignments (franchise_id, ballpark_id, start_season, end_season, is_primary)
SELECT f.id, b.id, NULL, NULL, 1
FROM franchises f JOIN ballparks b
ON f.key='LOTTE' AND b.name_kor='사직야구장';

-- LG → 서울종합운동장 야구장
INSERT INTO home_ballpark_assignments (franchise_id, ballpark_id, start_season, end_season, is_primary)
SELECT f.id, b.id, NULL, NULL, 1
FROM franchises f JOIN ballparks b
ON f.key='LG' AND b.name_kor='서울종합운동장 야구장';

-- 삼성 → 대구삼성라이온즈파크
INSERT INTO home_ballpark_assignments (franchise_id, ballpark_id, start_season, end_season, is_primary)
SELECT f.id, b.id, NULL, NULL, 1
FROM franchises f JOIN ballparks b
ON f.key='SAMSUNG' AND b.name_kor='대구삼성라이온즈파크';

-- 키움 → 고척스카이돔
INSERT INTO home_ballpark_assignments (franchise_id, ballpark_id, start_season, end_season, is_primary)
SELECT f.id, b.id, NULL, NULL, 1
FROM franchises f JOIN ballparks b
ON f.key='HEROES' AND b.name_kor='고척스카이돔';

-- KIA → 광주기아챔피언스필드
INSERT INTO home_ballpark_assignments (franchise_id, ballpark_id, start_season, end_season, is_primary)
SELECT f.id, b.id, NULL, NULL, 1
FROM franchises f JOIN ballparks b
ON f.key='KIA' AND b.name_kor='광주기아챔피언스필드';
```

---

## 3) 운영/적재 팁

* **부분 정보 허용**: `start_season/end_season`를 NULL로 두어도 되고, 나중에 KBO 팀 히스토리 페이지 크롤링으로 메워도 됩니다.
* **공유 구장**: 같은 시즌에 LG/두산처럼 **동일 구장 공유**가 가능하도록 설계되어 있습니다(제약 없음).
* **확장 여지**: 필요 시 `league_seasons`, `team_season_membership`(1군/퓨처스), `team_codes`(KBO ID 매핑) 등을 추가해도 스키마 충돌 없이 확장됩니다.
* **UPSERT 예시**: `INSERT ... ON CONFLICT(key) DO UPDATE SET ...` 형태로 `franchises.key` 기준 멱등 초기화가 가능합니다.

원하면 위 스키마를 `backend/db/migrations/000X_teams.sql`로 나눠 드리거나, 현재 SQLite에 바로 적용 가능한 **seed.sql** 파일 형태로 정리해 드릴게요.
