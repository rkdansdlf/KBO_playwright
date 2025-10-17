좋아요—KBO_Data에 맞춰 **“프로필(정적)”과 “기록(동적)”을 분리**하고, 정규시즌/포스트시즌/퓨처스까지 확장 가능한 **SQLite 스키마**를 설계했어요.
핵심 원칙은 **원천(게임·프로필)→집계(시즌) 멱등 UPSERT**, 그리고 **프랜차이즈/브랜딩/리그 단위 추적**입니다.

---

# 1) 설계 원칙 (요약)

* **players**는 불변 ID로 선수의 실체(생년월일·용병 여부 등)를 표현.
* **이름/로마자/개명**은 `player_identities`로 **기간 이력** 관리.
* **소속 이력(팀·등번호·포지션)**은 `player_stints`로 **기간 이력** 관리(프랜차이즈/브랜딩 모두 연결 가능).
* 기록은 **게임 단위(log)** 와 **시즌 단위(rollup)** 를 분리:

  * 게임: `player_game_batting`, `player_game_pitching`, `player_game_fielding`
  * 시즌: `player_season_batting`, `player_season_pitching`, `player_season_fielding`
* 시즌 테이블은 **카운팅 지표(원천)**를 우선 저장하고, **파생 지표(AVG/OBP/ERA/OPS 등)** 는 컬럼 포함하되 **야간 집계로 갱신**(또는 VIEW로 즉시 계산).
* 리그 구분은 `league`(REGULAR | POST | FUTURES) + `level`(KBO1 | KBO2 등)로 확장성 확보.
* 모든 테이블에 `created_at/updated_at`와 **UPSERT-friendly** 유니크 키를 둡니다.

---

# 2) DDL — 프로필 영역

```sql
PRAGMA foreign_keys = ON;

-- 선수 마스터(불변 ID, 인적사항)
CREATE TABLE players (
  id                 INTEGER PRIMARY KEY AUTOINCREMENT,
  kbo_person_id      TEXT,               -- KBO 인물 식별자(있으면 저장)
  birth_date         TEXT,               -- YYYY-MM-DD
  birth_place        TEXT,
  height_cm          INTEGER,
  weight_kg          INTEGER,
  bats               TEXT,               -- R/L/S
  throws             TEXT,               -- R/L
  is_foreign_player  INTEGER NOT NULL DEFAULT 0 CHECK (is_foreign_player IN (0,1)),
  debut_year         INTEGER,            -- 1군 데뷔 연도(알 수 없으면 NULL)
  retire_year        INTEGER,            -- 은퇴 연도(활동 중이면 NULL)
  status             TEXT NOT NULL DEFAULT 'ACTIVE', -- ACTIVE | INACTIVE | RETIRED | MILITARY | ETC
  notes              TEXT,
  created_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (kbo_person_id) -- 있으면 멱등
);
CREATE TRIGGER players_updated_at
AFTER UPDATE ON players
FOR EACH ROW BEGIN
  UPDATE players SET updated_at=CURRENT_TIMESTAMP WHERE id=OLD.id;
END;

-- 이름/표기 이력(개명·로마자 기준 등)
CREATE TABLE player_identities (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  player_id     INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
  name_kor      TEXT NOT NULL,
  name_eng      TEXT,                  -- 로마자/영문 표기
  start_date    TEXT,                  -- YYYY-MM-DD(모르면 NULL)
  end_date      TEXT,                  -- NULL=현재
  is_primary    INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0,1)),
  created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_player_identities_period ON player_identities(player_id, start_date, end_date);
CREATE TRIGGER player_identities_updated_at
AFTER UPDATE ON player_identities
FOR EACH ROW BEGIN
  UPDATE player_identities SET updated_at=CURRENT_TIMESTAMP WHERE id=OLD.id;
END;

-- 소속 이력(팀/브랜딩/등번호/포지션)
-- franchises/team_identities 테이블(이전 답변)과 연결
CREATE TABLE player_stints (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  player_id        INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
  franchise_id     INTEGER NOT NULL,  -- REFERENCES franchises(id)
  identity_id      INTEGER,           -- REFERENCES team_identities(id)  (브랜딩 단위)
  uniform_number   TEXT,
  primary_pos      TEXT,              -- 포지션 약어: P, C, 1B, 2B, 3B, SS, LF, CF, RF, DH, UT
  start_date       TEXT,
  end_date         TEXT,
  is_current       INTEGER NOT NULL DEFAULT 0 CHECK (is_current IN (0,1)),
  notes            TEXT,
  created_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(franchise_id)  REFERENCES franchises(id)     ON DELETE RESTRICT,
  FOREIGN KEY(identity_id)   REFERENCES team_identities(id)ON DELETE SET NULL
);
CREATE INDEX idx_player_stints_period ON player_stints(player_id, start_date, end_date);
CREATE INDEX idx_player_stints_team   ON player_stints(franchise_id, identity_id);
CREATE TRIGGER player_stints_updated_at
AFTER UPDATE ON player_stints
FOR EACH ROW BEGIN
  UPDATE player_stints SET updated_at=CURRENT_TIMESTAMP WHERE id=OLD.id;
END;
```

> 포인트
> 
> * **player_identities**로 개명/표기 변경을 자연스럽게 추적.
> * **player_stints**는 프랜차이즈·브랜딩을 모두 연결할 수 있어 **LG/두산 공동 홈**·브랜딩 교체 등에도 유연.

---

# 3) DDL — 기록(게임 로그)

```sql
-- 공통 키(참조): game_id는 별도 games 테이블의 PK(문자열)라고 가정
-- 배팅 게임 로그
CREATE TABLE player_game_batting (
  game_id        TEXT NOT NULL,
  player_id      INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
  franchise_id   INTEGER NOT NULL,         -- 경기 시점 소속(정규화 용도)
  identity_id    INTEGER,                  -- 경기 시점 브랜딩
  team_side      TEXT CHECK (team_side IN ('HOME','AWAY')),
  batting_order  INTEGER,                  -- 타순(없으면 NULL)
  pos_at_start   TEXT,                     -- 선발 포지션
  G              INTEGER,                  -- 출전(보통 1)
  PA             INTEGER, AB INTEGER, R INTEGER, H INTEGER,
  "2B"           INTEGER, "3B" INTEGER, HR INTEGER, RBI INTEGER,
  BB INTEGER, IBB INTEGER, HBP INTEGER, SO INTEGER,
  SB INTEGER, CS INTEGER, SH INTEGER, SF INTEGER, GDP INTEGER,
  -- 파생(원하면 즉시 계산 대신 저장도 허용)
  AVG REAL, OBP REAL, SLG REAL, OPS REAL, ISO REAL, BABIP REAL,
  source         TEXT NOT NULL,            -- 'GAMECENTER' 등
  created_at     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (game_id, player_id),
  FOREIGN KEY(franchise_id) REFERENCES franchises(id) ON DELETE RESTRICT,
  FOREIGN KEY(identity_id)  REFERENCES team_identities(id) ON DELETE SET NULL
);
CREATE INDEX idx_pgb_player ON player_game_batting(player_id);

-- 투수 게임 로그
CREATE TABLE player_game_pitching (
  game_id        TEXT NOT NULL,
  player_id      INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
  franchise_id   INTEGER NOT NULL,
  identity_id    INTEGER,
  team_side      TEXT CHECK (team_side IN ('HOME','AWAY')),
  is_starting    INTEGER CHECK (is_starting IN (0,1)),
  G INTEGER, GS INTEGER, GF INTEGER, CG INTEGER, SHO INTEGER, SV INTEGER, HLD INTEGER,
  IP_outs INTEGER,  -- 이닝*3(정수)로 저장하면 계산 안전(IP 5.2 = 17 outs)
  H INTEGER, R INTEGER, ER INTEGER, HR INTEGER,
  BB INTEGER, IBB INTEGER, HBP INTEGER, SO INTEGER, WP INTEGER, BK INTEGER,
  -- 파생
  ERA REAL, FIP REAL, WHIP REAL, K9 REAL, BB9 REAL, KBB REAL,
  source      TEXT NOT NULL,
  created_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (game_id, player_id),
  FOREIGN KEY(franchise_id) REFERENCES franchises(id) ON DELETE RESTRICT,
  FOREIGN KEY(identity_id)  REFERENCES team_identities(id) ON DELETE SET NULL
);
CREATE INDEX idx_pgp_player ON player_game_pitching(player_id);

-- 수비 게임 로그(선택: 포지션별 이닝, 에러 등)
CREATE TABLE player_game_fielding (
  game_id       TEXT NOT NULL,
  player_id     INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
  franchise_id  INTEGER NOT NULL,
  identity_id   INTEGER,
  pos           TEXT NOT NULL,           -- C, 1B, 2B, 3B, SS, LF, CF, RF, DH, P
  INN_outs      INTEGER,                 -- 수비 이닝(아웃 개수로 저장)
  PO INTEGER, A INTEGER, E INTEGER, DP INTEGER, PB INTEGER, SB_allowed INTEGER, CS_made INTEGER,
  source        TEXT NOT NULL,
  created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (game_id, player_id, pos),
  FOREIGN KEY(franchise_id) REFERENCES franchises(id) ON DELETE RESTRICT,
  FOREIGN KEY(identity_id)  REFERENCES team_identities(id) ON DELETE SET NULL
);
CREATE INDEX idx_pgf_player ON player_game_fielding(player_id);
```
> 포인트
>
> * **IP_outs**(정수)로 이닝 소수점 문제(예: 5.2이닝) 없이 안전 계산.
> * **파생 컬럼**은 선택 저장(야간 집계로 갱신) 혹은 VIEW로 계산 가능.
> * 유니크 키는 `(game_id, player_id[, pos])`로 멱등 수집 보장.

---

# 4) DDL — 기록(시즌 집계)

```sql
-- 공통: league/split(정규·포스트·퓨처스)와 레벨(KBO1/KBO2)
-- 배팅 시즌 집계
CREATE TABLE player_season_batting (
  player_id      INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
  season         INTEGER NOT NULL,
  league         TEXT NOT NULL,              -- REGULAR | POST | FUTURES
  level          TEXT NOT NULL DEFAULT 'KBO1',
  franchise_id   INTEGER NOT NULL,           -- 시즌 종료 시 소속(또는 누적 기준 소속)
  identity_id    INTEGER,                    -- 시즌 브랜딩
  G INTEGER, PA INTEGER, AB INTEGER, R INTEGER, H INTEGER,
  "2B" INTEGER, "3B" INTEGER, HR INTEGER, RBI INTEGER,
  BB INTEGER, IBB INTEGER, HBP INTEGER, SO INTEGER,
  SB INTEGER, CS INTEGER, SH INTEGER, SF INTEGER, GDP INTEGER,
  -- 파생(야간 집계로 업데이트)
  AVG REAL, OBP REAL, SLG REAL, OPS REAL, ISO REAL, BABIP REAL,
  wOBA REAL, wRC_plus REAL, OPS_plus REAL,
  source        TEXT NOT NULL,               -- 'ROLLUP' | 'PROFILE' 등
  created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (player_id, season, league, level),
  FOREIGN KEY(franchise_id) REFERENCES franchises(id) ON DELETE RESTRICT,
  FOREIGN KEY(identity_id)  REFERENCES team_identities(id) ON DELETE SET NULL
);
CREATE INDEX idx_psb_team ON player_season_batting(season, league, franchise_id);

-- 투수 시즌 집계
CREATE TABLE player_season_pitching (
  player_id      INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
  season         INTEGER NOT NULL,
  league         TEXT NOT NULL,
  level          TEXT NOT NULL DEFAULT 'KBO1',
  franchise_id   INTEGER NOT NULL,
  identity_id    INTEGER,
  W INTEGER, L INTEGER, SV INTEGER, HLD INTEGER, G INTEGER, GS INTEGER, CG INTEGER, SHO INTEGER,
  IP_outs INTEGER, H INTEGER, R INTEGER, ER INTEGER, HR INTEGER, BB INTEGER, IBB INTEGER, HBP INTEGER, SO INTEGER, WP INTEGER, BK INTEGER,
  -- 파생
  ERA REAL, FIP REAL, WHIP REAL, K9 REAL, BB9 REAL, KBB REAL, ERA_plus REAL,
  source        TEXT NOT NULL,
  created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (player_id, season, league, level),
  FOREIGN KEY(franchise_id) REFERENCES franchises(id) ON DELETE RESTRICT,
  FOREIGN KEY(identity_id)  REFERENCES team_identities(id) ON DELETE SET NULL
);
CREATE INDEX idx_psp_team ON player_season_pitching(season, league, franchise_id);

-- 수비 시즌(선택)
CREATE TABLE player_season_fielding (
  player_id      INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
  season         INTEGER NOT NULL,
  league         TEXT NOT NULL,
  level          TEXT NOT NULL DEFAULT 'KBO1',
  franchise_id   INTEGER NOT NULL,
  pos            TEXT NOT NULL,              -- 포지션별 집계
  G INTEGER, GS INTEGER,
  INN_outs INTEGER, PO INTEGER, A INTEGER, E INTEGER, DP INTEGER, PB INTEGER, SB_allowed INTEGER, CS_made INTEGER,
  -- 파생(수비율 등)
  FPCT REAL, RF9 REAL,
  source        TEXT NOT NULL,
  created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (player_id, season, league, level, pos),
  FOREIGN KEY(franchise_id) REFERENCES franchises(id) ON DELETE RESTRICT
);
CREATE INDEX idx_psf_team ON player_season_fielding(season, league, franchise_id);
```

> 포인트
>
> * **PRIMARY KEY에 (player_id, season, league, level)** → 시즌 단위 **멱등 업서트**.
> * **source**로 `PROFILE`(퓨처스 프로필 누적) vs `ROLLUP`(게임 로그 집계) 구분.
> * **파생 지표**는 ETL 재계산 시 업데이트(또는 VIEW).

---

# 5) 파생 지표 VIEW 예시 (선택)

```sql
-- 즉시 계산을 원하면 VIEW 사용(배팅)
CREATE VIEW v_player_season_batting_calc AS
SELECT
  player_id, season, league, level, franchise_id, identity_id,
  G, PA, AB, R, H, "2B","3B", HR, RBI, BB, IBB, HBP, SO, SB, CS, SH, SF, GDP,
  CASE WHEN AB>0 THEN CAST(H AS REAL)/AB ELSE NULL END AS AVG,
  CASE WHEN (AB+BB+HBP+SF)>0 
       THEN CAST(H+BB+HBP AS REAL)/(AB+BB+HBP+SF) ELSE NULL END AS OBP,
  -- SLG: (1B + 2*2B + 3*3B + 4*HR) / AB
  CASE WHEN AB>0 THEN CAST((H-("2B"+"3B"+HR)) + 2*"2B" + 3*"3B" + 4*HR AS REAL)/AB ELSE NULL END AS SLG,
  NULL AS OPS, NULL AS ISO, NULL AS BABIP
FROM player_season_batting;
```

---

# 6) 운영 팁 / UPSERT 스니펫

```sql
-- 프로필(선수) 멱등 삽입
INSERT INTO players (kbo_person_id, birth_date, bats, throws, is_foreign_player)
VALUES (:kbo_person_id, :birth_date, :bats, :throws, :is_foreign)
ON CONFLICT(kbo_person_id) DO UPDATE SET
  birth_date=excluded.birth_date,
  bats=excluded.bats,
  throws=excluded.throws,
  is_foreign_player=excluded.is_foreign_player,
  updated_at=CURRENT_TIMESTAMP;

-- 시즌 배팅 집계 멱등 업서트
INSERT INTO player_season_batting (
  player_id, season, league, level, franchise_id, identity_id,
  G, PA, AB, R, H, "2B","3B", HR, RBI, BB, IBB, HBP, SO, SB, CS, SH, SF, GDP,
  source
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(player_id, season, league, level) DO UPDATE SET
  G=excluded.G, PA=excluded.PA, AB=excluded.AB, R=excluded.R, H=excluded.H,
  "2B"=excluded."2B","3B"=excluded."3B", HR=excluded.HR, RBI=excluded.RBI,
  BB=excluded.BB, IBB=excluded.IBB, HBP=excluded.HBP, SO=excluded.SO,
  SB=excluded.SB, CS=excluded.CS, SH=excluded.SH, SF=excluded.SF, GDP=excluded.GDP,
  franchise_id=excluded.franchise_id, identity_id=excluded.identity_id,
  source=excluded.source, updated_at=CURRENT_TIMESTAMP;
```

---

## 7) 수집 파이프라인 매핑 가이드

* **퓨처스(프로필 기반)**: `player_season_batting/pitching`에 `league='FUTURES', source='PROFILE'`로 직접 UPSERT.
* **정규·포스트(게임 기반)**: 게임 로그(`player_game_*`)를 저장 후, 야간 **ROLLUP**으로 시즌 테이블 갱신(`league='REGULAR'/'POST'`, `source='ROLLUP'`).
* 팀 이력은 **player_stints**를 먼저 업데이트 → 시즌/게임 기록 적재 시점의 `franchise_id/identity_id`를 합류시켜 **연결 일관성** 확보.

---
