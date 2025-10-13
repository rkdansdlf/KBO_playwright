1. **플레이바이플레이(PBP) + 러너 이동**
2. **팀 순위/스탠딩 스냅샷**
3. **로스터/트랜잭션(등록·말소·이적 등)**

* (보너스) **심판/기록원** 메타

아래 DDL은 **SQLite/MySQL 둘 다** 무리 없이 쓰도록 `TEXT+CHECK`를 쓰고, FK/인덱스까지 포함했어.

---

# 1) PBP 이벤트 스키마

```sql
-- 1-1) 이벤트(한 줄 로그 단위) — Relay 피드 기준
CREATE TABLE pbp_events (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  game_id          VARCHAR(20) NOT NULL,
  event_index      INTEGER NOT NULL,                      -- 경기 내 순번(0..N)
  inning           INTEGER NOT NULL,
  half             TEXT NOT NULL CHECK(half IN ('T','B')),
  batter_id        INTEGER NULL,                          -- players.id (있으면)
  pitcher_id       INTEGER NULL,
  batting_order    INTEGER NULL,                          -- 타순(선택)
  count_before_b   TINYINT NULL,                          -- 볼카운트 이전
  count_before_s   TINYINT NULL,
  outs_before      TINYINT NOT NULL DEFAULT 0,
  base_state_before TINYINT NOT NULL DEFAULT 0,           -- 0..7 (000b,001=1루...111=만루)
  event_code       TEXT NOT NULL,                         -- '1B','2B','HR','BB','HBP','K','OUT','E','FC','SACB','SACF','SB','CS','PO','BK','WP','PB','SUB','MISC' 등
  description      TEXT,                                  -- 원문 텍스트
  runs_scored      TINYINT NOT NULL DEFAULT 0,            -- 이 이벤트로 득점난 총 합
  rbi              TINYINT NOT NULL DEFAULT 0,
  outs_after       TINYINT NOT NULL DEFAULT 0,
  base_state_after TINYINT NOT NULL DEFAULT 0,
  is_pa_end        TINYINT NOT NULL DEFAULT 0 CHECK (is_pa_end IN (0,1)), -- 타석 종료 여부
  created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (game_id, event_index)
);

CREATE INDEX idx_pbp_game_inning ON pbp_events(game_id, inning, half);
CREATE INDEX idx_pbp_batter ON pbp_events(batter_id);
CREATE INDEX idx_pbp_pitcher ON pbp_events(pitcher_id);

-- 1-2) 러너 이동(세부 내역): 한 이벤트에 여러 이동이 붙음
CREATE TABLE pbp_runner_advances (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  pbp_event_id  INTEGER NOT NULL,
  runner_id     INTEGER NULL,                 -- players.id (알 수 없으면 NULL)
  from_base     TINYINT NULL,                 -- 0=홈(득점), 1=1루, 2=2루, 3=3루, NULL=타자
  to_base       TINYINT NULL,                 -- 동일
  is_out        TINYINT NOT NULL DEFAULT 0 CHECK(is_out IN (0,1)),
  credited_to   INTEGER NULL,                 -- 수비 책임 선수 등(선택)
  note          TEXT,
  FOREIGN KEY(pbp_event_id) REFERENCES pbp_events(id) ON DELETE CASCADE
);
CREATE INDEX idx_runner_event ON pbp_runner_advances(pbp_event_id);

-- 1-3) (선택) 투구 단위: 카운트·결과를 더 쪼개고 싶을 때
CREATE TABLE pbp_pitches (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  pbp_event_id  INTEGER NOT NULL,
  pitch_no      INTEGER NOT NULL,                       -- 타석 내 투구 순번
  pitch_result  TEXT NOT NULL,                          -- 'B','C','S','F','X','HBP','IBB','WP','BK' 등
  speed_kmh     REAL NULL,                              -- 있으면
  note          TEXT,
  FOREIGN KEY(pbp_event_id) REFERENCES pbp_events(id) ON DELETE CASCADE,
  UNIQUE (pbp_event_id, pitch_no)
);
```

> 왜 이렇게?
>
> * `pbp_events`는 **타석/플레이 중심**으로, 카운트/베이스/아웃 변화까지 한 줄에 담음.
> * 러너 이동은 **정규화 분리**(`pbp_runner_advances`)로 도루·실책·진루/아웃을 정확히 추적.
> * KBO가 투구 단위까지 안정적으로 주지 않을 수 있어 `pbp_pitches`는 **선택**.

---

# 2) 팀 스탠딩/순위 스냅샷

```sql
-- 일자별 스냅샷(리그·레벨별)
CREATE TABLE team_standings_daily (
  snapshot_date   DATE NOT NULL,
  league          TEXT NOT NULL CHECK(league IN ('REGULAR','POST','FUTURES')),
  level           TEXT NOT NULL DEFAULT 'KBO1',
  franchise_id    INTEGER NOT NULL,                     -- franchises.id
  W INTEGER NOT NULL DEFAULT 0,
  L INTEGER NOT NULL DEFAULT 0,
  T INTEGER NOT NULL DEFAULT 0,
  PCT REAL,                                            -- 계산 저장(편의)
  GB  REAL,                                            -- 경기차
  RS  INTEGER, RA INTEGER,                             -- 득·실점(선택)
  home_W INTEGER, home_L INTEGER, away_W INTEGER, away_L INTEGER,
  streak TEXT, last10 TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (snapshot_date, league, level, franchise_id),
  FOREIGN KEY(franchise_id) REFERENCES franchises(id) ON DELETE RESTRICT
);
CREATE INDEX idx_standings_date ON team_standings_daily(snapshot_date, league, level);
```

> 운영 포인트
>
> * **야간 배치**에서 `games`+`team_box_scores`를 집계 → 스냅샷 저장.
> * 포스트시즌용 `series` 테이블과 조합하면 **시드/진행률** 뷰 만들기 쉬움.

---

# 3) 로스터/트랜잭션

```sql
-- 3-1) 트랜잭션 마스터(등록, 말소, 이적, FA, 군입대 등)
CREATE TABLE player_transactions (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  transaction_date DATE NOT NULL,
  type             TEXT NOT NULL CHECK(type IN (
    'REGISTER','DEREGISTER','TRADE','RELEASE','FA_SIGN','LOAN','RETURN',
    'IL_ON','IL_OFF','MIL_ON','MIL_OFF','DRAFT','POSTING'
  )),
  player_id        INTEGER NOT NULL,             -- players.id
  from_franchise_id INTEGER NULL,
  to_franchise_id   INTEGER NULL,
  note             TEXT,
  created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE RESTRICT,
  FOREIGN KEY(from_franchise_id) REFERENCES franchises(id) ON DELETE SET NULL,
  FOREIGN KEY(to_franchise_id)   REFERENCES franchises(id) ON DELETE SET NULL
);
CREATE INDEX idx_txn_date ON player_transactions(transaction_date, type);

-- 3-2) 일자 기준 로스터 스냅샷(선택) — 빠른 질의용 캐시
CREATE TABLE roster_daily (
  snapshot_date  DATE NOT NULL,
  franchise_id   INTEGER NOT NULL,
  player_id      INTEGER NOT NULL,
  status         TEXT NOT NULL CHECK(status IN ('ACTIVE','INACTIVE','IL','MIL','POST','FARM')),
  uniform_number TEXT,
  primary_pos    TEXT,
  PRIMARY KEY (snapshot_date, franchise_id, player_id),
  FOREIGN KEY(franchise_id) REFERENCES franchises(id) ON DELETE CASCADE,
  FOREIGN KEY(player_id)    REFERENCES players(id) ON DELETE CASCADE
);
```

> 운영 포인트
>
> * 파이프라인에서 **공식 공지/명단 변경**을 감지하면 `player_transactions`에 기록하고, 필요하면 `roster_daily`를 생성해 **타임머신 조회**를 빠르게 함.
> * 기존 `player_stints`(장기 소속 이력)과 **보완 관계**.

---

# 4) (보너스) 심판/기록원

```sql
CREATE TABLE officials (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  name_kor   TEXT NOT NULL,
  role_type  TEXT NOT NULL CHECK(role_type IN ('UMPIRE','SCORER','OTHER')),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE game_officials (
  game_id    VARCHAR(20) NOT NULL,
  official_id INTEGER NOT NULL,
  duty       TEXT NOT NULL CHECK(duty IN ('HP','1B','2B','3B','LF','RF','SCORER')),
  PRIMARY KEY (game_id, official_id, duty),
  FOREIGN KEY(game_id) REFERENCES games(game_id) ON DELETE CASCADE,
  FOREIGN KEY(official_id) REFERENCES officials(id) ON DELETE RESTRICT
);
```

---

## 다음 액션(추천 순서)

1. **PBP 파서에 맞춰 `pbp_events`/`pbp_runner_advances`부터 적용**

   * 리플레이 텍스트 → 이벤트코드 매핑 테이블(파이썬 딕셔너리)로 정규화
   * 베이스상태 비트마스크 헬퍼(0..7) 같이 구현
2. 야간 배치에 **`team_standings_daily` 집계 태스크** 추가
3. 공지/명단 변경 소스가 준비되는 즉시 **`player_transactions` 적재** 시작
4. 심판은 박스스코어에서 바로 가져와 **`game_officials`** 채우기
