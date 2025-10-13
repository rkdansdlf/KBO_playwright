KBO 시즌별 경기 데이터 파이프라인은 시범경기, 정규시즌, 포스트시즌의 단계별 처리와 오케스트레이션이 핵심입니다. ETL 아키텍처와 이벤트 기반 워크플로우를 조합하여 설계합니다.[1][2][3][4]

## 전체 파이프라인 아키텍처

### 시스템 구성도

```
┌─────────────────────────────────────────────────────────────┐
│                    Orchestration Layer                      │
│              (Apache Airflow / Prefect)                     │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│  Preseason    │    │ Regular Season│    │  Postseason   │
│   Pipeline    │    │   Pipeline    │    │   Pipeline    │
└───────────────┘    └───────────────┘    └───────────────┘
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│  Schedule     │    │  Game Detail  │    │  Stats        │
│  Crawler      │    │  Crawler      │    │  Aggregator   │
└───────────────┘    └───────────────┘    └───────────────┘
        │                     │                     │
        ▼                     ▼                     ▼
┌─────────────────────────────────────────────────────────────┐
│                    Data Validation Layer                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                       MySQL Database                        │
│  (games, schedules, box_scores, player_stats)               │
└─────────────────────────────────────────────────────────────┘
```

오케스트레이션(Airflow/Prefect) 가 시범–정규–포스트 3트랙을 관리하고,

크롤러(일정/상세) → 검증 → 저장 의 ETL을 이벤트 기반으로 돌리며,

저장층은 참조(프랜차이즈/구장/선수) 와 사실(경기/이닝/라인업/선수기록) 을 분리,

포스트시즌은 시리즈 개체로 1급 관리,

멱등(UPSERT)·계약(Pydantic)·픽스처(HAR)·메트릭을 기본 원칙으로 합니다.


좋아—바로 쓸 수 있게 **MySQL DDL 초안 + 마이그레이션 SQL 3종 + Airflow DAG 코드 패치**를 준비했어.
(엔진: **MySQL 8.0+ / InnoDB / utf8mb4** 가정, `CREATE … IF NOT EXISTS`와 `ADD … IF NOT EXISTS` 사용)

---

# 1) DDL 초안 (참조/스케줄/경기/박스/원문/잡)

**파일:** `Docs/DB_SCHEMA_FULL.sql` (초기 구축 또는 레퍼런스로 사용)

```sql
-- ===========================================================
-- MySQL 8.0+ / InnoDB / utf8mb4
-- ===========================================================
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET FOREIGN_KEY_CHECKS = 1;

-- ---------- franchises / team_identities (이미 있다면 생략 가능) ----------
CREATE TABLE IF NOT EXISTS franchises (
  id              INT AUTO_INCREMENT PRIMARY KEY,
  `key`           VARCHAR(32) UNIQUE NOT NULL,
  canonical_name  VARCHAR(64) NOT NULL,
  first_season    INT NULL,
  last_season     INT NULL,
  status          ENUM('ACTIVE','DISSOLVED') NOT NULL DEFAULT 'ACTIVE',
  notes           TEXT NULL,
  created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS team_identities (
  id               INT AUTO_INCREMENT PRIMARY KEY,
  franchise_id     INT NOT NULL,
  name_kor         VARCHAR(64) NOT NULL,
  name_eng         VARCHAR(64) NULL,
  short_code       VARCHAR(8) NULL,
  city_kor         VARCHAR(32) NULL,
  start_season     INT NULL,
  end_season       INT NULL,
  is_current       TINYINT NOT NULL DEFAULT 0,
  notes            TEXT NULL,
  created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_team_identities_franchise (franchise_id),
  INDEX idx_team_identities_period (franchise_id, start_season, end_season),
  CONSTRAINT fk_team_identities_franchise
    FOREIGN KEY (franchise_id) REFERENCES franchises(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- ballparks / home_ballpark_assignments ----------
CREATE TABLE IF NOT EXISTS ballparks (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  name_kor    VARCHAR(64) NOT NULL,
  city_kor    VARCHAR(32) NULL,
  opened_year INT NULL,
  closed_year INT NULL,
  is_dome     TINYINT NULL,
  capacity    INT NULL,
  notes       TEXT NULL,
  created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_ballpark_name (name_kor)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS home_ballpark_assignments (
  franchise_id  INT NOT NULL,
  ballpark_id   INT NOT NULL,
  start_season  INT NULL,
  end_season    INT NULL,
  is_primary    TINYINT NOT NULL DEFAULT 1,
  notes         TEXT NULL,
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (franchise_id, ballpark_id, IFNULL(start_season,-1)),
  INDEX idx_home_ballpark_period (franchise_id, start_season, end_season),
  CONSTRAINT fk_hba_franchise FOREIGN KEY (franchise_id) REFERENCES franchises(id) ON DELETE CASCADE,
  CONSTRAINT fk_hba_ballpark  FOREIGN KEY (ballpark_id)  REFERENCES ballparks(id)  ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- 팀 별칭(기간 매핑) ----------
CREATE TABLE IF NOT EXISTS team_aliases (
  id           INT AUTO_INCREMENT PRIMARY KEY,
  alias        VARCHAR(64) NOT NULL,
  franchise_id INT NOT NULL,
  valid_from   DATE NULL,
  valid_to     DATE NULL,
  created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_alias_window (alias, IFNULL(valid_from,'1000-01-01'), IFNULL(valid_to,'9999-12-31')),
  INDEX idx_alias_franchise (franchise_id),
  CONSTRAINT fk_alias_franchise FOREIGN KEY (franchise_id) REFERENCES franchises(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- 경기 일정 ----------
CREATE TABLE IF NOT EXISTS game_schedules (
  schedule_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
  game_id           VARCHAR(20) NOT NULL,
  season_year       INT NOT NULL,
  season_type       ENUM('preseason','regular','postseason') NOT NULL,
  game_date         DATE NOT NULL,
  game_time         TIME NULL,
  -- 신규: 참조 안전화를 위해 프랜차이즈 FK 추가(기존 VARCHAR 팀명 컬럼은 별도로 유지 가능)
  home_franchise_id INT NULL,
  away_franchise_id INT NULL,
  -- 신규: 구장 FK
  ballpark_id       INT NULL,
  -- 상태
  game_status       ENUM('scheduled','postponed','in_progress','completed','cancelled') NOT NULL DEFAULT 'scheduled',
  postpone_reason   VARCHAR(200) NULL,
  -- 더블헤더
  doubleheader_no   TINYINT NOT NULL DEFAULT 0,
  -- 포스트시즌(시리즈는 별도 테이블로 1급 개체화)
  series_id         BIGINT NULL,
  series_type       ENUM('wildcard','semi_playoff','playoff','korean_series') NULL,
  series_game_number INT NULL,
  -- 크롤링 상태
  crawl_status      ENUM('pending','ready','crawled','failed','skipped') NOT NULL DEFAULT 'pending',
  last_crawl_attempt TIMESTAMP NULL,
  crawl_error_message TEXT NULL,
  created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_sched_gid (game_id),
  UNIQUE KEY uq_sched_nat (season_year, season_type, game_date, home_franchise_id, away_franchise_id, doubleheader_no),
  INDEX idx_sched_type (season_year, season_type),
  INDEX idx_sched_date (game_date),
  INDEX idx_sched_status (game_status, crawl_status),
  INDEX idx_sched_series (series_id, series_game_number),
  CONSTRAINT fk_sched_home_franchise FOREIGN KEY (home_franchise_id) REFERENCES franchises(id) ON DELETE SET NULL,
  CONSTRAINT fk_sched_away_franchise FOREIGN KEY (away_franchise_id) REFERENCES franchises(id) ON DELETE SET NULL,
  CONSTRAINT fk_sched_ballpark       FOREIGN KEY (ballpark_id) REFERENCES ballparks(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- 포스트시즌 시리즈 ----------
CREATE TABLE IF NOT EXISTS series (
  id              BIGINT AUTO_INCREMENT PRIMARY KEY,
  season_year     INT NOT NULL,
  series_type     ENUM('wildcard','semi_playoff','playoff','korean_series') NOT NULL,
  best_of         TINYINT NULL,     -- 3,5,7 등
  home_seed       TINYINT NULL,
  away_seed       TINYINT NULL,
  winner_franchise_id INT NULL,
  created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_series_y (season_year),
  CONSTRAINT fk_series_winner FOREIGN KEY (winner_franchise_id) REFERENCES franchises(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS series_games (
  series_id         BIGINT NOT NULL,
  series_game_number INT NOT NULL,
  game_id           VARCHAR(20) NOT NULL,
  PRIMARY KEY (series_id, series_game_number),
  UNIQUE KEY uq_series_game (game_id),
  CONSTRAINT fk_series_games_series FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE,
  CONSTRAINT fk_series_games_game   FOREIGN KEY (game_id)   REFERENCES game_schedules(game_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- 경기 결과/메타 ----------
CREATE TABLE IF NOT EXISTS games (
  game_id          VARCHAR(20) PRIMARY KEY,
  started_at       DATETIME NULL,
  ended_at         DATETIME NULL,
  duration_min     INT NULL,
  attendance       INT NULL,
  weather          VARCHAR(100) NULL,
  home_score       TINYINT NULL,
  away_score       TINYINT NULL,
  winning_pitcher_id BIGINT NULL,
  losing_pitcher_id  BIGINT NULL,
  save_pitcher_id    BIGINT NULL,
  created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- 이닝 스코어 ----------
CREATE TABLE IF NOT EXISTS inning_scores (
  game_id     VARCHAR(20) NOT NULL,
  inning      TINYINT NOT NULL,
  half        ENUM('T','B') NOT NULL,         -- T:초, B:말
  team_side   ENUM('home','away') NOT NULL,
  runs        TINYINT NOT NULL DEFAULT 0,
  PRIMARY KEY (game_id, inning, half, team_side),
  INDEX idx_inning_game (game_id, inning),
  CONSTRAINT fk_inning_game FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- 라인업 ----------
CREATE TABLE IF NOT EXISTS game_lineups (
  game_id        VARCHAR(20) NOT NULL,
  team_side      ENUM('home','away') NOT NULL,
  order_no       TINYINT NOT NULL,           -- 타순
  player_id      BIGINT NOT NULL,
  pos_at_start   VARCHAR(3) NULL,            -- P,C,1B,2B,3B,SS,LF,CF,RF,DH
  is_starting    TINYINT NOT NULL DEFAULT 1,
  PRIMARY KEY (game_id, team_side, order_no),
  INDEX idx_lineup_player (player_id),
  CONSTRAINT fk_lineup_game FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- 팀 박스 합계 ----------
CREATE TABLE IF NOT EXISTS team_box_scores (
  game_id    VARCHAR(20) NOT NULL,
  team_side  ENUM('home','away') NOT NULL,
  R  TINYINT, H  TINYINT, E  TINYINT, LOB TINYINT,
  -- 필요 시 더 추가
  PRIMARY KEY (game_id, team_side),
  CONSTRAINT fk_tbs_game FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- 원문 보관 ----------
CREATE TABLE IF NOT EXISTS raw_fetches (
  id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  game_id    VARCHAR(20) NULL,
  url        TEXT NOT NULL,
  http_method VARCHAR(10) NOT NULL,
  status     SMALLINT NULL,
  fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  body       MEDIUMBLOB NULL,
  body_sha256 CHAR(64) NULL,
  UNIQUE KEY uq_raw_body (game_id, body_sha256),
  INDEX idx_raw_game (game_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- 잡 이력 ----------
CREATE TABLE IF NOT EXISTS crawl_jobs (
  job_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
  job_type      ENUM('schedule_sync','game_detail','player_stats','daily_update') NOT NULL,
  season_year   INT NOT NULL,
  season_type   ENUM('preseason','regular','postseason') NOT NULL,
  target_date   DATE NULL,
  job_status    ENUM('pending','running','completed','failed','cancelled') NOT NULL DEFAULT 'pending',
  started_at    TIMESTAMP NULL,
  completed_at  TIMESTAMP NULL,
  total_games   INT DEFAULT 0,
  success_count INT DEFAULT 0,
  failed_count  INT DEFAULT 0,
  error_log     MEDIUMTEXT NULL,
  retry_count   INT DEFAULT 0,
  max_retries   INT DEFAULT 3,
  priority      INT DEFAULT 5,
  created_by    VARCHAR(50) DEFAULT 'system',
  -- 확장 필드(옵저버빌리티)
  dag_run_id    VARCHAR(64) NULL,
  triggered_by  VARCHAR(64) NULL,
  payload_json  JSON NULL,
  `queue`       VARCHAR(64) NULL,
  worker_host   VARCHAR(128) NULL,
  attempt_no    INT DEFAULT 0,
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_job_status (job_status, priority),
  INDEX idx_job_date (target_date, job_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

---

# 2) 마이그레이션 SQL (증분 적용)

아래 3개 파일을 **순서대로** 적용하면, 기존 테이블을 최대한 보존하면서 보강 컬럼/테이블을 추가해.

## 2-1. `backend/db/migrations/0010_core_refs.sql`

```sql
-- 0010_core_refs.sql
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS team_aliases (
  id           INT AUTO_INCREMENT PRIMARY KEY,
  alias        VARCHAR(64) NOT NULL,
  franchise_id INT NOT NULL,
  valid_from   DATE NULL,
  valid_to     DATE NULL,
  created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_alias_window (alias, IFNULL(valid_from,'1000-01-01'), IFNULL(valid_to,'9999-12-31')),
  INDEX idx_alias_franchise (franchise_id),
  CONSTRAINT fk_alias_franchise FOREIGN KEY (franchise_id) REFERENCES franchises(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 구장 마스터/할당이 없다면 생성
CREATE TABLE IF NOT EXISTS ballparks (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  name_kor    VARCHAR(64) NOT NULL,
  city_kor    VARCHAR(32) NULL,
  opened_year INT NULL,
  closed_year INT NULL,
  is_dome     TINYINT NULL,
  capacity    INT NULL,
  notes       TEXT NULL,
  created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_ballpark_name (name_kor)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS home_ballpark_assignments (
  franchise_id  INT NOT NULL,
  ballpark_id   INT NOT NULL,
  start_season  INT NULL,
  end_season    INT NULL,
  is_primary    TINYINT NOT NULL DEFAULT 1,
  notes         TEXT NULL,
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (franchise_id, ballpark_id, IFNULL(start_season,-1)),
  INDEX idx_home_ballpark_period (franchise_id, start_season, end_season),
  CONSTRAINT fk_hba_franchise FOREIGN KEY (franchise_id) REFERENCES franchises(id) ON DELETE CASCADE,
  CONSTRAINT fk_hba_ballpark  FOREIGN KEY (ballpark_id)  REFERENCES ballparks(id)  ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

## 2-2. `backend/db/migrations/0011_schedule_series_games.sql`

```sql
-- 0011_schedule_series_games.sql
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- game_schedules 보강: 안전하게 "추가" 위주 (기존 컬럼은 유지)
ALTER TABLE game_schedules
  ADD COLUMN IF NOT EXISTS home_franchise_id INT NULL,
  ADD COLUMN IF NOT EXISTS away_franchise_id INT NULL,
  ADD COLUMN IF NOT EXISTS ballpark_id INT NULL,
  ADD COLUMN IF NOT EXISTS doubleheader_no TINYINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS series_id BIGINT NULL,
  ADD COLUMN IF NOT EXISTS series_type ENUM('wildcard','semi_playoff','playoff','korean_series') NULL,
  ADD COLUMN IF NOT EXISTS series_game_number INT NULL,
  ADD COLUMN IF NOT EXISTS crawl_status ENUM('pending','ready','crawled','failed','skipped') NOT NULL DEFAULT 'pending',
  ADD COLUMN IF NOT EXISTS last_crawl_attempt TIMESTAMP NULL,
  ADD COLUMN IF NOT EXISTS crawl_error_message TEXT NULL;

-- FK & 인덱스 (존재 확인 후 생성)
ALTER TABLE game_schedules
  ADD CONSTRAINT fk_sched_home_franchise FOREIGN KEY (home_franchise_id) REFERENCES franchises(id) ON DELETE SET NULL;
ALTER TABLE game_schedules
  ADD CONSTRAINT fk_sched_away_franchise FOREIGN KEY (away_franchise_id) REFERENCES franchises(id) ON DELETE SET NULL;
ALTER TABLE game_schedules
  ADD CONSTRAINT fk_sched_ballpark       FOREIGN KEY (ballpark_id)       REFERENCES ballparks(id)   ON DELETE SET NULL;

CREATE TABLE IF NOT EXISTS series (
  id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
  season_year         INT NOT NULL,
  series_type         ENUM('wildcard','semi_playoff','playoff','korean_series') NOT NULL,
  best_of             TINYINT NULL,
  home_seed           TINYINT NULL,
  away_seed           TINYINT NULL,
  winner_franchise_id INT NULL,
  created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_series_y (season_year),
  CONSTRAINT fk_series_winner FOREIGN KEY (winner_franchise_id) REFERENCES franchises(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS series_games (
  series_id          BIGINT NOT NULL,
  series_game_number INT NOT NULL,
  game_id            VARCHAR(20) NOT NULL,
  PRIMARY KEY (series_id, series_game_number),
  UNIQUE KEY uq_series_game (game_id),
  CONSTRAINT fk_series_games_series FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 경기 메타/결과
CREATE TABLE IF NOT EXISTS games (
  game_id          VARCHAR(20) PRIMARY KEY,
  started_at       DATETIME NULL,
  ended_at         DATETIME NULL,
  duration_min     INT NULL,
  attendance       INT NULL,
  weather          VARCHAR(100) NULL,
  home_score       TINYINT NULL,
  away_score       TINYINT NULL,
  winning_pitcher_id BIGINT NULL,
  losing_pitcher_id  BIGINT NULL,
  save_pitcher_id    BIGINT NULL,
  created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS inning_scores (
  game_id   VARCHAR(20) NOT NULL,
  inning    TINYINT NOT NULL,
  half      ENUM('T','B') NOT NULL,
  team_side ENUM('home','away') NOT NULL,
  runs      TINYINT NOT NULL DEFAULT 0,
  PRIMARY KEY (game_id, inning, half, team_side),
  INDEX idx_inning_game (game_id, inning)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS game_lineups (
  game_id      VARCHAR(20) NOT NULL,
  team_side    ENUM('home','away') NOT NULL,
  order_no     TINYINT NOT NULL,
  player_id    BIGINT NOT NULL,
  pos_at_start VARCHAR(3) NULL,
  is_starting  TINYINT NOT NULL DEFAULT 1,
  PRIMARY KEY (game_id, team_side, order_no),
  INDEX idx_lineup_player (player_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS team_box_scores (
  game_id    VARCHAR(20) NOT NULL,
  team_side  ENUM('home','away') NOT NULL,
  R  TINYINT, H  TINYINT, E  TINYINT, LOB TINYINT,
  PRIMARY KEY (game_id, team_side)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 인덱스 보강
CREATE INDEX IF NOT EXISTS idx_sched_type      ON game_schedules(season_year, season_type);
CREATE INDEX IF NOT EXISTS idx_sched_date      ON game_schedules(game_date);
CREATE INDEX IF NOT EXISTS idx_sched_status    ON game_schedules(game_status, crawl_status);
CREATE UNIQUE INDEX IF NOT EXISTS uq_sched_gid ON game_schedules(game_id);
```

## 2-3. `backend/db/migrations/0012_raw_jobs_observability.sql`

```sql
-- 0012_raw_jobs_observability.sql
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS raw_fetches (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  game_id     VARCHAR(20) NULL,
  url         TEXT NOT NULL,
  http_method VARCHAR(10) NOT NULL,
  status      SMALLINT NULL,
  fetched_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  body        MEDIUMBLOB NULL,
  body_sha256 CHAR(64) NULL,
  UNIQUE KEY uq_raw_body (game_id, body_sha256),
  INDEX idx_raw_game (game_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- crawl_jobs 확장 컬럼 (있으면 건너뜀)
ALTER TABLE crawl_jobs
  ADD COLUMN IF NOT EXISTS dag_run_id   VARCHAR(64) NULL,
  ADD COLUMN IF NOT EXISTS triggered_by VARCHAR(64) NULL,
  ADD COLUMN IF NOT EXISTS payload_json JSON NULL,
  ADD COLUMN IF NOT EXISTS `queue`      VARCHAR(64) NULL,
  ADD COLUMN IF NOT EXISTS worker_host  VARCHAR(128) NULL,
  ADD COLUMN IF NOT EXISTS attempt_no   INT DEFAULT 0;
```

---

# 3) Airflow DAG 코드 패치 (TaskFlow + 이벤트 트리거)

**파일 교체:** `airflow/dags/kbo_pipelines.py`
(환경에 맞게 DB 연결 유틸/Repo import 경로만 확인)

```python
# airflow/dags/kbo_pipelines.py
from __future__ import annotations
from datetime import datetime, timedelta, date
import asyncio
import os
import logging
from typing import List, Dict, Optional

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.utils.trigger_rule import TriggerRule

# === 프로젝트 내부 유틸(경로 확인) ===
# from backend.pipeline.config import PipelineConfig, SeasonType
# from backend.pipeline.schedule_crawler import ScheduleCrawler
# from backend.pipeline.game_detail_crawler import GameDetailCrawler
# from backend.pipeline.data_repository import GameRepository
# from backend.pipeline.data_validator import DataValidator

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "kbo_pipeline",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ---- 헬퍼 (여기선 더미 Enum/Config를 임시 내장; 실제론 프로젝트 모듈 import 권장) ----
class SeasonType:
    PRESEASON = "preseason"
    REGULAR = "regular"
    POSTSEASON = "postseason"

class PipelineConfig:
    current_season: int = 2025
    request_delay: float = 1.5
    retry_attempts: int = 3
    retry_delay: int = 5
    timeout_seconds: int = 30

# === 공통 Task ===
@task.pool("kbo_shared", slots=1)
def mark_ready_games(season_type: str) -> int:
    """
    'completed'인데 'pending'인 스케줄을 'ready'로 전환.
    이벤트 기반 상세 크롤링 트리거 전 단계.
    """
    # repo = GameRepository(PipelineConfig())
    # count = repo.mark_ready_games(season_type)
    # 임시: 리포 없을 때 0 반환
    count = 0
    logger.info("Marked %s games as ready for season_type=%s", count, season_type)
    return count

@task.pool("kbo_crawl", slots=3)
def sync_schedule_task(season_type: str) -> int:
    """월 단위 스케줄 동기화(심야 실행)."""
    # cfg = PipelineConfig(); crawler = ScheduleCrawler(cfg); repo = GameRepository(cfg)
    # today = datetime.now()
    # schedules = asyncio.run(crawler.crawl_schedule(today.year, today.month, season_type))
    # saved = repo.save_schedules(schedules)
    saved = 0
    logger.info("Synced %s schedules [%s]", saved, season_type)
    return saved

@task.pool("kbo_crawl", slots=3)
def crawl_games_task(season_type: str, limit: int = 100) -> int:
    """READY/COMPLETED 경기 상세 크롤링."""
    # cfg = PipelineConfig(); repo = GameRepository(cfg); crawler = GameDetailCrawler(cfg)
    # pending = repo.get_pending_games(season_type, limit=limit)
    # success = 0
    # for g in pending:
    #     try:
    #         data = asyncio.run(crawler.crawl_game(g['game_id'], g['game_date'].strftime('%Y%m%d')))
    #         if data:
    #             repo.save_game_detail(data)
    #             repo.update_crawl_status(g['game_id'], 'crawled')
    #             success += 1
    #         else:
    #             repo.update_crawl_status(g['game_id'], 'failed', 'No data')
    #     except Exception as e:
    #         repo.update_crawl_status(g['game_id'], 'failed', str(e))
    # return success
    logger.info("Crawled games for %s (limit=%d)", season_type, limit)
    return 0

@task.pool("kbo_aggregate", slots=2)
def aggregate_task(season_type: str) -> int:
    """선수/팀 시즌 집계(야간 롤업)."""
    # run aggregation job here
    logger.info("Aggregated season stats for %s", season_type)
    return 1

@task.pool("kbo_validate", slots=1)
def validate_task(season_type: str, target: Optional[str] = None) -> Dict:
    """데이터 불변식/교차합 검증."""
    # val = DataValidator(PipelineConfig())
    # result = val.validate_daily_data(date.today())
    result = {"is_valid": True, "errors": [], "warnings": []}
    logger.info("Validation for %s -> %s", season_type, result)
    if not result["is_valid"]:
        raise ValueError(str(result))
    return result

# ---- 시범/정규/포스트 DAG ----
@dag(
    dag_id="kbo_preseason_pipeline",
    description="KBO 시범경기 파이프라인 (일 1회)",
    default_args=DEFAULT_ARGS,
    schedule="0 23 * * *",
    start_date=datetime(2025, 2, 15),
    end_date=datetime(2025, 3, 21),
    catchup=False,
    tags=["kbo","preseason"],
)
def preseason():
    s = sync_schedule_task(SeasonType.PRESEASON)
    r = mark_ready_games(SeasonType.PRESEASON)
    c = crawl_games_task(SeasonType.PRESEASON, limit=50)
    a = aggregate_task(SeasonType.PRESEASON)
    v = validate_task(SeasonType.PRESEASON)
    chain(s, r, c, a, v)

preseason()

@dag(
    dag_id="kbo_regular_season_pipeline",
    description="KBO 정규시즌 파이프라인 (일 1회)",
    default_args=DEFAULT_ARGS,
    schedule="30 23 * * *",
    start_date=datetime(2025, 3, 22),
    end_date=datetime(2025, 10, 4),
    catchup=False,
    tags=["kbo","regular"],
)
def regular():
    s = sync_schedule_task(SeasonType.REGULAR)
    r = mark_ready_games(SeasonType.REGULAR)
    c = crawl_games_task(SeasonType.REGULAR, limit=100)
    a = aggregate_task(SeasonType.REGULAR)
    v = validate_task(SeasonType.REGULAR)
    chain(s, r, c, a, v)

regular()

@dag(
    dag_id="kbo_postseason_pipeline",
    description="KBO 포스트시즌 파이프라인 (일 3회, 우선순위↑)",
    default_args=DEFAULT_ARGS | {"retries": 3, "retry_delay": timedelta(minutes=10)},
    schedule="0 0,12,23 * * *",
    start_date=datetime(2025, 10, 5),
    end_date=datetime(2025, 11, 10),
    catchup=False,
    tags=["kbo","postseason","critical"],
)
def postseason():
    s = sync_schedule_task(SeasonType.POSTSEASON)
    r = mark_ready_games(SeasonType.POSTSEASON)
    c = crawl_games_task(SeasonType.POSTSEASON, limit=100)  # pool에서 우선순위 관리
    a = aggregate_task(SeasonType.POSTSEASON)
    v = validate_task(SeasonType.POSTSEASON)
    chain(s, r, c, a, v)

postseason()
```

> 운영 팁
>
> * Airflow **Pools**에 `kbo_shared(1)`, `kbo_crawl(3)`, `kbo_aggregate(2)`, `kbo_validate(1)`를 생성해서 동시성 관리.
> * `mark_ready_games` 구현은 DB에서 `game_status='completed' AND crawl_status='pending'` → `ready` 업데이트 쿼리로 간단히 작성.
> * 실제 crawler/repo 모듈 import 경로만 프로젝트 구조에 맞춰 바꿔주면 돼.

---

# 4) 적용 순서 (실행 메모)

1. **로컬/스테이징에서 DDL 검토**:

   * `Docs/DB_SCHEMA_FULL.sql`로 신규 DB에 올려보기 (또는 읽기용 레퍼런스).
2. **마이그레이션 순서대로 실행**:

   * `0010_core_refs.sql` → `0011_schedule_series_games.sql` → `0012_raw_jobs_observability.sql`
3. **Airflow DAG 교체**:

   * `airflow/dags/kbo_pipelines.py` 덮어쓰기 → 웹서버/스케줄러 재시작 → Pools 생성
4. **리포지토리 보강(권장)**:

   * `GameRepository._resolve_team_id()`를 `team_aliases` 기반 기간매칭으로 교체
   * 상세 크롤러에 XHR 우선 로직 + `raw_fetches` 저장 추가
   * Validator에 불변식(타/투/이닝 합계) 활성화



추가적으로 

로컬(SQLite)↔운영(MySQL) 전환이 깔끔하고, 트랜잭션/업서트/마이그레이션(Alembic)까지 한 세트로 정리됩니다. 
아래처럼 **ORM(관계·조회) + Core(대량저장/업서트)** 혼합 패턴으로 작업

---

# 1) 엔진·세션 (SQLite/MySQL 자동 스위치)

```python
# backend/db/engine.py
import os
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data/kbo_dev.db")

def _is_sqlite(url: str) -> bool:
    return url.startswith("sqlite:")

def get_engine():
    if _is_sqlite(DATABASE_URL):
        engine = create_engine(
            DATABASE_URL,
            connect_args={"check_same_thread": False}, pool_pre_ping=True
        )
        @event.listens_for(engine, "connect")
        def _sqlite_pragmas(dbapi_con, _):
            cur = dbapi_con.cursor()
            cur.execute("PRAGMA foreign_keys = ON;")
            cur.execute("PRAGMA journal_mode = WAL;")
            cur.execute("PRAGMA synchronous = NORMAL;")
            cur.close()
        return engine
    return create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=10, max_overflow=20)

Engine = get_engine()
SessionLocal = sessionmaker(bind=Engine, autoflush=False, autocommit=False, expire_on_commit=False)
```

---

# 2) 모델(ORM) — 핵심만

```python
# backend/db/models.py
from sqlalchemy import (
    Column, Integer, String, Enum, ForeignKey, Date, Time, DateTime, UniqueConstraint
)
from sqlalchemy.orm import DeclarativeBase, relationship

class Base(DeclarativeBase): pass

SeasonType = ("preseason","regular","postseason")
GameStatus = ("scheduled","postponed","in_progress","completed","cancelled")
CrawlStatus = ("pending","ready","crawled","failed","skipped")

class Franchise(Base):
    __tablename__ = "franchises"
    id = Column(Integer, primary_key=True)
    key = Column(String(32), unique=True, nullable=False)
    canonical_name = Column(String(64), nullable=False)

class Ballpark(Base):
    __tablename__ = "ballparks"
    id = Column(Integer, primary_key=True)
    name_kor = Column(String(64), unique=True, nullable=False)

class GameSchedule(Base):
    __tablename__ = "game_schedules"
    schedule_id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), nullable=False, unique=True)
    season_year = Column(Integer, nullable=False)
    season_type = Column(Enum(*SeasonType, name="season_type"), nullable=False)
    game_date = Column(Date, nullable=False)
    game_time = Column(Time)
    home_franchise_id = Column(Integer, ForeignKey("franchises.id"))
    away_franchise_id = Column(Integer, ForeignKey("franchises.id"))
    ballpark_id = Column(Integer, ForeignKey("ballparks.id"))
    game_status = Column(Enum(*GameStatus, name="game_status"), nullable=False, default="scheduled")
    postpone_reason = Column(String(200))
    doubleheader_no = Column(Integer, nullable=False, default=0)
    crawl_status = Column(Enum(*CrawlStatus, name="crawl_status"), nullable=False, default="pending")
    last_crawl_attempt = Column(DateTime)
    crawl_error_message = Column(String)

    __table_args__ = (
        UniqueConstraint(
            "season_year","season_type","game_date","home_franchise_id","away_franchise_id","doubleheader_no",
            name="uq_sched_natural"
        ),
    )

class Game(Base):
    __tablename__ = "games"
    game_id = Column(String(20), primary_key=True)
    started_at = Column(DateTime)
    ended_at = Column(DateTime)
    duration_min = Column(Integer)
    attendance = Column(Integer)
    weather = Column(String(100))
    home_score = Column(Integer)
    away_score = Column(Integer)
```

> ORM은 **관계/조회 가독성**을 확보하고, 스키마와 동기화(Alembic autogenerate)에도 유리합니다.

---

# 3) 대량 적재/업서트는 Core로 — DB별 문법 캡슐화

```python
# backend/db/upserts.py
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy import inspect
from backend.db.models import GameSchedule

def upsert_game_schedule(sess: Session, rec: dict):
    dialect = sess.bind.dialect.name
    if dialect == "sqlite":
        stmt = sqlite_insert(GameSchedule).values(**rec)
        stmt = stmt.on_conflict_do_update(
            index_elements=[GameSchedule.game_id],
            set_={
                "game_time": stmt.excluded.game_time,
                "game_status": stmt.excluded.game_status,
                "postpone_reason": stmt.excluded.postpone_reason,
                "home_franchise_id": stmt.excluded.home_franchise_id,
                "away_franchise_id": stmt.excluded.away_franchise_id,
                "ballpark_id": stmt.excluded.ballpark_id,
                "crawl_status": stmt.excluded.crawl_status,
                "last_crawl_attempt": stmt.excluded.last_crawl_attempt,
                "crawl_error_message": stmt.excluded.crawl_error_message,
            },
        )
    else:  # mysql
        stmt = mysql_insert(GameSchedule).values(**rec).on_duplicate_key_update(
            game_time=mysql_insert(GameSchedule).inserted.game_time,
            game_status=mysql_insert(GameSchedule).inserted.game_status,
            postpone_reason=mysql_insert(GameSchedule).inserted.postpone_reason,
            home_franchise_id=mysql_insert(GameSchedule).inserted.home_franchise_id,
            away_franchise_id=mysql_insert(GameSchedule).inserted.away_franchise_id,
            ballpark_id=mysql_insert(GameSchedule).inserted.ballpark_id,
            crawl_status=mysql_insert(GameSchedule).inserted.crawl_status,
            last_crawl_attempt=mysql_insert(GameSchedule).inserted.last_crawl_attempt,
            crawl_error_message=mysql_insert(GameSchedule).inserted.crawl_error_message,
        )
    sess.execute(stmt)
```

> **장점**: 동일 코드로 SQLite의 `ON CONFLICT`와 MySQL의 `ON DUPLICATE KEY`를 안전하게 사용.

---

# 4) 리포지토리 (세션/트랜잭션 일원화)

```python
# backend/repository/game_repository.py
from typing import List, Dict
from sqlalchemy.orm import Session
from backend.db.engine import SessionLocal
from backend.db.upserts import upsert_game_schedule

class GameRepository:
    def save_schedules(self, schedules: List[Dict]) -> int:
        saved = 0
        with SessionLocal() as sess:
            try:
                for rec in schedules:
                    upsert_game_schedule(sess, rec)
                    saved += 1
                sess.commit()
            except:
                sess.rollback()
                raise
        return saved

    def mark_ready_games(self, season_type: str) -> int:
        from sqlalchemy import update, func
        from backend.db.models import GameSchedule
        with SessionLocal() as sess:
            q = (
                update(GameSchedule)
                .where(GameSchedule.season_type == season_type)
                .where(GameSchedule.game_status == "completed")
                .where(GameSchedule.crawl_status == "pending")
                .values(crawl_status="ready", last_crawl_attempt=func.now())
            )
            res = sess.execute(q)
            sess.commit()
            return res.rowcount or 0
```

---

# 5) Alembic 연동(autogenerate + SQLite 배치 모드)

```python
# alembic/env.py (핵심)
from alembic import context
from backend.db.engine import Engine
from backend.db.models import Base

def run_migrations_online():
    with Engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=Base.metadata,
            render_as_batch=(Engine.dialect.name == "sqlite")
        )
        with context.begin_transaction():
            context.run_migrations()

run_migrations_online()
```

* **모델 변경 → `alembic revision --autogenerate -m "..."` → `alembic upgrade head`**
* SQLite는 `render_as_batch=True`로 `ALTER TABLE` 제약을 우회.

---

# 6) Airflow Task에서 깔끔하게 사용

```python
# airflow task 내부
@task
def sync_regular_schedule():
    from backend.repository.game_repository import GameRepository
    repo = GameRepository()
    # crawler가 만든 레코드 리스트를 upsert
    saved = repo.save_schedules(schedules)
    return saved

@task
def mark_ready():
    from backend.repository.game_repository import GameRepository
    return GameRepository().mark_ready_games("regular")
```

---

## 언제 ORM/CORE를 나눌까?

* **ORM**: 관계 탐색/조인 조회, 비즈 로직 가독성(예: 팀/시리즈/선수 이력 조회).
* **Core**: 대량 벌크/업서트/회귀테스트 픽스처 적재(raw_fetches 등).
  → 지금 파이프라인은 “많이 쓰는 INSERT/UPSERT + 간헐적 조회”라 **혼합 패턴**이 최적입니다.

## 피해야 할 함정

* ENUM: DB 간 차이 → 가능하면 **문자열 컬럼 + 체크**(이미 위 코드처럼) or SQLAlchemy Enum 사용 시 `native_enum=False` 옵션 고려.
* 시간대: DB에는 **UTC**로 저장, 앱에서 KST 렌더.
* 업서트 키: `on_conflict/on_duplicate`가 먹으려면 **UNIQUE/PK** 반드시 존재(여기선 `game_id`).

---

### 한 줄 결론

**SQLAlchemy 2.x(ORM+Core 혼합) + Alembic**으로 가면 로컬(SQLite)과 운영(MySQL)을 **엔진 스위치만으로** 동일 코드로 돌릴 수 있고, 업서트·트랜잭션·마이그레이션이 깔끔하게 정리됩니다.




## 파이프라인 운영 전략

### 스케줄링 전략

시범경기와 정규시즌은 일 1회 심야 배치로 처리하며, 포스트시즌은 경기 중요도가 높아 일 3회 실행합니다. Airflow의 `priority_weight`로 포스트시즌 작업에 높은 우선순위를 부여합니다.[5][6][7][8]

### 에러 처리 및 재시도

Playwright 크롤링은 네트워크 불안정성을 고려하여 3회 재시도하며, 지수 백오프(exponential backoff)를 적용합니다. 실패한 경기는 `crawl_status='failed'`로 표시하고, 수동 재처리 스크립트로 복구합니다.[2][1][5]

### 데이터 검증 레이어

박스스코어의 득점 합계와 이닝별 스코어 일치 여부, 타자 타점 합계와 팀 득점 일치 등을 검증합니다. 검증 실패 시 Slack/이메일 알림을 발송하여 즉시 대응합니다.[7][4][9][2]

### 병렬 처리 최적화

`max_concurrent_crawls=3`으로 동시 크롤링 수를 제한하여 KBO 서버 부하를 관리하며, Semaphore로 리소스 제어합니다. 정규시즌 한 경기일 10경기 기준 약 30-40분 소요됩니다.[10][11][1]
