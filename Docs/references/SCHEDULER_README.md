# KBO 크롤링 자동화 가이드 (v2)

## 1. 개요

APScheduler를 사용하여 정규시즌과 퓨처스리그 데이터 수집을 별도의 주기로 자동화합니다.

## 2. 스케줄링 전략: 분리된 작업

데이터 소스와 변경 빈도에 따라 두 개의 독립적인 크롤링 작업을 스케줄링합니다.

### 2.1. 정규시즌 경기 크롤링 (`crawl_games_regular`)
- **목적:** 매일 진행되는 정규시즌 경기의 상세 데이터를 수집하고, 시즌 통계를 롤업합니다.
- **실행 빈도:** **높음 (매일)**
- **권장 시간:** 매일 새벽 3시 (모든 경기가 종료된 후)
- **표준 엔트리포인트:** `src.cli.run_daily_update`를 사용합니다. 이 작업은 월간 스케줄 upsert, 당일 상세 재확정, 릴레이 복구(`scripts/fetch_kbo_pbp.py`), 리뷰/WPA 생성, 파생 테이블 재계산을 순서대로 수행합니다.

### 2.2. 퓨처스리그 프로필 크롤링 (`crawl_futures_profile`)
- **목적:** 전체 선수 프로필을 순회하며 퓨처스리그 누적 기록을 동기화합니다.
- **실행 빈도:** **낮음 (주 1회)**
- **권장 시간:** 매주 일요일 새벽 5시 (서버 트래픽이 가장 적은 시간)

## 3. Cron/워크플로우 예시

### 3.1. `docker-compose.yml` 서비스 예시

```yaml
services:
  scheduler:
    build: .
    command: python scheduler.py
    restart: unless-stopped
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
```

### 3.2. `scheduler.py` 작업 등록 예시

```python
from apscheduler.schedulers.blocking import BlockingScheduler
from tasks.regular_season import crawl_daily_games
from tasks.futures import crawl_all_futures_profiles

scheduler = BlockingScheduler(timezone='Asia/Seoul')

# Job 1: 정규시즌 (매일 새벽 3시)
scheduler.add_job(
    crawl_daily_games,
    trigger='cron',
    hour=3,
    minute=0,
    id='crawl_games_regular',
    name='Crawl daily regular season games and aggregate stats'
)

# Job 2: 퓨처스리그 (매주 일요일 새벽 5시)
scheduler.add_job(
    crawl_all_futures_profiles,
    trigger='cron',
    day_of_week='sun',
    hour=5,
    minute=0,
    id='crawl_futures_profile',
    name='Crawl futures league stats from all player profiles'
)

scheduler.start()
```

## 4. 동시성 및 데이터 무결성

### 4.1. 동시 실행 충돌 방지 (3단계 Locking)
- **문제:** 실시간 업데이트, 일일 집계, 그리고 주간 유지보수 작업이 동시에 실행될 경우 리소스 경합 및 데이터 정합성 문제가 발생할 수 있습니다.
- **해결책:** `scripts/scheduler.py`에는 작업을 성격에 따라 3개의 독립적인 락으로 구분하여 관리하는 **3단계 락 구조**가 도입되었습니다.
  1.  **`LIVE_LOCK`**: `crawl_live_refresh`, `crawl_pregame_refresh` 등 고빈도 실시간 작업 보호.
  2.  **`DAILY_LOCK`**: `crawl_daily_games` 등 핵심 일일 데이터 파이프라인 보호.
  3.  **`MAINTENANCE_LOCK`**: `crawl_all_futures_profiles`, OCI 동기화, 리포트 생성 등 장기 실행 유지보수 작업 보호.
- **구현:** `threading.Lock`을 사용하여 작업 단위별로 `with LOCK_NAME:` 블록을 통해 동시 실행을 제어합니다.

### 4.2. 재실행과 멱등성
- 모든 데이터 저장 로직은 `UPSERT`를 사용하여 여러 번 실행해도 결과가 동일하도록 **멱등성**을 보장합니다. 따라서 실패한 작업을 단순히 재실행해도 데이터가 중복으로 쌓이지 않습니다.
- 수동 상세/릴레이 수집 CLI는 기본적으로 기존 데이터가 있으면 스킵하고, 명시적으로 다시 덮어쓸 때만 `--force`를 사용합니다. 일일 finalize는 당일 경기의 최종 상태를 확정하기 위해 내부적으로 강제 재수집 정책을 사용합니다.

## 5. 실패 처리 및 재시도 정책

### 5.1. 작업 실패 시 재시도
- **정책:** 작업 실패 시, 즉시 재시도하지 않고 지수 백오프(Exponential Backoff)를 적용하여 일정 시간 대기 후 재시도합니다.
- **구현:** `APScheduler`의 `misfire_grace_time`과 `@retry` 데코레이터를 조합하여 구현할 수 있습니다.

```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=60, max=300))
def crawl_daily_games():
    # ... 크롤링 로직 ...
```

### 5.2. 재시도 큐 (Queue)
- **개념:** 3회 이상 재시도에 실패한 작업(예: 특정 `game_id`)은 즉시 폐기하지 않고, 별도의 실패 큐(Failure Queue, 예: Redis 리스트 또는 DB 테이블)에 저장합니다.
- **처리:** 관리자는 주기적으로 큐를 확인하고, 실패 원인(예: 웹사이트 구조 변경)을 분석하여 수동으로 재처리하거나, 수정 후 일괄 재실행할 수 있습니다.
