# KBO 크롤링 자동화 가이드 (v2)

## 1. 개요

APScheduler를 사용하여 정규시즌과 퓨처스리그 데이터 수집을 별도의 주기로 자동화합니다.

## 2. 스케줄링 전략: 분리된 작업

데이터 소스와 변경 빈도에 따라 두 개의 독립적인 크롤링 작업을 스케줄링합니다.

### 2.1. 정규시즌 경기 크롤링 (`crawl_games_regular`)
- **목적:** 매일 진행되는 정규시즌 경기의 상세 데이터를 수집하고, 시즌 통계를 롤업합니다.
- **실행 빈도:** **높음 (매일)**
- **권장 시간:** 매일 새벽 3시 (모든 경기가 종료된 후)

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

### 4.1. 동시 실행 충돌 방지 (Locking)
- **문제:** 시즌 누적 기록을 집계하는 동안 다른 프로세스가 `GAME_STATS` 테이블에 데이터를 쓸 경우, 데이터 불일치가 발생할 수 있습니다.
- **해결책:**
  1.  **파일 기반 Lock:** 집계 작업 시작 시 `.lock` 파일을 생성하고, 작업 종료 시 삭제합니다. 다른 프로세스는 이 파일의 존재 여부를 확인하여 대기합니다.
  2.  **DB 레벨 Lock:** `BEGIN IMMEDIATE` 트랜잭션을 사용하여 집계 작업 동안 관련 테이블에 대한 쓰기 Lock을 설정할 수 있습니다. (SQLite)

### 4.2. 재실행과 멱등성
- 모든 데이터 저장 로직은 `UPSERT`를 사용하여 여러 번 실행해도 결과가 동일하도록 **멱등성**을 보장합니다. 따라서 실패한 작업을 단순히 재실행해도 데이터가 중복으로 쌓이지 않습니다.

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
