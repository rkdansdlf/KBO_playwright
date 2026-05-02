# KBO 데이터 폴백(Fallback) 및 자가 치유 시스템 가이드

## 1. 개요
본 시스템은 KBO 공식 홈페이지의 '시즌 누적 기록' 페이지(`Basic1.aspx`) 장애 상황에 대비하여, 로컬 DB의 상세 경기 기록(`GameBattingStat`, `GamePitchingStat`)을 기반으로 시즌 통계를 자동으로 재계산하고 데이터 정합성을 유지합니다.

## 2. 주요 구성 요소

### 2.1 SeasonStatAggregator (`src/aggregators/season_stat_aggregator.py`)
- **역할:** SQLAlchemy 쿼리를 통해 특정 시즌/시리즈의 상세 기록을 합산.
- **주요 기능:**
    - `aggregate_batting_season`: 특정 타자의 시즌 기록 합산.
    - `aggregate_pitching_season`: 특정 투수의 시즌 기록 합산.
    - `_bulk` 메서드: 전수 재계산을 위한 고성능 집계 로직.

### 2.2 FallbackMonitor (`src/utils/fallback_monitor.py`)
- **역할:** 폴백 이벤트 로깅 및 외부 알림 전송.
- **알림 채널:** Slack Webhook (환경변수 `SLACK_WEBHOOK_URL` 필요).

### 2.3 Audit System (`scripts/verification/audit_fallback_stats.py`)
- **역할:** 공식 수집 데이터와 자체 집계 데이터를 비교하여 오차(Mismatch) 감지.
- **실행:** `PYTHONPATH=. ./venv/bin/python3 scripts/verification/audit_fallback_stats.py --year 2025`

## 3. 운영 가이드

### 3.1 실시간 폴백 (Real-time Fallback)
크롤러(`player_batting_all_series_crawler.py` 등) 실행 중 KBO 사이트 장애가 감지되면 자동으로 DB 집계 모드로 전환됩니다.
- **로그 메시지:** `🔄 [FALLBACK TRIGGERED] ...`
- **데이터 소스 표시:** DB의 `source` 컬럼에 `FALLBACK`으로 기록됨.

### 3.2 수동 재계산 (Manual Recalculation)
과거 데이터의 오차를 수정하거나 대량의 데이터를 보정할 때 사용합니다.
```bash
# 2025년 정규시즌 전체 데이터 재계산 및 저장
python -m src.cli.recalc_season_stats --year 2025 --series regular --type all --save
```

### 3.3 일일 정합성 검사
`run_daily_update.py` 파이프라인의 10.5단계에서 자동으로 수행됩니다. 결과는 로그와 Slack으로 전송됩니다.

## 4. 환경 설정
알림 기능을 활성화하려면 `.env` 파일에 다음을 추가하십시오:
```env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```
