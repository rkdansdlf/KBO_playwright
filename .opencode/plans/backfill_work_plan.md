# Tier 1 — 누락 데이터 자동 백필 구현 계획

## 목표

`backfill_missed_daily_crawls()`를 확장하여 3가지 추가 데이터 유형을 자동 복구:
1. PBP/Relay 데이터
2. Pregame preview 데이터
3. Player profile 데이터

## 변경 사항 상세

### 1. `scripts/scheduler.py` — `backfill_missed_daily_crawls()` 리팩토링

#### 새 헬퍼 함수 4개 추가

```python
def _find_detail_gaps(session, start_date: date) -> list[str]
# LEFT JOIN game_batting_stats WHERE NULL → 날짜 목록 반환 (기존 로직)

def _find_pbp_gaps(session, start_date: date) -> list[str]
# LEFT JOIN game_play_by_play WHERE NULL → 날짜 목록 반환 (신규)

def _find_preview_gaps(session, start_date: date) -> list[str]
# LEFT JOIN game_summary(summary_type='프리뷰') WHERE NULL → 날짜 목록 반환 (신규)

def _find_player_profile_gaps(session) -> list[int]
# photo_url IS NULL AND player_id >= 10000 AND status NOT IN ('NOT_FOUND','PSEUDO') → player_id 목록 (신규)

def _compact_date(d) -> str
# date 객체 → "YYYYMMDD" 문자열 변환 (공용)
```

#### 멀티페이즈 오케스트레이션

```
Phase 1 — Detail backfill (기존):
  _find_detail_gaps() → run_daily_update_main(["--date", date])

Phase 2 — PBP/relay backfill (신규):
  _find_pbp_gaps() → run_daily_update_main(["--date", date])
  (Phase 1에서 이미 처리된 날짜는 스킵)

Phase 3 — Pregame preview backfill (신규):
  _find_preview_gaps() → asyncio.run(run_preview_batch(date))

Phase 4 — Player profile backfill (신규):
  _find_player_profile_gaps() → asyncio.run(backfill(limit=5, delay=2.0, ids=batch))
  (사이클당 최대 5명으로 rate-limit)
```

#### 반환값 변경

기존: `list[str]` — 백필된 날짜 문자열
변경: `list[str]` — `"detail:20260603"`, `"pbp:20260603"`, `"preview:20260603"`, `"profiles:5"` 포맷

### 2. 필요한 임포트 추가

```python
# 이미 존재 (line 56):
from src.cli.daily_preview_batch import run_preview_batch

# 새로 추가 필요:
from scripts.backfill_player_profiles import backfill as backfill_player_profiles_fn
```

### 3. 주간 cron 변경 없음

이미 `backfill_missed_crawls` (Sunday 04:00 KST)가 등록되어 있음 (이전 작업 완료).

## 리스크 및 고려사항

| 리스크 | 완화 방안 |
|---|---|
| 프로필 백필이 너무 느림 (Playwright) | 사이클당 5명 제한, delay=2.0 |
| PBP 백필이 중복 실행 | Phase 1/2 날짜 중복 제거 |
| Preview 백필이 비싼 API 호출 | lookback_days=14로 제한, SCHEDULED 게임만 대상 |
| run_daily_update_main이 부분 실패 | 예외 캐치 + 로깅 (기존과 동일) |

## 테스트 계획

1. `python3 -c "from scripts.scheduler import _find_detail_gaps, _find_pbp_gaps, _find_preview_gaps, _find_player_profile_gaps; print('Import OK')"`
2. Python AST 체크: `python3 -c "import ast; ast.parse(open('scripts/scheduler.py').read()); print('Syntax OK')"`
3. 기존 테스트: `pytest tests/test_scheduler_alerting.py tests/test_scheduler_fix.py -x -q`
