# 스케줄러 작업 의존성 다이어그램

> 작성일: 2026-09-04 | 모듈: `src/scheduler/jobs/daily.py`

## 개요

`_JOB_REGISTRY`는 중앙 집중식 작업 상태 추적기를 통해 스케줄러 작업 간의 의존성을 관리합니다. 각 작업은 등록 시점에 의존성을 선언하고, `_can_run_job()`으로 의존성 상태를 검사하여 시작 가능 여부를 결정합니다.

## 핵심 개념

### JobStatus (열거형)

| 상태 | 값 | 의미 |
|------|-----|------|
| SUCCESS | `success` | 작업 완료 |
| FAILURE | `failure` | 작업 실패 |
| RUNNING | `running` | 작업 실행 중 (기본값) |
| SKIPPED | `skipped` | 의존성 미충족으로 건너짐 |

### JobResult (데이터클ASSES)

```python
@dataclass
class JobResult:
    job_name: str
    status: JobStatus
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    dependencies: list[str] = field(default_factory=list)
```

## 의존성 등록

```python
# 의존성 없는 기본 작업
_register_job("crawl_daily_games")

# 의존성 있는 작업
_register_job("backfill_missed_daily_crawls", dependencies=["crawl_daily_games"])

# 다중 의존성
_register_job("daily_gap_report_job", dependencies=["crawl_daily_games", "crawl_p0_non_game_job"])
```

## 의존성 검사

```python
can_run, reason = _can_run_job("backfill_missed_daily_crawls")
# can_run: True/False
# reason: 성공 시 "", 실패 시 사유
```

### 검사 규칙

1. 등록되지 않은 작업 → `True` (의존성 없음)
2. 의존성 목록이 비어 있음 → `True`
3. 모든 의존성의 상태가 `SUCCESS` → `True`
4. 일부 의존성의 상태가 `SUCCESS`가 아님 → `False` + 사유

## 작업 의존성 다이어그램

```
crawl_daily_games (basis)
├── backfill_missed_daily_crawls
│   ├── backfill_phase_detail
│   │   └── backfill_phase_pbp
│   │       └── backfill_phase_preview
│   │           └── backfill_phase_profiles
├── crawl_phase1_extra_job
├── crawl_p1p2_data_job
│   └── lock_health_check_job
├── crawl_p0_non_game_job
│   └── daily_gap_report_job
├── crawl_kbo_press_releases_job
└── crawl_futures_schedule_job
```

## 작업 목록

| 작업 | 의존성 | 설명 |
|------|--------|------|
| `crawl_daily_games` | 없음 | 일일 크롤링 기본 작업 |
| `backfill_missed_daily_crawls` | `crawl_daily_games` | 누락된 일일 크롤링 백필 |
| `backfill_phase_detail` | `crawl_daily_games` | 상세 데이터 백필 |
| `backfill_phase_pbp` | `backfill_phase_detail` | PBP 백필 |
| `backfill_phase_preview` | `backfill_phase_pbp` | 프리뷰 백필 |
| `backfill_phase_profiles` | `backfill_phase_preview` | 프로필 백필 |
| `crawl_phase1_extra_job` | `crawl_daily_games` | Phase 1 추가 크롤링 |
| `crawl_p1p2_data_job` | `crawl_daily_games` | P1/P2 데이터 크롤링 |
| `lock_health_check_job` | `crawl_p1p2_data_job` | 락 상태 헬스 체크 |
| `crawl_p0_non_game_job` | `crawl_daily_games` | 비경기 P0 데이터 |
| `crawl_kbo_press_releases_job` | `crawl_daily_games` | KBO 보도자료 |
| `crawl_futures_schedule_job` | `crawl_daily_games` | 파utures 스케줄 |
| `daily_gap_report_job` | `crawl_daily_games`, `crawl_p0_non_game_job` | 일일 갭 리포트 |

## API

### `get_job_status_summary()`

모든 등록된 작업의 상태를 딕셔너리로 반환합니다.

```python
{
    "crawl_daily_games": {
        "status": "success",
        "message": "Completed",
        "dependencies": [],
        "details": {}
    },
    "backfill_missed_daily_crawls": {
        "status": "skipped",
        "message": "Skipped: Dependency crawl_daily_games has status failure",
        "dependencies": ["crawl_daily_games"]
    }
}
```

### `clear_job_registry()`

레지스트리를 비웁니다. 테스트 간 격리용.

## 예시: 의존성 체인 실패 전파

```python
# 1단계: 기본 작업 실패
_register_job("step1")
_update_job_status("step1", JobStatus.FAILURE, "Error")

# 2단계: 의존성 작업은 자동으로 차단
_register_job("step2", dependencies=["step1"])
can_run, reason = _can_run_job("step2")
# can_run: False, reason: "Dependency step1 has status failure"

# 3단계: 최종 작업도 차단
_register_job("step3", dependencies=["step2"])
can_run, reason = _can_run_job("step3")
# can_run: False, reason: "Dependency step2 not registered"
# (step2가 RUNNING 상태이므로 SUCCESS가 아님)
```

## 관련 모듈

- `src/scheduler/jobs/daily.py`: 의존성 추적 구현
- `src/scheduler/locks.py`: 프로세스 락 관리
- `scripts/maintenance/fix_relay_state.py`: 릴레이 상태 정리 (의존성 패턴 유사)