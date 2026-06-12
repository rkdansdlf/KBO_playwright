# KBO 잔여 이슈 0건 런북 (OCI 기준)

## 목표
- `UNRESOLVED_MISSING = 0`
- `NULL player_id = 0` (`game_batting_stats`, `game_pitching_stats`, `game_lineups`)
- 로컬/OCI 지표 및 집합 완전 일치
- 참조 무결성 검증 `PASS` (`check_orphan_data.py --strict`)

## 실행 순서
사전 검증:
```bash
./venv/bin/python scripts/verification/check_orphan_data.py --strict --json --sample-limit 20
./venv/bin/python -m scripts.maintenance.quality_gate --skip-oci
```
CI 또는 agent 세션처럼 CSV 아티팩트 디렉터리에 쓰지 않는 실행에서는 품질 게이트에 `--no-write`를 추가합니다:
```bash
./venv/bin/python -m scripts.maintenance.quality_gate --skip-oci --no-write
```

0. fresh runner 운영 캐시 hydrate
```bash
python3 -m src.cli.hydrate_runtime_from_oci --year YYYY --date YYYYMMDD
```

1. 2019 일정 근거 수집
- 삭제된 legacy 조사 스크립트 대신 현재 데이터 상태는 `scripts/verification/check_orphan_data.py`와 `src.cli.gap_report`로 확인합니다.

2. `game_status_overrides.csv` 수동 확정
- 파일: `/Users/mac/project/KBO_playwright/data/game_status_overrides.csv`
- 허용 상태: `CANCELLED`, `POSTPONED`

3. override용 누락 선수 보강
- 누락 선수 보강은 `python3 -m src.cli.collect_profiles` 또는 필요한 수집 CLI로 수행합니다.

4. 보수적 NULL player_id 해소
```bash
python3 -m scripts.maintenance.resolve_null_player_ids_conservative \
  --overrides-csv /Users/mac/project/KBO_playwright/data/player_id_overrides.csv
```

5. game_status 재계산 (override + evidence 반영)
- 현재 운영 경로는 `python3 -m src.cli.run_daily_update --date YYYYMMDD --sync`이며, 내부에서 대상 날짜 상태 갱신을 수행합니다.

6. OCI 반영 (`game` 경량)
```bash
python3 -m src.cli.sync_oci --games-only
```

7. NULL player_id 변경 연도만 상세 재동기화
```bash
python3 -m src.cli.sync_oci --game-details --year YYYY
```

8. OCI 시퀀스 보정
```bash
./venv/bin/python -m scripts.maintenance.reset_oci_sequences
```

9. 참조 무결성 및 품질 게이트 실행
```bash
./venv/bin/python scripts/verification/check_orphan_data.py --db-url env:OCI_DB_URL --strict --json --sample-limit 20
python3 -m scripts.maintenance.quality_gate
```

10. FK migration 적용
데이터 검증이 먼저 통과한 뒤 repository migration process로 `migrations/oci/023_reference_integrity_foreign_keys.sql`을 적용합니다. 적용 후 다시 검증합니다:
```bash
./venv/bin/python scripts/verification/check_orphan_data.py --db-url env:OCI_DB_URL --strict --json --sample-limit 20
```

## 실시간 운영 phase
- 경기 전:
  - `python3 -m src.cli.daily_preview_batch --date YYYYMMDD`
- 경기 중:
  - `python3 -m src.cli.live_crawler --run-once`
- 경기 종료 직후:
  - `python3 -m src.cli.run_daily_update --date YYYYMMDD --sync`
- 완료 경기 freshness 확인:
  - `python3 -m src.cli.freshness_gate --date YYYYMMDD`

## PlayerGame 데이터 파이프라인

`player_game_batting` / `player_game_pitching`은 경기 단위 스탯(`game_batting_stats`, `game_pitching_stats`)을 선수별로 집계한 파생 테이블입니다.

### 파이프라인 순서
```bash
# 1) 재계산: 완료/무승부 경기의 스탯을 선수별로 집계
python3 -m src.cli.recalc_player_game_stats --date YYYYMMDD --save

# 2) OCI 동기화
python3 -m src.cli.sync_oci --player-game-stats

# 3) 품질 검증 (CI/CD에서 exit code로 활용)
python3 -m scripts.verification.verify_player_game_stats --exit-code
```

### 전체 시즌 재계산
```bash
# 특정 시즌
python3 -m src.cli.recalc_player_game_stats --season 2025 --save

# 특정 경기
python3 -m src.cli.recalc_player_game_stats --game-id 20250401LGSS0 --save

# 변경 사항 미리보기
python3 -m src.cli.recalc_player_game_stats --season 2025 --dry-run
```

### 문제 해결
| 증상 | 원인 | 조치 |
|---|---|---|
| 특정 년도 coverage < 90% | 해당 년도 미처리 | `recalc_player_game_stats --season YYYY --save` 실행 |
| verify에서 avg > obp 경고 | 희생플라이(SF)로 인한 정상 현상 | 확인 불필요 |
| DRAW coverage 낮음 | DRAW 경기 수가 적어 통계적으로 낮음 | 대부분 소수(1~12건) 누락, 무시 가능 |
| DB 연결 오류 | `OCI_DB_URL` 미설정 | `.env` 파일 확인 |

### 데이터 커버리지
- 2018-2026: COMPLETED 88~100%, DRAW 50~100% (연도별 편차 있음)
- 2001-2017: 미처리 (source data 존재 시 backfill 가능)

## GitHub Actions 운영 메모
- `daily_preview.yml`과 `daily_kbo_sync.yml`은 fresh GitHub runner에서 먼저 `hydrate_runtime_from_oci`를 실행합니다.
- `run_daily_update --sync`는 freshness gate를 통과한 뒤에만 OCI publish를 수행합니다.
- `sync_oci --game-details --unsynced-only`는 schedule-only parent `game` 행을 자동으로 제외합니다.
- OCI만 검증하는 fresh runner의 참조 무결성 게이트는 `quality_gate.py --oci-only --no-write`를 사용합니다.
- 릴리스 감사용 CSV 스냅샷이 필요한 운영자 실행에서는 `--no-write`를 생략합니다.
- `daily_kbo_sync.yml` quality job의 PlayerGame verify 단계는 `--exit-code`로 실행되어 품질 게이트 역할을 수행합니다.

## 아티팩트
- 상태 근거:
  - `/Users/mac/project/KBO_playwright/data/game_status_schedule_evidence.csv`
  - `/Users/mac/project/KBO_playwright/data/game_status_schedule_unmatched.csv`
- 상태 수동 확정:
  - `/Users/mac/project/KBO_playwright/data/game_status_overrides.csv`
- 선수 수동 확정:
  - `/Users/mac/project/KBO_playwright/data/player_id_overrides.csv`
- player_id 해소 결과:
  - `null_player_id_conservative_applied_*.csv`
  - `null_player_id_conservative_unresolved_*.csv`
- 품질 게이트 스냅샷 (`--no-write` 없이 실행한 경우):
  - `quality_gate_local_*.csv`
  - `quality_gate_oci_*.csv`
  - `quality_gate_missing_set_diff_*.csv`

## Coach/WPA 복구 판단 기준
- 현재 시즌 운영:
  - 완료 경기의 `game_events`와 `wpa`가 이미 채워져 있고, 완료 경기 기준 missing WPA가 0이면 추가 복구는 불필요합니다.
- 과거 시즌 또는 전체 히스토리 재구성:
  - 최우선 복구 대상은 `game_events`입니다.
  - 필요한 것은 메타/요약 테이블이 아니라 경기별 **raw event source**입니다.
- 필수 복구 필드:
  - `game_id`, `event_seq`, `inning`, `inning_half`, `outs`
  - `batter_id`, `batter_name`, `pitcher_id`, `pitcher_name`
  - `description`, `event_type`, `result_code`, `rbi`
  - `bases_before`, `bases_after`, `base_state`, `home_score`, `away_score`, `score_diff`
  - `wpa`, `win_expectancy_before`, `win_expectancy_after`, `extra_json`
- 선택 복구 대상:
  - `game_play_by_play`는 있으면 좋지만 현재 Coach WPA 리뷰 운영의 필수 조건은 아닙니다.
- 대체 불가한 이유:
  - Coach 승부처 문장과 선수별 WPA 통계는 `game_events`의 이벤트 행과 `game_events.wpa` 합계에 직접 의존합니다.
  - `game_metadata.source_payload` 및 기타 메타/요약성 테이블만으로는 이벤트 단위 승부처 근거를 재생성할 수 없습니다.
