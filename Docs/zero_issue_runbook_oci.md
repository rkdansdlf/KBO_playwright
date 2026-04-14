# KBO 잔여 이슈 0건 런북 (OCI 기준)

## 목표
- `UNRESOLVED_MISSING = 0`
- `NULL player_id = 0` (`game_batting_stats`, `game_pitching_stats`, `game_lineups`)
- 로컬/OCI 지표 및 집합 완전 일치

## 실행 순서
1. 2019 일정 근거 수집
```bash
python3 /Users/mac/project/KBO_playwright/scripts/maintenance/collect_2019_schedule_status_evidence.py \
  --year 2019 --months 3-10
```

2. `game_status_overrides.csv` 수동 확정
- 파일: `/Users/mac/project/KBO_playwright/data/game_status_overrides.csv`
- 허용 상태: `CANCELLED`, `POSTPONED`

3. override용 누락 선수 보강
```bash
python3 /Users/mac/project/KBO_playwright/scripts/maintenance/enrich_missing_players_for_overrides.py \
  --overrides-csv /Users/mac/project/KBO_playwright/data/player_id_overrides.csv
```

4. 보수적 NULL player_id 해소
```bash
python3 /Users/mac/project/KBO_playwright/scripts/maintenance/resolve_null_player_ids_conservative.py \
  --overrides-csv /Users/mac/project/KBO_playwright/data/player_id_overrides.csv
```

5. game_status 재계산 (override + evidence 반영)
```bash
python3 /Users/mac/project/KBO_playwright/scripts/maintenance/refresh_game_status.py \
  --overrides-csv /Users/mac/project/KBO_playwright/data/game_status_overrides.csv \
  --evidence-csv /Users/mac/project/KBO_playwright/data/game_status_schedule_evidence.csv
```

6. OCI 반영 (`game` 경량)
```bash
python3 -m src.cli.sync_oci --games-only
```

7. NULL player_id 변경 연도만 상세 재동기화
```bash
python3 -m src.cli.sync_oci --game-details --year YYYY
```

8. 품질 게이트 실행
```bash
python3 /Users/mac/project/KBO_playwright/scripts/maintenance/quality_gate.py
```

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
- 품질 게이트 스냅샷:
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
