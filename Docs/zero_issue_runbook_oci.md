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
