# KBO_playwright 명령어 완전 가이드

이 문서는 KBO_playwright 프로젝트의 모든 크롤링 명령어와 사용법을 정리한 완전 가이드입니다.

## 📋 목차

1. [빠른 시작](#빠른-시작)
2. [환경 설정](#환경-설정)
3. [데이터베이스 관리](#데이터베이스-관리)
4. [크롤링 명령어](#크롤링-명령어)
5. [자동화 스크립트](#자동화-스크립트)
6. [데이터 무결성 및 유지보수](#데이터-무결성-및-유지보수)
7. [OCI 동기화](#oci-동기화)
7. [문제 해결](#문제-해결)
8. [고급 사용법](#고급-사용법)

---

## 🚀 빠른 시작

### 최신 데이터 수집 (권장)
```bash
# 가상환경 활성화
source venv/bin/activate

# 스케줄러 실행 (maintenance lock 작업 포함)
python3 -m scripts.scheduler

# 수동 일일 업데이트
python3 -m src.cli.run_daily_update --date 20251015 --sync

# 특정 시즌 크롤링
python3 -m src.cli.crawl_schedule --year 2025 --month 10
python3 -m src.cli.collect_games --year 2024 --month 10
python3 -m src.cli.crawl_futures --season 2025 --concurrency 3

# 데이터 품질 검증
python3 -m src.cli.quality_gate_check --year 2025
```

> **참고**: 레거시 스크립트(`crawl_clean_and_sync.sh`, `crawl_all_historical.py`, `crawl_year_range.sh`)는 `python3 -m src.cli.*` 기반의 새 명령어로 대체되었습니다. 상세 명령어는 아래 섹션을 참고하세요.

---

## ⚙️ 환경 설정

### 1. 초기 설정
```bash
# 가상환경 생성 및 활성화
python3 -m venv venv
source venv/bin/activate  # Linux/macOS
# venv\Scripts\activate   # Windows

# 의존성 설치
pip install -r requirements.txt

# Playwright 브라우저 설치
playwright install chromium

# 환경변수 설정
cp .env.example .env
# .env 파일 편집하여 DATABASE_URL 설정
```

### 2. 데이터베이스 초기화
```bash
# SQLite 데이터베이스 생성 및 마이그레이션
python3 -c "from src.db.engine import init_db; init_db()"

# 기본 데이터 시드 (팀, 시즌, 데이터소스)
python3 -m scripts.maintenance.seed_data
```

---

## 🗄️ 데이터베이스 관리

### SQLite 초기화
```bash
# 전체 플레이어 데이터 삭제
./venv/bin/python3 reset_sqlite.py --all

# 특정 년도 데이터만 삭제
./venv/bin/python3 reset_sqlite.py --year 2025

# 연도 범위 삭제
./venv/bin/python3 reset_sqlite.py --range 2020 2025

# 특정 테이블만 초기화
./venv/bin/python3 reset_sqlite.py --all --tables player_season_batting

# 확인 없이 강제 실행
./venv/bin/python3 reset_sqlite.py --all --force
```

### 데이터 검증
```bash
# 데이터 무결성 검증
python3 -m src.cli.health_check

# PlayerGame 스탯 검증
python3 -m scripts.verification.verify_player_game_stats

# PA 공식 검증 (PA = AB + BB + HBP + SH + SF)
python3 -m scripts.maintenance.audit_pa_formula --year 2025

# 전체 품질 게이트 (로컬)
python3 -m src.cli.quality_gate_check --year 2025
```

---

## 🕷️ 크롤링 명령어

### 현대 크롤링 (2002년 이후)

#### 타자 데이터
```bash
# 기본 사용법
./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler \
    --year 2025 \
    --series regular \
    --save

# 전체 옵션
./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler \
    --year 2025 \
    --series regular \
    --save \
    --headless \
    --limit 100
```

#### 투수 데이터
```bash
# 기본 사용법
./venv/bin/python3 -m src.crawlers.player_pitching_all_series_crawler \
    --year 2025 \
    --series regular \
    --save

# 헤드리스 모드로 실행
./venv/bin/python3 -m src.crawlers.player_pitching_all_series_crawler \
    --year 2025 \
    --series regular \
    --save \
    --headless
```

#### 시리즈 옵션
- `regular`: 정규시즌
- `exhibition`: 시범경기
- `wildcard`: 와일드카드 결정전
- `semi_playoff`: 준플레이오프
- `playoff`: 플레이오프
- `korean_series`: 한국시리즈

### 레거시 크롤링 (2001년 이전)

#### 타자 데이터 (단순 구조)
```bash
# 2000년 정규시즌 타자
./venv/bin/python3 -m src.crawlers.legacy_batting_crawler \
    --year 2000 \
    --series regular \
    --save \
    --headless

# 1995년 한국시리즈 타자
./venv/bin/python3 -m src.crawlers.legacy_batting_crawler \
    --year 1995 \
    --series korean_series \
    --save
```

#### 투수 데이터 (단순 구조)
```bash
# 2001년 정규시즌 투수
./venv/bin/python3 -m src.crawlers.legacy_pitching_crawler \
    --year 2001 \
    --series regular \
    --save \
    --headless

# 1990년 시범경기 투수 (100명 제한)
./venv/bin/python3 -m src.crawlers.legacy_pitching_crawler \
    --year 1990 \
    --series exhibition \
    --save \
    --limit 100
```

### Futures 리그 크롤링
```bash
# 전체 Futures 선수 프로필
./venv/bin/python3 -m src.crawlers.futures.futures_batting \
    --save \
    --headless

# 개발/테스트용 (10명 제한)
./venv/bin/python3 -m src.crawlers.futures.futures_batting \
    --limit 10 \
    --save
```

### 은퇴선수 크롤링
```bash
# 특정 연도 범위 은퇴선수
./venv/bin/python3 -m src.cli.crawl_retire \
    --years 1982-2025 \
    --concurrency 3

# 최근 5년 은퇴선수만
./venv/bin/python3 -m src.cli.crawl_retire \
    --years 2020-2025 \
    --concurrency 5
```

### 운영 엔트리포인트 (신규 경기/선수 무결성)
```bash
# 운영 기준: 경기 종료 후 finalize + freshness gate + OCI publish
./venv/bin/python3 -m src.cli.run_daily_update --date 20251015 --sync

# fresh runner에서 운영 캐시를 먼저 OCI에서 hydrate
./venv/bin/python3 -m src.cli.hydrate_runtime_from_oci --year 2025 --date 20251015

# 경기 전 pregame refresh
./venv/bin/python3 -m src.cli.daily_preview_batch --date 20251015

# 경기 중 live refresh 1회
./venv/bin/python3 -m src.cli.live_crawler --run-once

# 완료 경기 릴레이/PBP 복구(표준 경로)
./venv/bin/python3 scripts/fetch_kbo_pbp.py --date 20251015

# 누락 릴레이만 시즌 단위 복구
./venv/bin/python3 scripts/fetch_kbo_pbp.py --season 2025 --missing-only

# 일일 finalize stability summary 기반 soft-failure 재시도
./venv/bin/python3 -m src.cli.retry_daily_failures --date 20251015 --dry-run
./venv/bin/python3 -m src.cli.retry_daily_failures --date 20251015 --apply
./venv/bin/python3 -m src.cli.retry_daily_failures --date 20251015 --apply --sync

# 릴리즈 전 deterministic gate 및 opt-in live smoke
./scripts/verification/crawler_release_check.sh
KBO_LIVE_SMOKE=1 KBO_LIVE_SMOKE_DATE=20251015 ./scripts/verification/crawler_release_check.sh
./venv/bin/python3 -m src.cli.crawler_live_smoke --date 20251015 --scope all --allow-network

# 릴리즈 노트 / 파일 위생 확인
cat Docs/release_notes/crawler_stability_20260511.md
git status --short

# 완료 경기 freshness 검증
./venv/bin/python3 -m src.cli.freshness_gate --date 20251015

# 스케줄만 월 단위 반영
./venv/bin/python3 -m src.cli.crawl_schedule --year 2025 --months 10

# 수동 상세 수집(월 단위 대상 필터; 기존 상세/릴레이는 기본 스킵)
./venv/bin/python3 -m src.cli.collect_games --year 2025 --month 10

# 기존 상세/릴레이를 강제로 다시 수집
./venv/bin/python3 -m src.cli.collect_games --year 2025 --month 10 --force

# 범용 unsynced-only 상세 동기화
# 주의: schedule-only parent game 행은 자동 제외되지만, fresh runner 운영에서는
# run_daily_update 또는 sync_specific_game 경로를 우선 사용
./venv/bin/python3 -m src.cli.sync_oci --game-details --unsynced-only
```

#### 릴레이/PBP 수집 경로 구분
- 완료 경기의 릴레이/PBP 복구는 `scripts/fetch_kbo_pbp.py`를 표준 경로로 사용합니다. 이 스크립트와 deprecated `src.cli.fetch_kbo_pbp` alias는 공통 `relay_recovery_service`를 사용합니다.
- 표준 복구 서비스는 Naver/KBO/import manifest를 순서대로 시도하고, `game_events`와 `game_play_by_play`가 모두 있는 경기는 `--force`가 없으면 건너뜁니다. 한쪽만 있으면 누락된 relay/PBP를 복구 대상으로 유지합니다.
- 경기 중 실시간 릴레이와 스코어보드 스냅샷은 `src.cli.live_crawler`가 담당합니다.
- `src.cli.collect_games`와 `src.cli.crawl_game_details --relay`의 릴레이 수집은 수동 보조 경로입니다. 완료 경기 대량 복구에는 `scripts/fetch_kbo_pbp.py`를 우선 사용하세요.
- 상세/릴레이 통합 수집 CLI는 기본적으로 기존 데이터가 있으면 재수집하지 않습니다. 다시 덮어써야 할 때만 `--force`를 사용합니다.

#### 릴레이 `game_events` 정제/보정
문자중계 저장은 두 갈래입니다. `game_play_by_play`는 이닝 헤더, 투구 로그, 교체/방문, 구분선까지 포함하는 원문 보존용이고, `game_events`는 WPA와 Coach 리뷰가 소비하는 실제 결과 이벤트만 저장합니다. 이닝 헤더, `N구`, `N번타자`, 구분선, 피치클락/비디오판독/경기중단 같은 행정 메시지가 승부처로 보이면 `game_events` 정제가 필요한 상태입니다.

```bash
# 2024-2026 시즌 정제 가능 여부만 점검
./venv/bin/python3 -m src.cli.rebuild_relay_events \
    --season 2024 --season 2025 --season 2026 \
    --dry-run \
    --report-out data/recovery/relay_event_rebuild_dry_run.csv

# 실제 로컬 반영 + OCI에는 game_events만 배치 동기화
./venv/bin/python3 -m src.cli.rebuild_relay_events \
    --season 2024 --season 2025 --season 2026 \
    --apply --sync-oci --oci-sync-mode events \
    --report-out data/recovery/relay_event_rebuild_apply.csv \
    --backup-out data/recovery/relay_event_rebuild_backup.csv

# 특정 경기만 다시 보정
./venv/bin/python3 -m src.cli.rebuild_relay_events \
    --game-id 20260428HTNC0 \
    --apply --sync-oci --oci-sync-mode events

# CSV/텍스트 파일의 경기 목록만 보정
./venv/bin/python3 -m src.cli.rebuild_relay_events \
    --game-ids-file /tmp/game_ids.txt \
    --apply --sync-oci --oci-sync-mode events

# 보정 skip 경기 원천 재수집 후보 점검(저장 없음)
./venv/bin/python3 scripts/fetch_kbo_pbp.py \
    --game-ids-file data/recovery/relay_event_rebuild_skipped_game_ids.txt \
    --force --dry-run \
    --source-order naver,kbo \
    --min-result-events 20 \
    --validate-final-score \
    --report-out data/recovery/relay_skip_recrawl_dry_run.csv

# 검증을 통과한 fresh relay만 저장
./venv/bin/python3 scripts/fetch_kbo_pbp.py \
    --game-ids-file data/recovery/relay_event_rebuild_skipped_game_ids.txt \
    --force \
    --source-order naver,kbo \
    --min-result-events 20 \
    --validate-final-score \
    --report-out data/recovery/relay_skip_recrawl_apply.csv
```

리포트 해석:
- `DRY_RUN_READY`: 적용 가능하지만 `--dry-run`이라 DB를 바꾸지 않음.
- `APPLIED`: 로컬 `game_events`를 경기 단위로 교체함.
- `SKIPPED_TOO_FEW_EVENTS`: 정제 후 결과 이벤트가 기본 기준 20개 미만이라 안전상 미적용. 원천 재수집 후보입니다.
- `SKIPPED_SCORE_MISMATCH`: 정제 이벤트의 최종 점수가 `game` 최종 점수와 달라 미적용. 점수 상태 누락/불완전 문자중계 후보입니다.
- `oci_status=synced_events:<rows>`: 적용 경기의 OCI `game_events`만 삭제 후 재삽입 완료. 전체 경기 child snapshot 동기화가 필요할 때만 `--oci-sync-mode specific-game`을 사용합니다.
- `fetch_kbo_pbp.py`의 `skipped_validation`: fresh relay를 가져왔지만 `--min-result-events` 또는 `--validate-final-score` 기준을 통과하지 못해 저장하지 않음.

#### 보정 후 Coach 리뷰 JSON 재생성
`game_events`를 보정하면 기존 `game_summary`의 `리뷰_WPA` JSON은 예전 승부처를 계속 들고 있을 수 있습니다. 보정 성공 경기 목록만 대상으로 리뷰 JSON을 다시 만들고, OCI에는 `game_summary` 리뷰 행만 빠르게 동기화합니다.

```bash
# 보정 성공 경기 목록으로 변경 예정 리뷰만 점검
./venv/bin/python3 -m src.cli.regenerate_review_summaries \
    --game-ids-file data/recovery/relay_event_rebuild_applied_game_ids.txt \
    --dry-run \
    --report-out data/recovery/review_summary_regen_dry_run.csv

# 로컬 리뷰 JSON 재생성 + OCI game_summary 리뷰 행 동기화
./venv/bin/python3 -m src.cli.regenerate_review_summaries \
    --game-ids-file data/recovery/relay_event_rebuild_applied_game_ids.txt \
    --apply --sync-oci \
    --report-out data/recovery/review_summary_regen_apply.csv \
    --backup-out data/recovery/review_summary_regen_backup.csv

# 특정 날짜 전체 리뷰만 다시 생성
./venv/bin/python3 -m src.cli.regenerate_review_summaries \
    --date 20251015 \
    --apply --sync-oci
```

리포트 해석:
- `DRY_RUN_READY`: 새 리뷰 JSON이 기존 값과 달라질 예정.
- `DRY_RUN_UNCHANGED`: 재생성해도 기존 로컬 리뷰와 동일.
- `APPLIED`: 로컬 `game_summary`의 `리뷰_WPA`를 갱신.
- `UNCHANGED`: 로컬 값은 그대로지만 `--sync-oci` 대상에는 포함.
- `SKIPPED_REVIEW_MOMENT_NOISE`: 재생성 결과에도 헤더/구분선/투구 로그 같은 noise 승부처가 있어 저장하지 않음.
- `oci_status=synced_summary:<rows>`: 대상 경기의 OCI `리뷰_WPA` summary 행을 교체/동기화 완료.

`freshness_gate`는 `missing_review_moments`뿐 아니라 `review_moment_noise`도 검사합니다. 이 항목이 실패하면 `game_events` 정제 또는 리뷰 재생성을 먼저 확인하세요.

#### LLM-ready 경기 스토리 JSON 생성
`game_events`를 원천으로 홈런, 역전/동점 득점, 결정적 실책, 후반 high-WPA 이벤트만 추린 타임라인 JSON을 `game_summary.summary_type = '경기_스토리'`로 저장합니다. `v_ai_game_context`에는 포함하지 않으므로 소비자는 `game_summary`를 직접 조회합니다.

```bash
# 특정 날짜 완료 경기의 경기 스토리 생성
./venv/bin/python3 -m src.cli.daily_story_batch \
    --date 20251015 \
    --no-sync

# 변경 예정 스토리 점검
./venv/bin/python3 -m src.cli.regenerate_game_stories \
    --season 2025 \
    --dry-run \
    --report-out data/reports/game_story_regen_2025_dry_run.csv

# 로컬 스토리 JSON 재생성 + OCI game_summary 경기_스토리 행 동기화
./venv/bin/python3 -m src.cli.regenerate_game_stories \
    --season 2025 \
    --apply --sync-oci \
    --report-out data/reports/game_story_regen_2025_apply.csv \
    --backup-out data/recovery/game_story_regen_2025_backup.csv
```

리포트 해석:
- `DRY_RUN_READY`: 새 경기 스토리 JSON이 기존 값과 달라질 예정.
- `DRY_RUN_UNCHANGED`: 재생성해도 기존 로컬 스토리와 동일.
- `APPLIED`: 로컬 `game_summary`의 `경기_스토리`를 갱신.
- `UNCHANGED`: 로컬 값은 그대로지만 `--sync-oci` 대상에는 포함.
- `SKIPPED_NOT_COMPLETED`: 완료/무승부 상태가 아니어서 생성하지 않음.
- `warnings=missing_game_events`: 원천 이벤트가 없어 빈 timeline으로 생성됨.
- `oci_status=synced_summary:<rows>`: 대상 경기의 OCI `경기_스토리` summary 행을 교체/동기화 완료.

---

## 🤖 자동화 스크립트

### 1. 스케줄러 (권장)
```bash
# 로컬 APScheduler 실행 (실시간/일일/주간 작업 자동화)
python3 -m scripts.scheduler

# 특정 작업만 실행 (다른 프로세스와 충돌 방지)
python3 -m scripts.scheduler --no-backfill

# help
python3 -m scripts.scheduler --help
```

### 2. 일일 데이터 동기화
```bash
# 경기 종료 후 수동 실행
python3 -m src.cli.run_daily_update --date 20251015 --sync
```

### 3. 연도 범위 크롤링
```bash
# 최근 3년 빠르게
python3 -m src.cli.collect_games --year 2024 --month 4
python3 -m src.cli.collect_games --year 2025 --month 5

# 수동 범위 (기존 상세/릴레이는 기본 스킵)
python3 -m src.cli.collect_games --year 2024 --month 10

# 강제 재수집
python3 -m src.cli.collect_games --year 2024 --month 10 --force
```

---

## 🛠️ 데이터 무결성 및 유지보수

시스템의 데이터 일관성을 유지하고, 누락되거나 잘못된 상태의 데이터를 복구하는 도구들입니다.

### 1. 전체 고아 데이터 백필 (Orphan Games Backfill)
스탯 데이터만 있고 경기 정보(메타데이터)가 없는 '고아 데이터'를 대량으로 복구합니다. 취소된 경기를 감지하여 중복 시도를 방지하는 기능이 포함되어 있습니다.

```bash
# 기본 실행 (최대 1000건, 100개씩 배치 처리)
./venv/bin/python3 scripts/crawling/run_full_orphan_backfill.py

# 파라미터 지정 (대상 건수, 배치 크기)
./venv/bin/python3 scripts/crawling/run_full_orphan_backfill.py 2000 50
```

### 2. 누락 선수 프로필 보충 (Missing Player Backfill)
시즌 스탯에는 존재하지만 기본 프로필(`player_basic`)이 없는 선수 정보를 자동으로 수집합니다.

```bash
./venv/bin/python3 scripts/crawling/backfill_missing_players.py
```

### 3. 상태 지연 경기 수정 (Past Scheduled Fix)
과거 날짜임에도 여전히 `SCHEDULED` 상태로 남아있는 경기들을 찾아 `UNRESOLVED_MISSING`으로 업데이트하여 자동 갱신을 유도합니다.

```bash
./venv/bin/python3 -m src.cli.run_daily_update --date YYYYMMDD --sync
```

### 4. Player ID 무결성 보수 (Player ID Repair)
`player_id` NULL 또는 generated placeholder 보수는 보수 범위를 좁혀 순차 실행합니다. curated row-level override는 `data/player_id_row_overrides.csv`에 보관하며, report/backup CSV는 로컬 산출물로 둡니다. row override는 `source_table + game_id + appearance_seq + team_code + player_name` exact key만 수정하고, report `status`가 `needs_update` 또는 `already_correct`일 때만 안전합니다. `missing`, `ambiguous`, `conflict`는 `--apply`를 중단하며 DB를 변경하지 않습니다.

```bash
# 1) 로컬 보수 후보를 보수적으로 해석 (dry-run 후 --apply)
./venv/bin/python3 -m scripts.maintenance.resolve_null_player_ids_conservative \
    --years 2009,2010,2019,2025,2026 \
    --output-dir data/null_player_id_conservative

# 2) 최종 품질 게이트
./venv/bin/python3 -m scripts.maintenance.quality_gate
```

### 5. PlayerGame 스탯 재계산 (Recalc)
경기 단위 스탯(`game_batting_stats`, `game_pitching_stats`)을 선수별로 집계하여 `player_game_batting`/`player_game_pitching`에 저장합니다. 완료/무승부 경기만 처리합니다.

```bash
# 특정 경기만 재계산
./venv/bin/python3 -m src.cli.recalc_player_game_stats --game-id 20250401LGSS0 --save

# 특정 날짜 재계산
./venv/bin/python3 -m src.cli.recalc_player_game_stats --date 20250401 --save

# 특정 시즌 전체 재계산 (2018-2026 지원)
./venv/bin/python3 -m src.cli.recalc_player_game_stats --season 2025 --save

# 변경 사항 미리보기 (저장 안 함)
./venv/bin/python3 -m src.cli.recalc_player_game_stats --season 2025 --dry-run
```

### 6. PlayerGame 데이터 품질 검증
`player_game_batting`/`player_game_pitching` 테이블의 무결성을 자동으로 검증합니다.

| 검증 항목 | 설명 |
|---|---|
| 중복 (game_id, player_id) | 동일 선수가 한 경기에 두 번 이상 집계되지 않았는지 확인 |
| NULL 필드 | player_id, player_name, team_side NULL 여부 확인 |
| Rate stat 범위 | avg/obp 0~1, era 0~200, whip 0~30 범위 내 확인 |
| 년도별 커버리지 | 2018년 이후 각 년도의 경기 coverage 90% 이상인지 확인 |

```bash
# 기본 검증
./venv/bin/python3 -m scripts.verification.verify_player_game_stats

# 상세 연도별 coverage 출력
./venv/bin/python3 -m scripts.verification.verify_player_game_stats --verbose

# CI/CD용: 경고 있으면 exit code 1 또는 2 반환
./venv/bin/python3 -m scripts.verification.verify_player_game_stats --exit-code
```

### 7. 품질 게이트 (Quality Gate Audit)
데이터베이스의 전반적인 무결성 지표를 점검합니다. 로컬 SQLite와 OCI 원격 DB 간의 일치 여부, 고아 데이터 현황, NULL 값 등을 종합적으로 체크합니다.

```bash
# 로컬 DB만 점검
./venv/bin/python3 -m scripts.maintenance.quality_gate --skip-oci

# 로컬 및 OCI 전체 점검 (권한 필요)
./venv/bin/python3 -m scripts.maintenance.quality_gate
```

---

## ☁️ OCI 동기화

### 환경변수 설정
```bash
# OCI PostgreSQL 연결 정보 설정
export OCI_DB_URL='postgresql://user:password@host:5432/bega_backend'
```

### 동기화 명령어
```bash
# 운영 권장: 검증된 경기 상세만 OCI에 반영
python3 -m src.cli.sync_oci --game-details --unsynced-only

# 특정 연도 경기 상세 동기화
python3 -m src.cli.sync_oci --game-details --year 2025

# player_basic 동기화
python3 -m src.cli.sync_oci --player-basic

# PlayerGame 스탯 (game-batting/pitching에서 집계) 동기화
python3 -m src.cli.sync_oci --player-game-stats

# 전체 파이프라인: recalc + sync
python3 -m src.cli.recalc_player_game_stats --season 2025 --save
python3 -m src.cli.sync_oci --player-game-stats

# 시즌 스탯 동기화
python3 -m src.cli.sync_oci --season-stats

# OCI 전체 동기화 (truncate + full sync)
python3 -m src.cli.sync_oci --truncate
```

---

## 📊 품질 게이트 및 데이터 감사

### 1. 통계 품질 게이트 (Quality Gate)
시즌별 타자/투수/PA 공식 통계의 무결성을 검증합니다.

```bash
# 특정 시즌 검증
python3 -m src.cli.quality_gate_check --year 2025

# 전체 시즌 검증
python3 -m src.cli.quality_gate_check --all-years

# OCI 기준 검증
python3 -m src.cli.quality_gate_check --year 2025 --oci
```

### 2. PA 공식 감사 (PA Formula Audit)
`PA = AB + BB + HBP + SH + SF` 공식 위반을 탐지하고 수정합니다.

```bash
# 전체 연도 감사
python3 -m scripts.maintenance.audit_pa_formula --all-years

# 특정 연도 수정 (비율 기반 SH/SF 보정)
python3 -m scripts.maintenance.audit_pa_formula --fix-year 2020

# PBP 기반 SH/SF 백필
python3 -m scripts.maintenance.backfill_sh_sf_from_pbp --year 2020

# 월간 자동 감사 (APScheduler가 매월 1일 03:00 KST 실행)
python3 -m src.cli.monthly_unified_audit
python3 -m src.cli.monthly_unified_audit --team-only
python3 -m src.cli.monthly_pa_audit
```

### 3. Freshness Gate
최근 데이터가 정상적으로 수집되었는지 검증합니다.

```bash
# 기본 검증 (1일 기준)
python3 -m src.cli.freshness_gate --date 20251015

# 확장 검증 (14일 기준, OCI)
python3 -m src.cli.freshness_gate --days 14 --source-url-env OCI_DB_URL
```

### 4. Gap Report
카테고리별 데이터 수집 갭을 분석합니다.

```bash
# 전체 갭 리포트
python3 -m src.cli.gap_report

# 특정 카테고리
python3 -m src.cli.gap_report --categories relay profile
```

### 5. PlayerGame 통계 재계산
경기 단위 스탯을 선수별로 집계합니다.

```bash
# 특정 시즌 전체 재계산
python3 -m src.cli.recalc_player_game_stats --season 2025 --save

# 특정 날짜 재계산
python3 -m src.cli.recalc_player_game_stats --date 20251015 --save

# 특정 경기만 재계산
python3 -m src.cli.recalc_player_game_stats --game-id 20250401LGSS0 --save

# 변경 사항 미리보기
python3 -m src.cli.recalc_player_game_stats --season 2025 --dry-run
```

### 6. 팀/선수 시즌 스탯 재계산
```bash
# 타자 시즌 스탯
python3 -m src.cli.recalc_player_stats --season 2025 --save

# 팀 통계 재계산
python3 -m src.cli.recalc_team_stats --season 2025 --save
```

---

## 🚨 문제 해결

### 크롤링 실패 시
```bash
# 에러 로그 확인
tail -f crawl_errors.log

# 특정 년도/시리즈 재시도
./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler \
    --year 2024 \
    --series regular \
    --save \
    --headless

# 브라우저 표시로 디버깅
./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler \
    --year 2024 \
    --series regular \
    --save
```

### 데이터베이스 문제
```bash
# SQLite 손상 시 재구축
rm data/kbo_dev.db*
python3 -c "from src.db.engine import init_db; init_db()"
python3 -m scripts.maintenance.seed_data

# 팀/시즌 데이터만 빠졌을 경우
python3 -m scripts.maintenance.seed_data

# 외래키 제약조건 확인
python3 -c "
from src.db.engine import SessionLocal
with SessionLocal() as session:
    session.execute('PRAGMA foreign_key_check')
"
```

### 팀 매핑 문제
```bash
# 팀 매핑 테스트
./venv/bin/python3 -c "
from src.utils.team_mapping import get_team_code
print(get_team_code('MBC청룡', 1985))  # LG 트윈스로 매핑
print(get_team_code('해태타이거즈', 1990))  # KIA 타이거즈로 매핑
"

# OCI 팀/시즌 기준 데이터 확인
./venv/bin/python3 scripts/check_oci_summary.py
```

---

## 🎓 고급 사용법

### 개발/테스트 크롤링
```bash
# 소수 데이터로 테스트
./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler \
    --year 2025 \
    --series regular \
    --limit 10

# 브라우저 표시로 디버깅
./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler \
    --year 2025 \
    --series regular \
    --save
```

### 성능 최적화
```bash
# 병렬 크롤링 (조심해서 사용)
./venv/bin/python3 -m src.cli.crawl_retire \
    --years 2020-2025 \
    --concurrency 5

# OCI 변경분 중심 동기화
./venv/bin/python3 -m src.cli.sync_oci --game-details --unsynced-only
```

### 데이터 분석
```bash
# 데이터 통계 확인
./venv/bin/python3 -c "
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from sqlalchemy import func

with SessionLocal() as session:
    # 연도별 데이터 수
    for year in range(2020, 2026):
        batting = session.query(PlayerSeasonBatting).filter_by(season=year).count()
        pitching = session.query(PlayerSeasonPitching).filter_by(season=year).count()
        print(f'{year}년: 타자 {batting}, 투수 {pitching}')
"

# 시리즈별 통계
./venv/bin/python3 -c "
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting
from sqlalchemy import func

with SessionLocal() as session:
    stats = session.query(
        PlayerSeasonBatting.league,
        func.count(PlayerSeasonBatting.id)
    ).group_by(PlayerSeasonBatting.league).all()

    for league, count in stats:
        print(f'{league}: {count}건')
"
```

### 커스텀 크롤링 스크립트
```python
# custom_crawl.py
from src.crawlers.player_batting_all_series_crawler import crawl_batting_stats

# 특정 조건 크롤링
results = crawl_batting_stats(
    year=2025,
    series_key='regular',
    save_to_db=True,
    headless=True
)

print(f"수집된 선수 수: {len(results)}")
```

---

## 📊 크롤링 전략 가이드

### 연도별 권장 전략

#### 1982-2001년 (레거시 모드)
- **특징**: 단순 컬럼 구조
- **타자**: 순위, 선수명, 팀명, AVG, G, PA, AB, H, 2B, 3B, HR, RBI, SB, CS, BB, HBP, SO, GDP, E
- **투수**: 순위, 선수명, 팀명, ERA, G, GS, W, L, SV, HLD, IP, H, HR, BB, SO, R, ER
- **명령어**: `legacy_batting_crawler.py`, `legacy_pitching_crawler.py`

#### 2002년-현재 (현대 모드)
- **특징**: 복합 구조, 상세 통계
- **타자**: 기본 + OPS, wOBA, WAR 등 세이버메트릭스
- **투수**: 기본 + WHIP, FIP, K/9, BB/9 등 고급 통계
- **명령어**: `player_batting_all_series_crawler.py`, `player_pitching_all_series_crawler.py`

### 시리즈별 우선순위
1. **정규시즌** (`regular`): 가장 중요, 우선 크롤링
2. **한국시리즈** (`korean_series`): 포스트시즌 최고 단계
3. **플레이오프** (`playoff`): 준결승/결승
4. **와일드카드** (`wildcard`): 추가 진출전
5. **시범경기** (`exhibition`): 참고용 데이터

### 크롤링 빈도 권장사항
- **정규시즌 중**: 매일 03:00 KST
- **포스트시즌**: 경기 후 즉시
- **비시즌**: 주 1회 (일요일 05:00 KST)
- **역사 데이터**: 월 1회 검증

---

## 🔧 환경별 설정

### 개발 환경
```bash
# SQLite 전용
export DATABASE_URL="sqlite:///./data/kbo_dev.db"

# 빠른 테스트
./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler \
    --year 2025 --series regular --limit 5
```

### 프로덕션 환경
```bash
# PostgreSQL 연결
export DATABASE_URL="postgresql://user:pass@localhost:5432/kbo_prod"
export OCI_DB_URL="postgresql://user:pass@oci-host:5432/bega_backend"

# 안정적인 크롤링
./crawl_clean_and_sync.sh
```

### Docker 환경
```bash
# 컨테이너 빌드
docker-compose build

# 스케줄러 실행
docker-compose up -d scheduler

# 로그 모니터링
docker-compose logs -f scheduler
```

---

## 📝 마무리

이 가이드는 KBO_playwright의 주요 CLI 명령어를 다룹니다. 추가 문서:

- **[AGENTS.md](../../AGENTS.md)**: 최신 명령어, 워크플로우, 시크릿 설정
- **[프로젝트 개요](../ProjectOverview.md)**: 전체 아키텍처
- **[스케줄러 가이드](SCHEDULER_README.md)**: APScheduler 설정
- **[OCI 런북](../zero_issue_runbook_oci.md)**: OCI 운영/품질 게이트
- **[크롤링 제약사항](CRAWLING_LIMITATIONS.md)**: 알려진 이슈들

---

**⚠️ 중요 알림**: KBO 사이트 정책을 준수하고, 크롤링 간격을 충분히 두어 서버에 부하를 주지 않도록 주의하세요.
