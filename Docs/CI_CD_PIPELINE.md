# CI/CD Pipeline

GitHub Actions 기반, 14개 워크플로와 3개 Composite Action으로 구성됩니다.

운영 DB는 Oracle Autonomous Database이며, RAG dense 검색은 별도 PostgreSQL/pgvector를 사용합니다. 현재 workflow의 기본 DB fallback은 허용하지 않습니다.

## Composite Actions

| Action | 역할 |
|--------|------|
| `python-env` | Python 3.12 환경 셋업, 패키지 설치, Playwright 캐시 |
| `kbo-job-setup` | Checkout + python-env + 날짜 해석 |
| `notify` | Telegram/Slack 상태 알림 |

## 워크플로 목록

### 일일 파이프라인 (`daily_kbo_sync.yml`)
- **Trigger**: 현재 `workflow_dispatch` (schedule 블록은 운영 재활성화 전 검토 필요)
- **Jobs**: finalize → post-process → quality → advanced-sync
- **Secrets**: `KBO_USER_ID`, `KBO_USER_PWD`

### 경기 전 새로고침 (`daily_preview.yml`)
- **Trigger**: 현재 `workflow_dispatch` (schedule 블록은 주석 처리됨)
- **1 Job**: daily_preview_batch
- **KBO 로그인 필요** (KBO_USER_ID/PWD)

### 투수 Backfill (`pitcher_backfill.yml`)
- **Trigger**: 현재 `workflow_dispatch` (schedule 블록은 주석 처리됨)
- **1 Job**: `backfill_pregame_previews --days-ahead`

### 통계 재계산 (`full_recalculation.yml`)
- **Trigger**: `workflow_dispatch` (수동)
- **Inputs**: year, series
- **Jobs**: recalc_season_stats → recalc_player_game_stats → verify

### 테스트 (`test_suite.yml`)
- **Trigger**: push/PR on main
- **Jobs**: lint (ruff) → test (pytest matrix: 3.12)

### Backfill Matrix (`backfill.yml`)
- **Trigger**: 현재 `workflow_dispatch` (schedule 블록은 주석 처리됨)
- **Matrix**: missed_crawls, player_game_stats, sh_sf, advanced_stats, player_ids, roster
- 6개 작업을 매트릭스로 병렬 실행

### 주간 유지보수 (`weekly_maintenance.yml`)
- **Trigger**: 현재 `workflow_dispatch` (schedule 블록은 주석 처리됨)
- **1 Job**: `run_weekly_maintenance --profile-limit`

### 월간 작업 (`periodic_extras.yml`)
- **Trigger**: 현재 `workflow_dispatch` (schedule 블록은 주석 처리됨)
- **Jobs**: `run_periodic_extras` + `monthly_unified_audit`

### 보안 감사 (`security_audit.yml`)
- **Schedule**: 일요일 21:00 UTC (월 06:00 KST)
- **1 Job**: `pip-audit --requirement requirements.txt`

### Docker 빌드 (`docker_build.yml`)
- **Trigger**: Push to main (Dockerfile/requirements 변경)
- **1 Job**: Buildx → GHCR Push

### 수동 자동화 (`kbo_automation.yml`)
- **Trigger**: `workflow_dispatch`
- **8개 Phase**: pregame → live → finalize → freshness → quality → gap → backfill → recalc

### 스마트 폴링 (`kbo_smart_polling.yml`)
- **Trigger**: 현재 `workflow_dispatch` (schedule 블록은 주석 처리됨)
- **Purpose**: 경기 종료 감지 후 daily update 실행

### Text Relay Docker (`text_relay_docker.yml`)
- **Trigger**: schedule + `workflow_dispatch`
- **Purpose**: 별도 pgvector/RAG와 독립적인 문자중계 Docker 운영

### Historical Team Code Backfill (`backfill_season_team_codes.yml`)
- **Trigger**: `workflow_dispatch`
- **Purpose**: Futures 시즌 팀코드 보정

## Secrets

| Secret | 필수 | 설명 |
|--------|------|------|
| `KBO_USER_ID` | 예 | KBO 웹사이트 로그인 ID |
| `KBO_USER_PWD` | 예 | KBO 웹사이트 로그인 PW |
| `TELEGRAM_BOT_TOKEN` | 예 | Telegram 알림 봇 토큰 |
| `TELEGRAM_CHAT_ID` | 예 | 기본 Telegram 알림 채널 |
| `DATABASE_URL` | 예 | Oracle Autonomous Database URL |
| `ORACLE_WALLET_B64` | 예 | Oracle Wallet zip의 base64 값 |
| `OCI_WALLET_PASSWORD` | 조건부 | Wallet 비밀번호 |
| `YOUTUBE_API_KEY` | 아니오 | 팬 문화 유튜브 API |
| `NAVER_CLIENT_ID/SECRET` | 아니오 | 네이버 API |
