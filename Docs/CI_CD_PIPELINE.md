# CI/CD Pipeline

GitHub Actions 기반, 11개 워크플로와 3개 Composite Action으로 구성됩니다.

## Composite Actions

| Action | 역할 |
|--------|------|
| `python-env` | Python 3.12 환경 셋업, 패키지 설치, Playwright 캐시 |
| `kbo-job-setup` | Checkout + python-env + 날짜 해석 + OCI Hydrate 통합 |
| `notify` | Telegram/Slack 상태 알림 |

## 워크플로 목록

### 일일 파이프라인 (`daily_kbo_sync.yml`)
- **Schedule**: 매일 18:00 UTC (03:00 KST)
- **Jobs**: finalize → post-process → quality → advanced-sync
- **Secrets**: `OCI_DB_URL`, `KBO_USER_ID`, `KBO_USER_PWD`

### 경기 전 새로고침 (`daily_preview.yml`)
- **Schedule**: 경기일 15분 간격 (KST 오전/오후)
- **1 Job**: hydrate → daily_preview_batch
- **KBO 로그인 필요** (KBO_USER_ID/PWD)

### 투수 Backfill (`pitcher_backfill.yml`)
- **Schedule**: 매일 00:00, 14:00 UTC
- **1 Job**: Hydrate → `backfill_pregame_previews --days-ahead`

### 통계 재계산 (`full_recalculation.yml`)
- **Trigger**: `workflow_dispatch` (수동)
- **Inputs**: year, series, sync
- **Jobs**: recalc_season_stats → recalc_player_game_stats → sync_oci → verify

### 테스트 (`test_suite.yml`)
- **Trigger**: push/PR on main
- **Jobs**: lint (ruff) → test (pytest matrix: 3.12)

### Backfill Matrix (`backfill.yml`)
- **Trigger**: Schedule + `workflow_dispatch`
- **Matrix**: missed_crawls, player_game_stats, sh_sf, advanced_stats, player_ids, roster
- 6개 작업을 매트릭스로 병렬 실행

### 주간 유지보수 (`weekly_maintenance.yml`)
- **Schedule**: 일요일 20:00 UTC (월 05:00 KST)
- **1 Job**: `run_weekly_maintenance --profile-limit --sync`

### 월간 작업 (`periodic_extras.yml`)
- **Schedule**: 매월 1일 21:00 UTC (2일 06:00 KST)
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

## Secrets

| Secret | 필수 | 설명 |
|--------|------|------|
| `OCI_DB_URL` | 예 | OCI 데이터베이스 URL |
| `KBO_USER_ID` | 예 | KBO 웹사이트 로그인 ID |
| `KBO_USER_PWD` | 예 | KBO 웹사이트 로그인 PW |
| `TELEGRAM_BOT_TOKEN` | 예 | Telegram 알림 봇 토큰 |
| `TELEGRAM_CHAT_ID` | 예 | 기본 Telegram 알림 채널 |
| `YOUTUBE_API_KEY` | 아니오 | 팬 문화 유튜브 API |
| `NAVER_CLIENT_ID/SECRET` | 아니오 | 네이버 API |
