# CI/CD Pipeline

GitHub Actions 기반, 14개 워크플로와 3개 Composite Action으로 구성됩니다.

운영 DB와 RAG sparse/dense 검색은 모두 Oracle Autonomous Database를 사용합니다. PostgreSQL/pgvector는 production target이 아닌 격리된 local acceptance용으로만 유지합니다. 현재 workflow의 기본 DB fallback은 허용하지 않습니다.

## Composite Actions

| Action | 역할 |
|--------|------|
| `python-env` | Python 3.14 환경 셋업, 패키지 설치, Playwright 캐시 |
| `kbo-job-setup` | Checkout + python-env + 날짜 해석 |
| `notify` | Telegram/Slack 상태 알림 |

## 워크플로 목록

### 일일 파이프라인 (`daily_kbo_sync.yml`)
- **Trigger**: 현재 `workflow_dispatch` (schedule 블록은 운영 재활성화 전 검토 필요)
- **Jobs**: finalize → post-process → quality → daily-extras
- **`daily-extras`**: manual-run complement to the canonical scheduler. The advanced daily
  work (team/player stat crawls, team defense aggregate) moved into the scheduled
  `run_daily_update` pipeline; this job keeps the work the scheduler does not cover
  (milestone/split/draft crawls, alert dispatch, extended integrity gates).
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
- **Jobs**: lint (ruff) → test (pytest matrix: 3.14)

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
- **1 Job**: `pip-audit --local` (설치된 런타임 환경 감사)

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
| `DATABASE_URL` | 예 | 운영 PostgreSQL (Tailscale `100.108.105.65:5432`, `bega_prod`) |
| `ORACLE_WALLET_B64` | 예 | Oracle Wallet zip의 base64 값 |
| `OCI_WALLET_PASSWORD` | 조건부 | Wallet 비밀번호 |
| `YOUTUBE_API_KEY` | 아니오 | 팬 문화 유튜브 API |
| `NAVER_CLIENT_ID/SECRET` | 아니오 | 네이버 API |

### `DATABASE_URL` 을 쓰는 워크플로는 DB 에 닿을 수 없다 (2026-11-23 확인)

운영 PostgreSQL 은 **tailnet 전용**이다(공인 포트 없음). 그런데 저장소 시크릿
`DATABASE_URL` 을 참조하는 워크플로 9개(`daily_kbo_sync`, `weekly_maintenance`,
`backfill`, `kbo_automation`, `kbo_smart_polling`, `daily_preview`, `pitcher_backfill`,
`periodic_extras`, `full_recalculation`)는 **어느 것도 tailnet 에 참여하지 않는다** —
`tailscale/github-action` 을 쓰는 워크플로는 `live_db_verification` 과
`db_availability_watchdog` 두 개뿐이고, 그 둘은 `secrets.DATABASE_URL` 이 아니라
`LIVE_DB_READONLY_URL` 을 쓴다.

즉 **시크릿 값을 고쳐도 위 9개는 접속에 실패한다.** 원인은 값이 아니라 경로다.
`daily_kbo_sync` 의 `schedule:` 은 비활성(주석)이라 자동 실행되지 않으므로 이 결함이
매일 드러나지는 않지만, 수동 dispatch 하면 연결 단계에서 멈춘다. 해소는 둘 중 하나다:

1. 해당 워크플로에 tailnet 참여 스텝을 추가한다(`tag:ci` OAuth — `TS_OAUTH_CLIENT_ID` /
   `TS_OAUTH_SECRET` 필요, 아래 4절 참조).
2. GitHub Actions 가 DB 를 만지지 않는다고 결정하고, DB 작업은 호스트 스케줄러
   (`WINDOWS_SCHEDULER.md`) 단일 경로로 둔다. 이 경우 위 9개는 **실행되지 않는 경로**임을
   문서에 명시해야 한다(지금은 "백업 경로"로 읽힌다).

현재 상태는 2에 가깝다 — `DATABASE_URL` 시크릿은 2026-11-22 에 갱신됐지만, 그 값을
실제로 쓸 수 있는 CI 잡은 아직 하나도 없다.
