# Project Progress Overview

## Current Status
- **Schedules**: 2025 preseason, regular season, and initial postseason fixtures ingested from saved HTML; `game_schedules` holds 770 entries total.
- **Game Details**: Box score parser verified (`20251001NCLG0` fixture) with 16 batting and 16 pitching rows stored in `player_game_batting` / `player_game_pitching`.
- **Validation**: Game data validator active; failures block persistence and update crawl status with diagnostics.
- **Futures Pipeline**: ✅ **OPERATIONAL** - Year-by-year Futures batting stats crawler implemented and tested successfully.
- **Mock Ingest**: End-to-end demo script (`run_pipeline_demo`) supports offline HTML ingestion and reporting.

## Recent Improvements (2025-10-14)
- Added offline schedule/game parsers and ingest CLIs for reproducible loads.
- Implemented structured player-season models with provenance (`source='PROFILE'` for Futures, `source='GAMECENTER'` for game logs).
- Hardened error handling in repositories (validation, crawl status updates, reporting helpers).
- Created `.gitignore` entries for venvs, caches, local data, and fixtures.

### ✅ **NEW: Futures Batting Crawler Completed (2025-10-14)**

**Implementation:**
1. **Crawler** (`src/crawlers/futures_batting.py`):
   - Fetches year-by-year Futures stats from `HitterTotal.aspx` page
   - Parses season records with header normalization (handles Korean/English mixed headers)
   - Extracts: season, AVG, G, AB, R, H, 2B, 3B, HR, RBI, SB, BB, HBP, SO, SLG, OBP
   - Computes missing SLG/OBP if not present in source
   - Uses Playwright with `networkidle` wait for JavaScript-rendered content
   - BeautifulSoup for robust HTML parsing

2. **Repository** (`src/repositories/save_futures_batting.py`):
   - UPSERT logic for `player_season_batting` table
   - Compatible with SQLite and MySQL dialects
   - PK constraint: `(player_id, season, league='FUTURES', level='KBO2')`
   - Idempotent saves (safe to re-run)

3. **Testing:**
   - E2E test verified with player `51868` (고명준)
   - Successfully parsed 5 seasons (2021-2025)
   - Data saved and verified in database
   - Test file: `test_futures_e2e.py`

**Key Technical Solutions:**
- ✅ Resolved Windows cp949 encoding issues with `safe_print` utility
- ✅ Correct URL pattern: `/Futures/Player/HitterTotal.aspx` (not `HitterDetail`)
- ✅ Header normalization map handles Korean/English/mixed column names
- ✅ Robust table detection via label proximity + header heuristics

**Sample Output:**
```
2021: AVG=0.258, G=35, H=31, HR=4
2022: AVG=0.077, G=4, H=1, HR=0
2023: AVG=0.289, G=66, H=57, HR=5
2024: AVG=0.267, G=5, H=4, HR=0
2025: AVG=0.357, G=4, H=5, HR=1
```

## ✅ Completed Tasks (2025-10-14 Session)

### 1. Futures CLI Integration ✅
- ~~Integrate Futures Crawler into CLI~~ **COMPLETE**
- `src/cli/crawl_futures.py` with bulk player processing
- Concurrent crawling with semaphore control (--concurrency, --delay)
- Progress reporting and error handling
- Successfully tested with multiple players

### 2. Team Data Infrastructure ✅
- ~~Seed KBO Team Data~~ **COMPLETE**
- `seed_teams.py` executed successfully
- 11 franchises, 21 team identities, 9 ballparks, 7 home assignments
- Based on `Docs/schema/KBO_teams_schema.md`

### 3. Data Status Reporting ✅
- ~~Create SQL/CLI reporting checks~~ **COMPLETE**
- `src/cli/check_data_status.py` with validation
- Checks: schedules, players, Futures data, game stats
- Expected count validation with warnings

### 4. Scheduler Automation ✅
- ~~Integrate APScheduler~~ **COMPLETE**
- `scheduler.py` with 2 jobs:
  - Daily games crawl (03:00 KST)
  - Weekly Futures sync (Sunday 05:00 KST)
- `Dockerfile` and `docker-compose.yml` for deployment
- `SCHEDULER_USAGE.md` documentation
- Retry logic with tenacity (exponential backoff)

### 5. RELAY Crawler Infrastructure ✅
- ~~RELAY section crawler infrastructure~~ **COMPLETE**
- `src/crawlers/relay_crawler.py` (play-by-play logic)
- `src/models/game.py` (`GamePlayByPlay` model added)
- `src/repositories/relay_repository.py` (save/query logic)
- **Status**: Infrastructure ready, needs live postseason game for final testing

### 6. Project Organization ✅
- ~~Organize test files~~ **COMPLETE**
- All `test_*.py` and `debug_*.py` moved to `tests/` folder
- Clean project root structure

## Pending Tasks
- **RELAY Crawler Testing**: Test with live postseason game (selector verification needed)
- **Postseason Schedule**: Capture remaining 2025 postseason fixtures
- **Expand Mock Fixtures**: Add more games for regression testing

## Next Steps
1. **Verify RELAY Section**: Confirm KBO GameCenter RELAY tab structure for postseason games
2. **Postseason Data**: Capture final postseason schedule once all matchups confirmed
3. **Production Deployment**: Deploy scheduler with `docker-compose up -d scheduler`
4. **Monitoring**: Set up alerts for crawler failures and data quality checks

---
_Last updated: 2025-10-14 (Session 2)_


 오늘 세션에서 완료한 주요 작업들:

  ✅ 1. 팀 데이터 초기화

  - seed_teams.py 실행 완료
  - 11개 프랜차이즈, 21개 팀 명칭, 9개 구장, 7개 홈구장 데이터 저장

  ✅ 2. Futures CLI 통합

  - src/cli/crawl_futures.py - 대량 선수 처리, 동시성 제어, 진행률 보고

  ✅ 3. 데이터 상태 체크

  - src/cli/check_data_status.py - 스케줄/선수/Futures 데이터 검증

  ✅ 4. 스케줄러 자동화

  - scheduler.py - 일일 경기 크롤링 (03:00 KST) + 주간 Futures 동기화 (일요일 05:00 KST)
  - Dockerfile, docker-compose.yml, SCHEDULER_USAGE.md

  ✅ 5. RELAY 크롤러 인프라

  - src/crawlers/relay_crawler.py - Play-by-play 데이터 수집 로직
  - src/models/game.py - GamePlayByPlay 모델
  - src/repositories/relay_repository.py - 저장/조회 로직
  - 상태: 인프라 완성, 실제 포스트시즌 경기로 최종 테스트 필요

  ✅ 6. 프로젝트 정리

  - 모든 테스트/디버그 파일을 tests/ 폴더로 이동

  ---
  현재 프로젝트 상태: 운영 준비 완료!
  - Futures 파이프라인 작동 확인
  - 스케줄러 배포 가능
  - RELAY는 실제 게임 데이터로 최종 검증 후 운영 투입 가능

  모든 주요 작업이 완료되었습니다! 🚀

> 그럼 도커 서버 열기전에 mysql 서버에 붙어서 할거라서 env 파일 생성해서 sqlite에 데이터가 온전하면 mysql에         
저장하는 로직까지 구현해보자 