# KBO 크롤링 제한사항 및 한계 (v2)

## 1. 정규시즌 (경기 단위 수집) 시 고려사항

### 1.1. API 호출량 증가 및 Rate Limiting
- **이슈:** 경기 단위 데이터 수집은 `선수 수 × 경기 수`에 비례하여 KBO 웹사이트에 대한 요청 횟수가 크게 증가합니다.
- **정책:**
  - **엄격한 Rate Limiting:** 모든 요청 사이에 최소 1~2초의 간격(`time.sleep`)을 강제하여 서버 부하를 최소화합니다.
  - **지수 백오프(Exponential Backoff):** 429(Too Many Requests) 에러 또는 일시적인 네트워크 오류 발생 시, 재시도 간격을 점진적으로 늘려(예: 1s, 2s, 4s...) 서버를 보호합니다.
  - **실행 시간 분산:** 모든 크롤링 작업은 트래픽이 가장 적은 새벽 시간대(02:00 ~ 05:00 KST)에 집중하여 실행합니다.

### 1.2. 데이터 완전성 문제
- **이슈:** 경기 종료 직후에는 일부 데이터(예: PBP, 상세 분석)가 아직 집계되지 않았을 수 있습니다.
- **정책:**
  - **지연 수집:** 경기 종료 시점으로부터 최소 1시간 이상 지난 후에 데이터를 수집하는 것을 원칙으로 합니다.
  - **재검증 로직:** 일일 롤업 작업 시, 전날 수집된 데이터의 완전성을 재확인하고 누락된 부분이 있다면 추가 수집을 시도합니다.

### 1.3. 중복 수집 방지와 예외 경로
- **기본 정책:** 수동 상세 수집 CLI(`collect_games`, `crawl_game_details`)와 주요 백필 스크립트는 기존 박스스코어/릴레이 데이터가 있으면 기본적으로 재수집하지 않습니다. 다시 덮어써야 할 때만 `--force` 또는 스크립트 내부의 명시적 repair 모드를 사용합니다.
- **완료 경기 PBP 표준 경로:** 완료 경기 릴레이/PBP 복구는 `scripts/fetch_kbo_pbp.py`를 사용합니다. 이 스크립트와 deprecated `src.cli.fetch_kbo_pbp` alias는 공통 `relay_recovery_service`를 사용하며, `game_events`와 `game_play_by_play`가 모두 있는 경기만 기본 skip합니다. 실시간 수집은 `src.cli.live_crawler`가 담당합니다.
- **공통 상세 저장 서비스 예외:** `live_crawler`의 lightweight snapshot, orphan parent 복구, 기존 player_id 보존용 커스텀 복구 스크립트, bootstrap/demo 워크플로우, 디버그/검증 스크립트는 의도적으로 직접 크롤러 호출을 유지합니다. `src.crawlers.game_detail_crawler --save`, `scripts/maintenance/init_data_collection.py`, `scripts/crawl_2009_game_details.py`, `scripts/maintenance/debug_*.py`, `scripts/maintenance/test_*crawl.py`, `scripts/maintenance/test_cancel_detect.py`, `scripts/maintenance/verify_2018_fix.py`, `scripts/maintenance/prototype_2000_crawler.py`는 운영 진입점이 아니라 bootstrap 또는 parser/레거시 조사용 예외입니다.
- **JSON manifest 예외:** `scripts/maintenance/collect_historical_game_ids.py`와 `scripts/maintenance/crawl_historical_schedule.py`는 schedule crawler를 직접 호출하지만 DB에 저장하지 않고 로컬 JSON manifest만 생성합니다. DB 일정 저장은 `src.cli.crawl_schedule`을 사용합니다.
- **국제대회 일정 예외:** `scripts/crawling/collect_international_games.py`는 정규시즌 schedule 페이지가 아닌 국제대회 전용 페이지를 읽습니다. 저장 시에는 직접 ORM upsert가 아니라 공통 `save_game_snapshot` 경로를 사용합니다.
- **중복 game_id 정리:** `smart_deduplicate.py`, `deduplicate_games.py`, `hard_deduplicate.py`, `absolute_completeness.py`, `fix_2026_only.py`는 모두 공통 `game_deduplication_service`를 통해 슬롯별 primary game을 선정합니다.

## 2. 퓨처스리그 (선수 프로필 기반 수집) 시 고려사항

### 2.1. 데이터 공백 및 누락 케이스
- **이슈:** 선수 프로필 페이지는 KBO 관리자가 직접 편집하는 경우가 많아, 데이터가 누락되거나 비어있을 수 있습니다.
  - **신인 선수:** 시즌 초반에는 퓨처스리그 기록 테이블 자체가 존재하지 않을 수 있습니다.
  - **기록 없는 선수:** 경기에 출전하지 않은 선수는 기록 테이블이 비어있습니다.
  - **비정형 데이터:** 간혹 특정 컬럼에 `-.---` 와 같은 비정형 문자열이 포함될 수 있습니다.
- **폴백(Fallback) 규칙:**
  - **테이블 부재:** 퓨처스리그 기록 테이블 셀렉터가 존재하지 않을 경우, 해당 선수는 기록이 없는 것으로 간주하고 오류 없이 넘어갑니다. (0으로 기록)
  - **타입 변환 실패:** 숫자여야 할 필드에서 `-.---` 같은 문자열이 발견되면, `0` 또는 `NULL`로 안전하게 변환하여 저장합니다.
  - **로깅:** 데이터 누락이나 비정형 데이터 발견 시, `WARNING` 레벨의 로그를 남겨 수동으로 검토할 수 있도록 합니다.

### 2.2. 업데이트 지연
- **이슈:** 프로필 페이지의 데이터는 실시간으로 업데이트되지 않으며, 관리자의 수동 작업에 의존할 수 있습니다.
- **정책:** 퓨처스리그 데이터는 "최종본"이 아니며, 약간의 지연이 있을 수 있음을 인지해야 합니다. 주 1회 동기화 시점의 스냅샷으로 간주합니다.

## 3. 공통 제한사항

### 3.1. 세이버메트릭스 미제공
- KBO 공식 웹사이트는 **WAR, wOBA, wRC+, FIP** 등과 같은 고급 세이버메트릭스 지표를 직접 제공하지 않습니다.
- **해결책:** 이 프로젝트는 수집한 원천 데이터를 바탕으로 `ISO`, `BABIP`, `FIP`, `ERA+` 등의 지표를 자체적으로 **계산**하여 `SEASON_STATS` 테이블에 저장합니다. 더 복잡한 WAR 등은 외부 데이터를 활용하거나 추가적인 계산 로직이 필요합니다.

### 3.2. 과거 데이터의 신뢰성
- 2000년대 초반 이전의 데이터는 일부 누락되거나, 현재의 기록 방식과 차이가 있을 수 있습니다.
- 오래된 데이터일수록 교차 검증이 필요하며, 완전성을 보장하기 어렵습니다.
