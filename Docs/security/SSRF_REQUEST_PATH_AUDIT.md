# SSRF Request Path Audit (Stage 1-2)

status: SURVEY_ONLY - findings are documentation, not protection

## 1. Baseline

- 조사 기준 커밋: `d11b732b` (브라우저/HTTP 경로 조사 시작 시점)
- 조사 종료 시점: `a93c4ec9` — 조사 중 타 작업 커밋 6건 추가(`2033c363`~`a93c4ec9`). 브라우저 요청 경로와 무관(engine/playwright 패치 타입 정리, API 라우터 타입 정리, scheduler import 경로 정리).
- 검증 시점: `519fa633` — SSRF 테스트 3종은 이 커밋에서 실행. 위 커밋들 중 요청 경로 관련 diff 없음을 `git diff d11b732b..a93c4ec9 -- src/__init__.py src/scheduler/jobs/daily.py`로 확인(import 경로·어노테이션만 변경).
- 미커밋 변경(조사 종료 시점): `src/repositories/game_helpers.py`, `src/repositories/game_relay.py` — 본 조사 대상 아님, 본 문서의 근거 행번호는 `d11b732b` 기준.
- 미커밋 상태였다가 커밋된 변경: `src/crawlers/base.py`의 `goto_with_retry` 사전 검증 추가(이전 세션 작업)와 그 테스트 — 조사 시점 기준 커밋 상태로 존재.
- 런타임(Docker/launchd 실가동 프로세스, 실제 DNS)은 관측하지 않음. 환경별 통신은 커밋된 설정 파일의 "구성"으로만 기록.
- 이 문서는 방어 구현이 아니다. "차단된다"는 표현은 네트워크 계층 검증 후에만 사용한다.

## 2. 요청 경로 조사표 (입력 → 변환 → 검증 → 실행 → 실제 위치)

공통 검증기: `src/utils/url_validator.py:177` `validate_url()` — 스킴 제한(`:38` http/https), 옵션 도메인 allowlist(`:126`), 리터럴/해석 IP 비공개 차단(`:76`, `:141`, `:154`). 도메인 allowlist는 `validate_crawler_url`(`:223`)에만 기본 적용되고, **프로덕션 호출부는 없음**(테스트 전용).
프로덕션에서 `validate_url`을 호출하는 곳은 2곳뿐: `src/crawlers/base.py:151`(goto 전), `src/crawlers/base.py:190`(httpx request event hook).

### 2.1 브라우저 — context/연결 생성

| 경로 | 근거 | 검증 적용 | 실제 실행 위치(구성) |
|---|---|---|---|
| AsyncPlaywrightPool → local `chromium.launch` + `new_context` + 리소스 블로킹 설치 | `src/utils/playwright_pool.py:137,146,102-103` | goto 시에만(base.py:151) | launchd: 로컬 Chromium. Docker: browserless로 우회(§2.5) |
| 전역 launch 패치 → 원격 WS connect, 실패 시 로컬 launch fallback | `src/__init__.py:44,78,92-98,132,146-152` | WS 엔드포인트 검증 없음 | Docker: `ws://browserless:3000`(docker-compose.yml:41). fallback 활성 기본값(`src/__init__.py:59`) — browserless 장애 시 scheduler 컨테이너 로컬 브라우저로 전환 |
| sync 백필 서비스: 로컬 launch + page.route(확장자 차단) | `src/services/historical_detail_backfill_service.py:255-265` | goto(:169) 검증 없음 | launchd/CI 호스트 로컬 Chromium |
| 인증 로그인(자격증명 fill + 클릭, goto :79) | `src/utils/kbo_auth.py:62,69,79-86` | goto URL 검증 없음 | 로컬 launch(:62) |

- `launch_persistent_context`/`connect_over_cdp`/`expect_popup`/`route_web_socket`/`frame.goto`: `src/`·`scripts/` 범위 검색에서 0건.
- 원격 브라우저에서 페이지가 열리면 **하위 리소스 요청은 browserless 컨테이너의 네트워크에서 나간다** — scheduler 컨테이너와 격리 단위가 다르며, 현재 어떤 egress 제한도 없음.

### 2.2 브라우저 — 탐색(nav)·요청 처리

| 경로 | 근거 | 검증 |
|---|---|---|
| 공통 wrapper `goto_with_retry` — 리다이렉트 최종 URL 재검증 없음 | `src/crawlers/base.py:132-155` | 1회 사전검증 only |
| 직접 `page.goto` 약 40곳 | 예: `text_relay_crawler.py:233,237`, `kbo_event_crawler.py:192`, `src/cli/sync/refresh_source_snapshots.py:142` | 대부분 없음 |
| 클릭/포스트백/폼 제출로 시작되는 탐색(목적지 URL이 페이지 DOM·JS 결정) | `player_search_crawler.py:466-481`, `daily_roster_crawler.py:120-133`, `fielding_stats_crawler.py:88` 등 | 없음 — 검증기로 볼 수 없는 영역 |
| context/page 라우트 | 리소스 차단 2종: `playwright_blocking.py:101-109,131-139`(abort/continue_ + 예외 흡수), `historical_detail_backfill_service.py:134-141`; 인증 스크립트: `run_live_smoke_gate.py:322`, `run_gate_r2_live_relay.py:873` | **기존 page.route는 context 라우트보다 우선**하므로, 향후 context 가드 도입 시 페이지 라우트가 가드를 우회할 수 있음(Playwright 매칭 규칙 — 구현 단계에서 검증 필요) |
| Playwright HTTP API | `preview_crawler.py:331` `page.request.post` — 리다이렉트 자동 추종, 검증 없음 | 없음 |

### 2.3 HTTP 클라이언트

| 경로 | 근거 | 검증 |
|---|---|---|
| BaseHttpCrawler.http_client — event hook으로 리다이렉트 포함 검증 | `src/crawlers/base.py:203-208` | 있음(단, 도메인 allowlist 미적용) |
| 기타 httpx 직접 생성 30여 곳 | 예: `ticket_crawler.py:189,259,294`, `seat_crawler.py:106`, `relay_crawler.py:725`, `refresh_source_snapshots.py:201` | 없음 |
| requests/urlopen | `src/scheduler/jobs/sentinel.py:36`, `src/utils/alerting.py:71,111,153` | 없음 |
| trust_env/프록시 환경변수/`proxy=` 인자 | `src/` 범위 검색 0건 | httpx 기본 `trust_env=True` → 환경의 HTTP(S)_PROXY/NO_PROXY 상속 가능성 있음(환경별 미확인) |

### 2.4 URL 입력원 (공격 표면 관점)

| 유입원 | 근거 | 위험 등급 |
|---|---|---|
| 코드 상수(KBO/네이버 공식) | 각 crawler 상단 상수 | 낮음 |
| DB `data_sources.base_url` 그대로 fetch | `src/cli/sync/refresh_source_snapshots.py:117,142` | 중 — DB 쓰기 권한이 있으면 임의 URL |
| CLI `--url` 인자 | `src/cli/collection/crawl_kbo_official_events.py:29,50` | 중 — 로컬 실행자 입력 |
| 페이지 DOM의 href·폼(팀 티켓 페이지 등) | `ticket_crawler.py:218-222` 수집 → `:266` fetch | 중 — 크롤링 대상 페이지가 오염되면 임의 URL |
| DB에 저장된 팀 티켓 URL(런타임 누적) | `TEAM_TICKET_INFO` 갱신 후 재사용 | 중 |
| 응답 리다이렉트(httpx) | base.py event hook으로 검증됨 | 낮음 |
| 브라우저 리다이렉트·하위 리소스 | goto 최종 URL 재검증 없음, 리소스는 브라우저 자동 로드 | 높음 — 코드 레벨 검사 불가 영역 |

## 3. 실행 환경·통신 표 (커밋된 구성 기준, 관측 아님)

| 항목 | Docker 구성 | launchd 구성 | CI(GitHub Actions) |
|---|---|---|---|
| 프로세스 | kbo_scheduler + kbo_api_server + browserless | scripts/scheduler.py 단일 PID(plist:9-14) | 워크플로우별 잡 |
| 브라우저 위치 | **별도 browserless 컨테이너**(`ws://browserless:3000`, docker-compose.yml:41) — 네트워크 격리 단위가 scheduler와 다름 | 호스트 로컬 Chromium | runner 로컬 Chromium(`playwright: true`) |
| 로컬 fallback | browserless 연결 실패 시 로컬 launch(`src/__init__.py:92-98`) → **격리 단위 전환 발생** | 해당 없음(로컬 사용) | 로컬 사용 |
| DB | Oracle(Autonomous) | Oracle | Oracle |
| browserless 제어 포트 | 3000이 호스트에도 노출(docker-compose.yml:97-98) | 해당 없음 | 해당 없음 |
| 프록시/DNS | 커밋된 설정에 proxy·DNS 커스터마이징 없음 → 호스트/데몬 설정 상속, 실제 값 UNKNOWN | 동일 | runner 환경, UNKNOWN |
| 도커 네트워크 | default bridge(compose 정의 네트워크 단일) — 서비스 간 상호 도달 가능; text-relay는 별도 네트워크(docker-compose.text-relay.yml:67-68) | 해당 없음 | 해당 없음 |

**공유 권한 문제(§2의 핵심 제약)**: scheduler 컨테이너는 Oracle·browserless 제어·외부 HTTP 수집을 모두 동일 네트워크 권한으로 수행한다. IP/포트 기반 egress 규칙만으로는 "수집용 호출"과 "제어용 호출"을 구분할 수 없으므로, HTTP 수집 경로가 내부 목적지(Oracle SNAT 주소, browserless:3000)에 도달 가능하다는 사실은 정책 문서에서 감추지 않는다.
**컨테이너 내부 loopback**: Docker 방화벽(FORWARD 체인)은 컨테이너 안 loopback 트래픽을 보지 못한다. 브라우저에서 `127.0.0.1` 목적지 차단은 라우트 가드 등 브라우저 내부 메커니즘이 담당해야 하며, 호스트 방화벽으로 커버 주장 불가.

## 4. 요청 정책 계약 (확정안)

두 판정은 독립 계약이다. (1) 요청 대상 허용 — 이 요청 유형에서 scheme·host·port가 허용되는가. (2) 연결 목적지 허용 — 실제 연결 IP가 금지 대역이 아닌가. 도메인 허용이 IP 금지를 대체하지 않고, 그 역도 성립하지 않는다.

| 정책 항목 | 계약 |
|---|---|
| 스킴 | http/https만. 나머지 전부 거부 |
| 도메인 | 크롤러 대상은 명시 allowlist만. 문서 탐색/하위 리소스/HTTP 수집/인프라 제어는 **별도 허용 프로필**로 관리하며 리다이렉트가 더 넓은 프로필로 확장되지 않음 |
| IP 판정 | `100.64.0.0/10` 등 `is_private=False`·`is_global=False` 중간대를 명시적 금지 목록으로 처리(`url_validator.py`는 이미 별도 목록으로 커버 — 유지). IPv4-mapped IPv6 재검사 유지(`:91-93`). 경계값 테스트 후속 배치 |
| DNS | 검증 완료 못하면 허용하지 않음. resolve 실패·빈 응답은 차단(현재 구현 유지). A/AAAA 중 일부만 실패한 경우는 후속 배치에서 결과 분류 정의 전까지는 "모든 반환 주소 검사 후 하나라도 금지면 차단" 유지. 사전 검사만으로 연결 시점 보호 완료로 간주하지 않음 |
| 리다이렉트 | httpx: event hook 유지(현재 검증됨). Playwright 탐색: 리다이렉트·하위 리소스는 코드 레벨 미보호로 명시 — egress 담당 |
| 인프라 예외 | Oracle·browserless·알림(telegram/slack)·외부 통계 소스(openrouter 등)는 목적지 목록에 명시. 브라우저(웹 콘텐츠 프로세스)에 DB·browserless 제어 포트 접근 권한 부여하지 않음 |
| 로그 | 차단 기록은 host+사유만. URL 전체(쿼리·토큰) 원문 금지 — 기존 검증기 관례 유지(`tests/test_url_validator.py:167-172`) |

## 5. 구현 방식 검토 (egress 옵션과 남는 공백)

| 옵션 | 보호 범위 | 남는 공백 | 판단 |
|---|---|---|---|
| 커스텀 httpx transport(주소 고정) | httpx 경로만, DNS rebinding 차단 | 브라우저 전체 미보호 | 후속 배치 후보 |
| Playwright 라우트 가드 | 페이지/context에서 시작되는 요청 필터 | page.route 우선 규칙 우회 가능, 서비스 워커·연결 시점 미보장 | 보조 수단. 기존 `continue_()` 핸들러와의 규칙 정의가 선행 |
| browserless에 강제 프록시 | 브라우저 전 트래픽을 단일 지점으로 | Chromium 암묵적 localhost 우회 규칙 존재 — 프록시 지정만으로 직접 연결 차단 선언 불가, 별도 검증 필요 | egress와 병행 시 유효 |
| Docker 네트워크/방화벽 egress | browserless·scheduler 컨테이너에서 금지 대역 원천 차단 | 컨테이너 내부 loopback·같은 호스트 bind 포트는 FORWARD 체인 밖. scheduler는 Oracle/browserless 예외 필요 | 정답 후보. 운영 승인 후 적용 |
| launchd(호스트) 프로세스 | macOS 방화벽/격리 수단 제한 | 사실상 코드 레벨만 가능 | 리스크 수용 or Docker로 이관 검토 |

## 6. 미확인 사항 (UNKNOWN)

- 런타임 관측 없음: 실제 scheduler 프로세스, browserless 컨테이너의 실제 통신, 호스트 DNS 설정.
- Chromium 암묵적 프록시 우회·서비스 워커·`route.continue_()` 이후 동작은 고정된 Playwright 1.60 + browserless 이미지에서 검증 필요(이번 배치 범위 밖).
- httpx `trust_env=True` 환경변수 상속 여부 — 각 실행 환경에서 `env | grep -i proxy` 확인 후 기록.
- 도메인 allowlist 후보 중 CDN 의존성(스포츠 정적 리소스 등)은 관측 근거 없이 확정하지 않음 — 후속 canary 관측 목록.
- `100.64.0.0/10` IPv6 대응(UGC/64: `fc00::/7` 등) 포함 정책 재확인 필요.

## 7. 후속 배치 착수 조건

1. 이 문서의 미확인 항목 해소(운영 canary는 별도 승인).
2. 정책 계약(§4)에 따른 가드 구현 범위 결정 — 기존 라우트 우선순위 규칙 포함.
3. egress 방식 선택 — §5 표의 판단 근거로 결정, 운영 방화벽/재시작은 별도 승인.
4. 코드 변경 배치에서 `pytest tests/test_url_validator.py tests/crawlers/test_base_crawler.py tests/utils/test_playwright_blocking.py` + `ruff check src/ tests/ scripts/` + `ruff format --check .` 실행.

## 8. 기준선 검증 결과 (조사 종료 시점)

- SSRF 관련 기존 테스트 3종: **108 passed** (`tests/test_url_validator.py` + `tests/crawlers/test_base_crawler.py` + `tests/utils/test_playwright_blocking.py`).
- `ruff check src/ tests/ scripts/`: 커밋 트리 기준 **0 errors**. 작업 트리 1 error(`C901 _replace_records`, `src/repositories/game_helpers.py:1020`)는 **타 세션 미커밋 WIP**에서 유래(Oracle ORA-12860 회피용 `session.commit()` 분기 추가)로 본 배치 대상 아님 — 미커밋 변경은 stash→검증→즉시 복원으로 원본 보존했고 내용을 수정하지 않았음.
- 커밋 없음: 본 배치는 문서 산출물만 생성(`Docs/security/SSRF_REQUEST_PATH_AUDIT.md`), 운영 DB·스케줄러·방화벽·브라우저 실행 없음.
