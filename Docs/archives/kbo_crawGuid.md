# KBO 데이터 수집 파이프라인 가이드 (v2)

## 1. 핵심 아키텍처: 2-트랙 데이터 수집

본 프로젝트는 데이터 소스의 특성에 맞춰 **정규시즌**과 **퓨처스리그**의 데이터 수집 경로를 분리하는 **2-트랙(Two-Track) 파이프라인**을 채택합니다.

수집 방법 및 DB스키마 설계는
Docs\schema\KBOseasonGamePipeLine.md 참조

---


## 2. 데이터 처리 순서 원칙

모든 데이터 수집은 아래 4단계의 순서를 엄격히 따릅니다.

1.  **수집 (Collect):** Playwright를 사용하여 웹페이지의 HTML 또는 API 응답(JSON)을 가져옵니다.
2.  **파싱 (Parse):** 수집된 원본 데이터에서 필요한 정보를 선택자(Selector)나 정규식을 사용하여 추출하고, 의미 있는 데이터 구조(딕셔너리, 객체)로 변환합니다.
3.  **검증 (Validate):** 파싱된 데이터가 예상된 타입, 범위, 형식인지 확인합니다. (예: 타율은 0과 1 사이)
4.  **저장 (Save):** 검증을 통과한 데이터를 SQLite 또는 MySQL 데이터베이스에 저장합니다. 중복 데이터는 `UPSERT` 로직을 통해 멱등성을 보장합니다.

---

## 3. Track A: 정규시즌 (경기 단위) 파이프라인

### 3.1. 수집 대상
- **경기 일정:** `Schedule/Schedule.aspx`
- **경기 메타:** `Schedule/GameCenter/Main.aspx`
- **라인업 분석:** `Schedule/GameCenter/Main.aspx` (XHR: `GetLineUpAnalysis`)
- **박스스코어:** `Schedule/GameCenter/Main.aspx` (섹션: `REVIEW`)
- **Play-by-Play:** `Schedule/GameCenter/Main.aspx` (섹션: `RELAY`)

*(상세 URL 및 파라미터는 `URL_REFERENCE.md` 참조)*

### 3.2. 핵심 셀렉터
- **경기 링크:** `a[href*="gameId="]`
- **박스스코어 테이블:** `.tblAwayHitter1`, `.tblHomeHitter1` 등
- **라인업 탭:** `//a[contains(text(), '라인업 분석')]`

### 3.3. 기술적 고려사항
- **페이지 레이턴시:** `page.wait_for_selector()`, `page.wait_for_load_state('networkidle')` 등을 사용하여 비동기적으로 로드되는 콘텐츠를 안정적으로 기다립니다.
- **재시도 및 오류 처리:**
  - `try...except` 블록으로 감싸 특정 선수나 경기에서 오류가 발생해도 전체 프로세스가 중단되지 않도록 합니다.
  - `max_retries` (예: 3회)와 `time.sleep()`을 조합하여 일시적인 네트워크 오류에 대응합니다.
- **데이터 식별:** 수집된 모든 데이터에는 `league='KBO'`, `source='game'` 과 같은 식별자를 명시하여 퓨처스리그 데이터와 명확히 구분합니다.

---

## 4. Track B: 퓨처스리그 (프로필 기반) 파이프라인

### 4.1. 수집 대상
- **선수 목록:** `Player/Search.aspx`
- **선수 프로필:** `Record/Player/HitterDetail/Basic.aspx?playerId={playerId}`

### 4.2. 핵심 셀렉터
- **퓨처스리그 탭:** `//a[contains(text(), '퓨처스')]`
- **연도별 통계 테이블:** 프로필 페이지 내 `table.tbl.tt`
- **테이블 파싱 규칙:**
  - KBO 웹사이트는 연도별로 미세하게 HTML 구조를 변경할 수 있습니다.
  - 따라서 특정 `class`나 `id`에 의존하기보다, `<thead>`의 컬럼명(예: 'AVG', 'G', 'PA')을 기준으로 동적으로 컬럼 인덱스를 매핑하는 방식이 더 안정적입니다.

### 4.3. 기술적 고려사항
- **주기:** 퓨처스리그 데이터는 변동성이 낮으므로, 주 1회 또는 월 1회 등 정규시즌보다 낮은 빈도로 수집을 권장합니다.
- **데이터 식별:** `league='FUTURES'`, `source='profile'` 식별자를 명시합니다.
- **키 체계:** `(player_id, season, team_id, game_type='FUTURES')` 와 같은 복합 키를 사용하여 데이터를 고유하게 식별합니다.
- **결측치 처리:** 프로필 페이지에 특정 연도 기록이 없거나, 특정 스탯이 누락된 경우(예: 신인 선수)를 대비하여 기본값(0 또는 NULL) 처리 로직이 필수적입니다.

---

## 5. 크롤링 정책 및 에티켓

- **Rate Limiting (속도 제어):** 모든 요청 사이에 최소 1~2초의 간격(`time.sleep(1)`)을 두어 KBO 서버에 과도한 부하를 주지 않습니다.
- **robots.txt 준수:** 크롤링 전 `robots.txt` 파일을 확인하여 차단된 경로에 접근하지 않습니다. (현재 KBO 사이트는 제한이 거의 없음)
- **User-Agent:** 스크립트의 목적을 명확히 알 수 있는 User-Agent 문자열을 사용합니다. (예: `PlaywrightKBO Crawler/1.0`)
- **실행 시간:** 트래픽이 적은 새벽 시간대(예: 02:00 ~ 05:00 KST)에 스케줄링하는 것을 원칙으로 합니다.
