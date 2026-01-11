# KBO 공식 웹사이트 URL 레퍼런스 (v2)

이 문서는 KBO 데이터 크롤링에 사용되는 주요 URL, 쿼리스트링 파라미터, CSS 셀렉터를 정리합니다.

## 1. Track A: 정규시즌 (경기 단위) URL

### 1.1. 경기 일정
- **URL:** `https://www.koreabaseball.com/Schedule/Schedule.aspx`
- **주요 파라미터:**
  - `year={YYYY}`: 시즌 연도
  - `month={MM}`: 월 (1~12)
  - `seriesId=0`: 정규시즌 (기본값)
- **핵심 셀렉터:**
  - 경기 링크: `a[href*="gameId"]` (이 링크에서 `gameId` 추출)

### 1.2. 게임 센터 (경기별 데이터 종합)
- **URL:** `https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx`
- **주요 파라미터:**
  - `gameId={YYYYMMDD_AWAY_HOME_0}`: 경기 고유 ID
  - `gameDate={YYYYMMDD}`: 경기 날짜
  - `section`: `REVIEW`(박스스코어), `RELAY`(문자중계/PBP), `HIGHLIGHT` 등

### 1.3. 라인업 분석 (XHR)
- **엔드포인트:** `https://www.koreabaseball.com/ws/Schedule.asmx/GetLineUpAnalysis`
- **요청 방식:** `POST`
- **주요 Payload:** `{"gameId": "{game_id}"}`
- **데이터 범위:** 정규시즌, 포스트시즌 경기에서만 제공. 시범경기나 일부 이벤트 경기는 데이터가 없을 수 있음.

### 1.4. 박스스코어
- **URL:** 게임 센터 URL에서 `section=REVIEW` 사용
- **핵심 셀렉터:**
  - 원정팀 타자: `.tblAwayHitter1`, `.tblAwayHitter2`
  - 홈팀 타자: `.tblHomeHitter1`, `.tblHomeHitter2`
  - 원정팀 투수: `div.away-pitcher-record table`
  - 홈팀 투수: `div.home-pitcher-record table`
- **데이터 한계:** 서스펜디드 게임의 경우, 최종 기록이 합산되지 않고 분리되어 있을 수 있음.

### 1.5. Play-by-Play (PBP)
- **URL:** 게임 센터 URL에서 `section=RELAY` 사용
- **핵심 셀렉터:**
  - 이닝별 PBP 데이터: `div.relay-bx`
  - 타구 결과: `.txt-box`
- **데이터 한계:** 텍스트 기반이라 파싱이 복잡하고, 비정형 데이터가 많아 예외 처리가 중요.

---

## 2. Track B: 퓨처스리그 (프로필 기반) URL

### 2.1. 선수 목록
- **URL:** `https://www.koreabaseball.com/Player/Search.aspx`
- **주요 파라미터:**
  - `searchWord=%25`: 모든 선수 검색
- **핵심 셀렉터:**
  - 선수 목록 테이블: `table.tEx`
  - 선수 링크: `a[href*="playerId"]` (이 링크에서 `playerId` 추출)

### 2.2. 선수 프로필 페이지
- **URL:** `https://www.koreabaseball.com/Record/Player/HitterDetail/Basic.aspx?playerId={playerId}`
- **설명:** 타자/투수 구분 없이 위 URL 하나로 접근 가능. 페이지 내에서 탭으로 구분됨.

### 2.3. 퓨처스리그 기록 탭
- **탭 셀렉터:** `//a[contains(text(), '퓨처스')]` (XPath)
- **동작:** 이 탭을 클릭하면 해당 선수의 퓨처스리그 시즌별 누적 기록 테이블이 로드됩니다.
- **핵심 셀렉터:**
  - 퓨처스 기록 테이블: `div#cphContents_cphContents_cphContents_udpPlayerFutures > table.tbl.tt`
  - 연도별 기록 행: `tbody > tr`
- **데이터 한계:**
  - 경기별 상세 기록은 제공되지 않음 (시즌 누적 스탯만 존재).
  - 신인 선수나 출전 기록이 없는 선수는 테이블 자체가 없을 수 있음.

---

## 3. 데이터 범위 및 한계

| URL 경로 | 정규시즌 | 포스트시즌 | 시범경기 | 퓨처스리그 | 비고 |
| :--- | :---: | :---: | :---: | :---: | :--- |
| `Record/Player/*` | ✅ | ✅ | ✅ | ✅ | 선수 프로필 내 탭으로 구분 |
| `Schedule/Schedule.aspx` | ✅ | ✅ | ✅ | ✅ | `seriesId` 파라미터로 구분 |
| `Schedule/GameCenter/*` | ✅ | ✅ | ⚠️ | ❌ | 게임센터는 퓨처스리그를 지원하지 않음 |
| `Player/Search.aspx` | ✅ | ✅ | ✅ | ✅ | 모든 등록 선수를 포함 |

**⚠️ 경고:** KBO 웹사이트는 사전 공지 없이 URL 구조나 CSS 셀렉터를 변경할 수 있습니다. 크롤러 실패 시, 이 문서를 기준으로 변경 사항을 우선 확인해야 합니다.
