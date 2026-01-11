# 💎 Team Data Quality Improvement & Schema Optimization Plan

## 1. Objective (목표)
KBO 공식 웹사이트의 **구단 소개** 및 **구단 변천사** 데이터를 크롤링하여 팀 정보의 정확성을 극대화하고, 이를 효율적으로 관리하기 위해 데이터베이스 스키마를 최적화합니다.

## 2. Optimized Schema Design (스키마 설계)
기존의 파편화된 테이블(5개)을 **3개**의 정규화된 테이블로 통합하고, PostgreSQL의 최신 기능을 활용합니다.

### 2.1 team_franchises (프랜차이즈, 불변)
*   **Role**: 구단의 정체성 및 최신 관리 정보 (크롤링 데이터 포함)
*   **Columns**:
    *   `id`: `Integer` (PK)
    *   `name`: `String` (현재 구단명)
    *   `code`: `String` (대표 코드)
    *   `metadata`: `JSONB` **(New)**
        *   구단주(Owner), CEO, 홈페이지(Website), 창단일(Found Date) 등 `TeamInfo.aspx` 상세 정보를 JSON으로 저장
    *   `web_url`: `String` (KBO 소개 페이지 URL)

### 2.2 teams (팀 코드, 관계)
*   **Role**: 통계 테이블과의 연결점 (Foreign Key), 별칭 관리
*   **Columns**:
    *   `team_id`: `String` (PK, 예: 'SS', 'OB')
    *   `franchise_id`: `Integer` (FK)
    *   `is_active`: `Boolean`
    *   `aliases`: `Text[]` **(New)**
        *   기존 `team_name_mapping` 테이블을 대체. 검색 효율성 증대.

### 2.3 team_history (변천사, 시계열)
*   **Role**: 연도별 구단 명칭, 로고, 연고지 변경 이력
*   **Columns**:
    *   `id`: `Integer` (PK)
    *   `franchise_id`: `Integer` (FK)
    *   `season`: `Integer` (해당 연도)
    *   `team_name`: `String` (당시 구단명)
    *   `team_code`: `String` (당시 코드)
    *   `logo_url`: `String` **(New)**
    *   `ranking`: `Integer` (당시 순위)

---

## 3. Crawling Strategy (데이터 수집 전략)

### 3.1 Team Info Crawler (구단 소개)
*   **Target**: `https://www.koreabaseball.com/Kbo/League/TeamInfo.aspx`
*   **Action**:
    1.  팀 목록 순회 및 팝업(Modal) 오픈
    2.  **Fields**: 구단주, 단장, 감독, 홈페이지, 주소, 전화번호
    3.  **Storage**: `team_franchises.metadata` (Upsert logic)

### 3.2 Team History Crawler (구단 변천사)
*   **Target**: `https://www.koreabaseball.com/Kbo/League/TeamHistory.aspx`
*   **Action**:
    1.  1982년 ~ 현재까지 연도별 Grid 파싱
    2.  **Fields**: 연도, 팀명, CI(로고) 이미지 URL, 순위
    3.  **Storage**: `team_history` 테이블에 연도별 스냅샷 저장

---

## 4. Execution Steps (실행 단계)

1.  **Schema Migration**:
    *   SQLite 로컬 DB에 JSONB/Array 컬럼 추가 (SQLite는 JSON 지원, Array는 JSON으로 대체 가능하거나 별도 처리)
    *   Legacy 테이블(`team_profiles`, `team_name_mapping`) 백업 및 데이터 이관
2.  **Develop Crawlers**:
    *   `src/crawlers/team_info_crawler.py`
    *   `src/crawlers/team_history_crawler.py`
3.  **Verify & Sync**:
    *   수집된 데이터 검증 (특히 역사적 팀명과 team_code 매핑 정확성)
    *   Supabase 동기화 (스키마 변경 사항 반영)

## 5. Timeline (예상 일정)
*   **Day 1**: 스키마 변경 및 크롤러 구현
*   **Day 2**: 데이터 수집, 정제 및 검증
