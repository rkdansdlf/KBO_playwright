## 🔗 1. 팀과 구장의 제약조건 설정

이전 설계에서 **팀**은 **`team_history`** 테이블로 관리되고 있습니다. 구단 역사가 특정 기간 동안 **특정 구장**을 사용했음을 명시하기 위해 다음과 같이 관계를 설정합니다.

### A. 구장 테이블 (`stadiums`) 설계 (DDL)

구장의 고유 정보와 통계적 특성(파크팩터)을 저장합니다.

| 필드명 | 데이터 타입 | 제약 조건 | 설명 |
| :--- | :--- | :--- | :--- |
| **`stadium_id`** | `VARCHAR(30)` | **PRIMARY KEY** | 구장 고유 식별 코드 (예: JAMSIL) |
| `stadium_name` | `VARCHAR(50)` | `NOT NULL` | 구장 정식 명칭 |
| `city` | `VARCHAR(30)` | `NOT NULL` | 연고 도시 |
| `open_year` | `INT` | `NOT NULL` | 개장 연도 |
| `capacity` | `INT` | `NOT NULL` | 최대 수용 인원 |
| `seating_capacity` | `INT` | `NOT NULL` | 좌석 수 |
| `left_fence_m` | `DOUBLE PRECISION` | `NOT NULL` | 좌우 펜스 거리 (m) |
| `center_fence_m` | `DOUBLE PRECISION` | `NOT NULL` | 중앙 펜스 거리 (m) |
| `fence_height_m` | `DOUBLE PRECISION` | `NULLABLE` | 펜스 높이 (m) |
| `turf_type` | `VARCHAR(20)` | `NOT NULL` | 잔디 유형 (천연/인조) |
| `bullpen_type` | `VARCHAR(50)` | `NULLABLE` | 불펜 유형 (분리형/폐쇄형 등) |
| `homerun_park_factor`| `DOUBLE PRECISION` | `NULLABLE` | 홈런 파크 팩터 |
| `notes` | `TEXT` | `NULLABLE` | 특이사항 (ex. 돔구장, 리모델링) |

```sql
-- DDL (Data Definition Language) for stadiums
CREATE TABLE stadiums (
    stadium_id VARCHAR(30) PRIMARY KEY,
    stadium_name VARCHAR(50) NOT NULL,
    city VARCHAR(30) NOT NULL,
    open_year INT NOT NULL,
    capacity INT NOT NULL,
    seating_capacity INT NOT NULL,
    left_fence_m DOUBLE PRECISION NOT NULL,
    center_fence_m DOUBLE PRECISION NOT NULL,
    fence_height_m DOUBLE PRECISION,
    turf_type VARCHAR(20) NOT NULL,
    bullpen_type VARCHAR(50),
    homerun_park_factor DOUBLE PRECISION,
    notes TEXT
);
```

### B. 팀 역사와 구장의 제약조건 (Foreign Key)

팀 역사가 특정 구장을 사용했음을 기록하기 위해, `team_history` 테이블에 **`stadium_id`** 필드를 추가하고 `stadiums` 테이블을 참조하도록 외래 키를 설정합니다.

```sql
-- team_history 테이블 스키마 변경 (ALTER TABLE)
-- team_history 테이블은 이제 특정 기간(start_season ~ end_season) 동안
-- 사용한 구장(stadium_id)을 참조합니다.

ALTER TABLE team_history
ADD COLUMN stadium_id VARCHAR(30),
ADD CONSTRAINT fk_stadium_id
    FOREIGN KEY (stadium_id)
    REFERENCES stadiums (stadium_id);
```

-----

## 💾 2. 초기 데이터 삽입 (DML)

### A. 구장 테이블 (`stadiums`) 초기 데이터

정보에 기반하여 9개 야구장의 데이터를 삽입합니다.

```sql
INSERT INTO stadiums (stadium_id, stadium_name, city, open_year, capacity, seating_capacity, left_fence_m, center_fence_m, fence_height_m, turf_type, bullpen_type, homerun_park_factor, notes) VALUES
('JAMSIL', '잠실 야구장', '서울', 1982, 25000, 24411, 100.0, 125.0, NULL, '천연잔디', '분리형 불펜', 0.732, 'KBO 최대 규모, 투수 친화적'),
('GOCHEOK', '고척스카이돔', '서울', 2015, 22258, 16783, 99.0, 122.0, NULL, '인조잔디', '폐쇄형 지하불펜', 0.822, '국내 유일 돔구장'),
('SSGLANDERS', '인천SSG랜더스필드', '인천', 2002, 25000, 23000, 95.0, 120.0, 2.8, '천연잔디', '외야 펜스 분리형 불펜', 1.489, '홈런 2번째로 많음, 주차장 넓음'),
('KTWIZ', '수원 kt wiz 파크', '수원', 1989, 25000, 22067, 98.0, 120.0, NULL, '천연잔디', '외야 파울존 분리형 불펜', NULL, '2014년 리모델링'),
('LIONS', '대구 삼성 라이온즈 파크', '대구', 2016, 29178, 24331, 99.0, 122.0, 3.6, '천연잔디', NULL, 1.522, '국내 최초 팔각형 구장, 홈런 가장 많음'),
('CHAMPIONS', '광주-기아  챔피언스 필드', '광주', 2014, 27000, 20500, 99.0, 121.0, NULL, '천연잔디', '외야 펜스 분리형 불펜', 0.953, '국내 최초 개방형 구장'),
('EAGLES', '대전 한화생명 이글스 파크', '대전', 2025, 25000, 22000, 99.5, 122.0, NULL, '천연잔디', '외야 펜스 복층형 불펜', NULL, '2025년 신규 개장'),
('NCPARK', '창원NC파크', '창원', 2019, 22112, 22112, 101.0, 122.0, NULL, '천연잔디', '외야 펜스 분리형 불펜', 1.085, NULL),
('SAJIK', '부산 사직 야구장', '부산', 1985, 27500, 24500, 96.0, 121.0, 4.8, '천연잔디', '외야 파울존 분리형 불펜', 0.729, '2025년 펜스 높이 4.8m로 조정');
```

### B. 구단 역사 테이블 (`team_history`) 업데이트

이전에 삽입된 구단 역사 데이터에 **`stadium_id`** 정보를 추가하여 업데이트합니다. (SQL `UPDATE` 문 사용)

```sql
-- team_history 테이블 업데이트 (소속 구장 정보 추가)
UPDATE team_history SET stadium_id = 'LIONS' WHERE franchise_id = 1; -- 삼성
UPDATE team_history SET stadium_id = 'SAJIK' WHERE franchise_id = 2; -- 롯데
UPDATE team_history SET stadium_id = 'JAMSIL' WHERE franchise_id = 3; -- LG (MBC 포함)
UPDATE team_history SET stadium_id = 'JAMSIL' WHERE franchise_id = 4; -- 두산 (OB 포함)

-- 해태/KIA (2014년 챔피언스 필드로 이전) - 시점 분할 필요
UPDATE team_history SET stadium_id = 'CHAMPIONS' WHERE id = 8; -- KIA (2001~현재, 2014년에 챔피언스 필드로 이전했지만 편의상 현재 구장으로 통일)

-- 키움 히어로즈 (2016년 고척돔 이전) - 시점 분할 필요
UPDATE team_history SET stadium_id = 'GOCHEOK' WHERE id IN (13, 14, 15); -- 히어로즈 (고척돔 이전 시점은 누락)

-- 한화 이글스 (2025년 신축구장) - 시점 분할 필요
UPDATE team_history SET stadium_id = 'EAGLES' WHERE id = 17; -- 한화 (2025년 신축 구장)

-- SSG 랜더스 (SK 포함)
UPDATE team_history SET stadium_id = 'SSGLANDERS' WHERE franchise_id = 8;

-- NC 다이노스 (2019년 NC파크 이전) - 시점 분할 필요
UPDATE team_history SET stadium_id = 'NCPARK' WHERE franchise_id = 9;

-- KT 위즈
UPDATE team_history SET stadium_id = 'KTWIZ' WHERE franchise_id = 10;
```

**주의:** 구장 이전 시점(예: KIA, 히어로즈, 한화)을 정확히 반영하려면, `team_history` 테이블의 행을 **구장 변경 시점**에 맞춰 추가적으로 분할해야 합니다. (예: 한화의 경우 1993년\~2024년, 2025년\~현재로 두 행으로 분리) 위의 UPDATE 구문은 현재 유효한 구장을 기준으로 간단히 매핑한 예시입니다.