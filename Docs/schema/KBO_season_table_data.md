CSV 파일에 정의된 대로 `season_meta_id`가 PK(기본 키) 역할을 하므로, 1982년부터 2030년까지 모든 연도의 모든 시즌 종류(정규시즌, 포스트시즌 등)에 대해 고유한 `season_meta_id`를 생성하여 데이터를 채워 넣습니다.

-----

## 📝 시즌 메타 테이블 설계 (DDL)

CSV 파일의 제약 조건을 반영한 `kbo_seasons_meta` 테이블의 DDL(Data Definition Language)입니다.

| 필드명 | 설명 | 데이터 타입 | 제약 조건 | 비고 |
| :--- | :--- | :--- | :--- | :--- |
| `season_meta_id` | PK, 기본 키 | `INT` | `PRIMARY KEY` | 선수 기록 테이블의 FK로 사용됨 |
| `season_year` | 시즌 연도 | `INT` | `NOT NULL` | |
| `league_type_code` | 시즌 종류 코드 | `INT` | `NOT NULL` | 0: 정규시즌, 5: 한국시리즈 등 |
| `league_type_name` | 시즌 종류 이름 | `VARCHAR(50)` | `NOT NULL` | |
| `start_date` | 시즌 시작일 | `DATE` | `NULLABLE` | |
| `end_date` | 시즌 종료일 | `DATE` | `NULLABLE` | |

```sql
-- DDL (Data Definition Language)
CREATE TABLE kbo_seasons_meta (
    season_meta_id INT PRIMARY KEY,
    season_year INT NOT NULL,
    league_type_code INT NOT NULL,
    league_type_name VARCHAR(50) NOT NULL,
    start_date DATE,
    end_date DATE
);
```

-----

## 💾 시즌 메타 초기 데이터 삽입 (DML)

1982년부터 2030년까지의 데이터를 삽입하기 위해, 프로그래밍 언어나 스크립트를 사용하는 것이 일반적이지만, 여기서는 SQL로 직접 삽입할 수 있도록 데이터 구조를 보여드립니다.

각 연도(1982년 \~ 2030년)마다 CSV에 정의된 6가지 시즌 종류(코드 0\~5)가 반복되어 삽입됩니다.

### 📌 데이터 생성 로직

1.  **반복 연도:** 1982년부터 2030년까지 (총 49개 연도).
2.  **반복 시즌 종류 (6가지):**
      * 0: 정규시즌
      * 1: 시범경기
      * 2: 와일드카드 (2015년 이후에만 발생하지만, 데이터 일관성을 위해 모든 연도에 데이터 항목 생성)
      * 3: 준플레이오프
      * 4: 플레이오프
      * 5: 한국시리즈
3.  **총 행 수:** $49 \text{ (연도)} \times 6 \text{ (시즌 종류)} = 294$ 행.

### 📊 SQL 삽입 구문 (DML)

모든 294개 행을 한 번에 보여드리기 어려우므로, **1982년, 2024년, 2030년** 데이터의 일부와 전체를 생성할 수 있는 \*\*반복 SQL 로직 (PostgreSQL 기준)\*\*을 예시로 보여드립니다.

#### A. SQL INSERT 예시 (일부 데이터)

```sql
-- 1982년 시즌 데이터 삽입
INSERT INTO kbo_seasons_meta (season_meta_id, season_year, league_type_code, league_type_name) VALUES
(1, 1982, 0, '정규시즌'),
(2, 1982, 1, '시범경기'),
(3, 1982, 2, '와일드카드'), -- 1982년에는 없었으나, 일관된 메타데이터 구조를 위해 삽입
(4, 1982, 3, '준플레이오프'),
(5, 1982, 4, '플레이오프'),
(6, 1982, 5, '한국시리즈');

-- 2024년 시즌 데이터 삽입 (실제 사용 예시)
INSERT INTO kbo_seasons_meta (season_meta_id, season_year, league_type_code, league_type_name) VALUES
(250, 2024, 0, '정규시즌'),
(251, 2024, 1, '시범경기'),
(252, 2024, 2, '와일드카드'),
(253, 2024, 3, '준플레이오프'),
(254, 2024, 4, '플레이오프'),
(255, 2024, 5, '한국시리즈');
```

#### B. SQL 반복 삽입 스크립트 (PostgreSQL 또는 T-SQL 등 반복문 지원 DB)

대부분의 실제 DB 환경에서는 스크립트나 프로시저를 사용하여 전체 데이터를 생성합니다. 다음은 **PostgreSQL의 `GENERATE_SERIES`** 함수를 사용하여 1982년부터 2030년까지의 294개 행을 자동으로 생성하는 방법입니다.

```sql
WITH seasons_map AS (
    -- 시즌 종류와 코드 정의
    SELECT * FROM (VALUES
        (0, '정규시즌'),
        (1, '시범경기'),
        (2, '와일드카드'),
        (3, '준플레이오프'),
        (4, '플레이오프'),
        (5, '한국시리즈')
    ) AS t(code, name)
)
INSERT INTO kbo_seasons_meta (season_meta_id, season_year, league_type_code, league_type_name)
SELECT
    -- season_meta_id는 1부터 순차적으로 부여
    (s.year - 1982) * 6 + sm.code + 1 AS season_meta_id,
    s.year AS season_year,
    sm.code AS league_type_code,
    sm.name AS league_type_name
FROM
    GENERATE_SERIES(1982, 2030) AS s(year) -- 1982년부터 2030년까지 연도 생성
CROSS JOIN
    seasons_map sm
ORDER BY
    season_year, league_type_code;
```

1982년\~2030년까지의 모든 연도별 시즌 메타 데이터를 일관된 구조로 테이블에 자동으로 삽입합니다. 실제 서비스에 통합하실 때 사용하시는 DB 종류에 맞게 이 로직을 적용하시면 됩니다.