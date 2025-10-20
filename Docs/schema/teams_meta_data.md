## 📝 KBO 팀 정보 테이블 설계 (DDL)

CSV 파일(`teams (구단 정보).csv`)의 컬럼 정보를 반영하여 SQL 테이블을 정의했습니다.

**테이블명:** `teams`

| 컬럼 이름 | 데이터 타입 | 제약 조건 | 설명 |
| :--- | :--- | :--- | :--- |
| `team_id` | `VARCHAR(10)` | `PRIMARY KEY` | 구단 고유 ID (크롤링/DB 조인용) |
| `team_name` | `VARCHAR(50)` | `NOT NULL` | 구단 정식 명칭 |
| `team_short_name` | `VARCHAR(20)` | `NOT NULL` | 구단 약칭 |
| `city` | `VARCHAR(30)` | `NOT NULL` | 연고 도시 |
| `founded_year` | `INT` | `NULLABLE` | 창단 연도 |
| `stadium_name` | `VARCHAR(50)` | `NULLABLE` | 홈 구장 명칭 |

```sql
-- DDL (Data Definition Language)
CREATE TABLE teams (
    team_id VARCHAR(10) PRIMARY KEY,
    team_name VARCHAR(50) NOT NULL,
    team_short_name VARCHAR(20) NOT NULL,
    city VARCHAR(30) NOT NULL,
    founded_year INT,
    stadium_name VARCHAR(50)
);
```

-----

## KBO 팀 초기 데이터 (DML)

2025년 기준 KBO 10개 구단의 정보를 반영하여 데이터를 삽입하는 SQL 구문입니다. (일반적으로 사용되는 KBO 코드를 `team_id`로 지정했습니다.)

```sql
-- DML (Data Manipulation Language)
INSERT INTO teams (team_id, team_name, team_short_name, city, founded_year, stadium_name) VALUES
('SS', '삼성 라이온즈', '삼성', '대구', 1982, '대구 삼성 라이온즈 파크'),
('LG', 'LG 트윈스', 'LG', '서울', 1990, '서울종합운동장 잠실 야구장'),
('NC', 'NC 다이노스', 'NC', '창원', 2011, '창원NC파크'),
('DO', '두산 베어스', '두산', '서울', 1982, '서울종합운동장 잠실 야구장'),
('KT', 'kt wiz', 'kt', '수원', 2013, '수원 kt wiz 파크'),
('HT', 'KIA 타이거즈', 'KIA', '광주', 2001, '광주-기아 챔피언스 필드'),
('LT', '롯데 자이언츠', '롯데', '부산', 1982, '부산 사직 야구장'),
('HH', '한화 이글스', '한화', '대전', 1986, '대전 한화생명 이글스 파크'),
('WO', '키움 히어로즈', '키움', '서울', 2008, '고척스카이돔'),
('SK', 'SSG 랜더스', 'SSG', '인천', 2021, '인천SSG랜더스필드');
```