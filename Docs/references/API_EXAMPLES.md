# KBO 데이터베이스 활용 예제 (v2)

데이터베이스에 저장된 KBO 데이터를 v2 스키마에 맞춰 활용하는 예제입니다.

## 1. Python으로 데이터 조회

### 1.1. 특정 경기의 상세 기록 조회 (경기 단위 Raw 데이터)

```python
import sqlite3

def get_game_details(game_id: str):
    conn = sqlite3.connect('data/kbo_2025.db')
    cursor = conn.cursor()

    query = """
    SELECT
        s.player_name,
        s.team_id,
        s.batting_order,
        s.position,
        s.at_bats,
        s.hits,
        s.runs,
        s.rbis,
        s.home_runs,
        s.strikeouts
    FROM batter_game_stats s
    WHERE s.game_id = ?
    ORDER BY s.team_id, s.batting_order;
    """

    cursor.execute(query, (game_id,))
    game_data = cursor.fetchall()
    conn.close()
    return game_data

# 2025년 10월 13일 SK vs SS 경기 데이터 조회
game_id = '20251013SKSS0'
details = get_game_details(game_id)

print(f"--- {game_id} 경기 타자 기록 ---")
for player in details:
    print(f"{player[1]} {player[0]}({player[3]}): {player[4]}타수 {player[5]}안타 {player[7]}타점")
```

### 1.2. 시즌 누적 기록 조회 (View 활용)

시즌 누적 기록은 `SEASON_STATS` 테이블에서 직접 조회합니다.

```python
import pandas as pd
import sqlite3

def get_season_leaders(year: int, league: str = 'KBO'):
    conn = sqlite3.connect('data/kbo_2025.db')

    query = f"""
    SELECT
        player_name,
        team_id,
        batting_avg,
        home_runs,
        rbis,
        ops
    FROM SEASON_STATS
    WHERE year = ? AND league = ? AND at_bats >= 100
    ORDER BY ops DESC
    LIMIT 10;
    """

    df = pd.read_sql_query(query, conn, params=(year, league))
    conn.close()
    return df

# 2025년 정규시즌 OPS 순위
leaders = get_season_leaders(2025, 'KBO')
print("--- 2025 정규시즌 OPS TOP 10 ---")
print(leaders)
```

### 1.3. 퓨처스리그 기록 필터링 조회

`league` 컬럼을 필터링하여 퓨처스리그 기록만 손쉽게 조회할 수 있습니다.

```python
# 2025년 퓨처스리그 다승 순위 futures_pitching_leaders = get_season_pitching_leaders(2025, 'FUTURES')
print("\n--- 2025 퓨처스리그 다승 TOP 5 ---")
print(futures_pitching_leaders)

# (get_season_pitching_leaders 함수는 get_season_leaders와 유사하게 구현)
```

## 2. SQL 쿼리 예제 (v2)

### 2.1. 특정 선수의 경기별 타격 기록 추이

```sql
SELECT
    g.game_date,
    s.at_bats,
    s.hits,
    s.home_runs
FROM batter_game_stats s
JOIN games g ON s.game_id = g.game_id
WHERE s.player_id = (SELECT player_id FROM kbo_player_profiles WHERE player_name = '이정후' LIMIT 1)
  AND g.season_year = 2025
ORDER BY g.game_date;
```

### 2.2. 정규시즌과 퓨처스리그 기록 동시 비교

```sql
SELECT
    player_name,
    year,
    league,
    games,
    batting_avg,
    home_runs,
    ops
FROM SEASON_STATS
WHERE player_id = (SELECT player_id FROM kbo_player_profiles WHERE player_name = '문동주' LIMIT 1)
  AND year = 2025
ORDER BY league;
```

### 2.3. 퓨처스리그 유망주 찾기 (OPS 기준)

```sql
SELECT
    player_name,
    team_id,
    ops,
    games,
    at_bats
FROM SEASON_STATS
WHERE year = 2025
  AND league = 'FUTURES'
  AND at_bats >= 50
ORDER BY ops DESC
LIMIT 20;
```

## 3. FastAPI 서버 예제 (v2)

`league` 파라미터를 추가하여 API를 확장할 수 있습니다.

```python
# api_server.py
from fastapi import FastAPI, Query
from typing import List, Optional

app = FastAPI(title="KBO Data API v2")

# ... (DB 연결 설정) ...

class SeasonStat(BaseModel):
    player_name: str
    team_id: str
    batting_avg: float
    ops: float

@app.get("/stats/batting/top10", response_model=List[SeasonStat])
def get_top_batters(
    year: int = 2025,
    league: str = Query('KBO', enum=['KBO', 'FUTURES'])
):
    """시즌별, 리그별 타격 OPS TOP 10을 반환합니다."""
    # ... (get_season_leaders 함수 로직) ...
    return top_10_batters

@app.get("/stats/game/{game_id}")
def get_game_stats(game_id: str):
    """특정 경기의 상세 기록을 반환합니다."""
    # ... (get_game_details 함수 로직) ...
    return game_stats
```