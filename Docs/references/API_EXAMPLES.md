# KBO API 및 데이터 조회 예제

운영 데이터는 Oracle Autonomous Database에 저장됩니다. SQLite 파일명이나
과거 `SEASON_STATS`, `batter_game_stats` 테이블명은 현재 계약이 아닙니다.

## 1. SQLAlchemy로 경기 조회

```python
from src.db.engine import get_db_session
from src.models.game import Game

with get_db_session() as session:
    games = (
        session.query(Game)
        .filter(Game.season_id == 2025)
        .order_by(Game.game_date.desc())
        .limit(10)
        .all()
    )

for game in games:
    print(game.game_id, game.game_date, game.away_team, game.home_team)
```

## 2. 시즌 타격 리더 조회

```python
from src.db.engine import get_db_session
from src.models.player import PlayerSeasonBatting

with get_db_session() as session:
    leaders = (
        session.query(PlayerSeasonBatting)
        .filter(
            PlayerSeasonBatting.season == 2025,
            PlayerSeasonBatting.league == "REGULAR",
            PlayerSeasonBatting.at_bats >= 100,
        )
        .order_by(PlayerSeasonBatting.ops.desc())
        .limit(10)
        .all()
    )
```

## 3. Oracle 직접 조회

```sql
SELECT game_id, game_date, away_team, home_team, home_score, away_score
FROM game
WHERE season_id = 2025
ORDER BY game_date DESC
FETCH FIRST 20 ROWS ONLY;
```

초기 schema 생성과 migration은 다음 명령을 사용합니다.

```bash
python3 -m src.cli.apply_oracle_migrations
python3 -m src.cli.apply_oracle_migrations --check
```

## 4. FastAPI 경기 조회

```bash
curl -H "X-API-Key: $API_KEY" \
  "http://localhost:8000/api/games?season=2025&limit=20"
```

지원 endpoint는 `/docs`에서 현재 OpenAPI schema를 확인합니다.

## 5. RAG hybrid 검색

Oracle 단일 저장소를 사용합니다. sparse(BM25 후보)는 `RAG_CHUNK_TERMS` postings
인덱스(기본 `RAG_ORACLE_SPARSE_MODE=terms`), dense는 같은 테이블의 네이티브
`VECTOR` 컬럼과 HNSW 인덱스로 처리합니다.

```bash
curl -X POST "http://localhost:8000/api/v1/rag/hybrid-search" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{
    "query": "2025년 LG 투수 성적",
    "top_k": 5,
    "filters": {"season_year": 2025, "team_id": "LG"}
  }'
```

RAG index build 명령:

```bash
python3 -m src.cli.index_rag_knowledge
python3 -m src.cli.build_rag_index --source all --dry-run
python3 -m src.cli.build_rag_index --source all

# sparse postings 색인/감사 (전체 옵션은 COMMAND_REFERENCE.md §RAG 인덱스 관리)
python3 -m src.cli.rag.build_oracle_sparse_index --apply --catch-up --batch-size 40
python3 -m src.cli.audit_rag_index --require-nonempty --require-postings --json
```
