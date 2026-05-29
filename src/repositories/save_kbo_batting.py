"""
Save KBO player season batting stats to database with UPSERT logic.
Compatible with SQLite, PostgreSQL, and MySQL.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from src.db.engine import Engine, SessionLocal
from src.models.player import PlayerSeasonBatting


def save_kbo_player_season_batting(player_data: dict[str, Any]) -> bool:
    """
    Save KBO player season batting stats to player_season_batting table.

    Args:
        player_data: Dictionary containing player batting statistics

    Returns:
        True if saved successfully, False otherwise
    """
    try:
        dialect = Engine.dialect.name

        with SessionLocal() as session:
            # 필수 필드 검증
            required_fields = ["player_id", "year", "league", "team_code"]
            for field in required_fields:
                if field not in player_data or player_data[field] is None:
                    print(f"   ⚠️ 필수 필드 누락: {field}")
                    return False

            # 데이터 매핑
            values = {
                "player_id": player_data["player_id"],
                "season": player_data["year"],
                "league": player_data.get("league", "KBO"),
                "level": player_data.get("level", "KBO1"),
                "source": player_data.get("source", "PROFILE"),
                "team_code": player_data["team_code"],
                # 기본 타격 스탯
                "games": player_data.get("games"),
                "plate_appearances": player_data.get("plate_appearances"),
                "at_bats": player_data.get("at_bats"),
                "runs": player_data.get("runs"),
                "hits": player_data.get("hits"),
                "doubles": player_data.get("doubles"),
                "triples": player_data.get("triples"),
                "home_runs": player_data.get("home_runs"),
                "rbi": player_data.get("rbis"),  # rbis -> rbi 매핑
                # 볼넷/삼진 스탯
                "walks": player_data.get("walks"),
                "intentional_walks": player_data.get("intentional_walks"),
                "hbp": player_data.get("hit_by_pitch"),
                "strikeouts": player_data.get("strikeouts"),
                # 기타 스탯
                "stolen_bases": player_data.get("stolen_bases"),
                "caught_stealing": player_data.get("caught_stealing"),
                "sacrifice_hits": player_data.get("sacrifice_bunts"),
                "sacrifice_flies": player_data.get("sacrifice_flies"),
                "gdp": player_data.get("gdp"),
                # 비율 스탯
                "avg": player_data.get("avg"),
                "obp": player_data.get("obp"),
                "slg": player_data.get("slg"),
                "ops": player_data.get("ops"),
                # 확장 스탯 (JSON)
                "extra_stats": player_data.get("extra_stats", {}),
            }

            # 데이터베이스별 UPSERT 실행
            if dialect == "sqlite":
                stmt = sqlite_insert(PlayerSeasonBatting).values(**values)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["player_id", "season", "league", "level"],
                    set_={k: stmt.excluded[k] for k in values if k not in ["player_id", "season", "league", "level"]},
                )
            elif dialect == "postgresql":
                stmt = postgresql_insert(PlayerSeasonBatting).values(**values)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["player_id", "season", "league", "level"],
                    set_={k: stmt.excluded[k] for k in values if k not in ["player_id", "season", "league", "level"]},
                )
            else:  # MySQL
                stmt = mysql_insert(PlayerSeasonBatting).values(**values)
                stmt = stmt.on_duplicate_key_update(
                    **{k: stmt.inserted[k] for k in values if k not in ["player_id", "season", "league", "level"]}
                )

            session.execute(stmt)
            session.commit()

            return True

    except Exception:
        logger.exception("   ❌ 데이터 저장 중 오류")
        return False


def save_kbo_batting_batch(players_data: dict[int, dict[str, Any]], series_name: str) -> int:
    """
    배치로 여러 선수 데이터 저장

    Args:
        players_data: 선수별 데이터 딕셔너리
        series_name: 시리즈명 (로깅용)

    Returns:
        저장된 레코드 수
    """
    saved_count = 0
    total_count = len(players_data)

    print(f"💾 {series_name} 데이터 저장 중... (총 {total_count}명)")

    for _player_id, player_data in players_data.items():
        try:
            if save_kbo_player_season_batting(player_data):
                saved_count += 1
                if saved_count % 50 == 0:  # 50명마다 진행상황 출력
                    print(f"   📊 진행상황: {saved_count}/{total_count}명 저장 완료")
        except Exception:
            logger.exception(f"   ⚠️ {player_data.get('player_name', 'Unknown')} 저장 실패")
            continue

    print(f"   ✅ {saved_count}/{total_count}명 저장 완료")
    return saved_count
