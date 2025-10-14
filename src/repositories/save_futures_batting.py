"""
Save Futures batting stats to database with UPSERT logic.
Compatible with SQLite and MySQL.
"""
from typing import List, Dict
from sqlalchemy import create_engine
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.mysql import insert as mysql_insert

from src.db.engine import Engine, SessionLocal
from src.models.player import PlayerSeasonBatting


def save_futures_batting(
    player_id_db: int,
    rows: List[Dict],
    league: str = "FUTURES",
    level: str = "KBO2"
) -> int:
    """
    Save Futures batting stats to player_season_batting table.

    Args:
        player_id_db: Database player ID (integer)
        rows: List of season dicts from fetch_and_parse_futures_batting()
        league: League name (default: "FUTURES")
        level: Level name (default: "KBO2")

    Returns:
        Number of records saved
    """
    if not rows:
        return 0

    dialect = Engine.dialect.name
    saved = 0

    with SessionLocal() as session:
        for r in rows:
            season = r.get("season")
            if not season:
                continue  # Skip invalid rows

            values = {
                "player_id": player_id_db,
                "season": season,
                "league": league,
                "level": level,
                "games": r.get("G"),
                "at_bats": r.get("AB"),
                "runs": r.get("R"),
                "hits": r.get("H"),
                "doubles": r.get("2B"),
                "triples": r.get("3B"),
                "home_runs": r.get("HR"),
                "rbi": r.get("RBI"),
                "walks": r.get("BB"),
                "hbp": r.get("HBP"),
                "strikeouts": r.get("SO"),
                "stolen_bases": r.get("SB"),
                "avg": r.get("AVG"),
                "obp": r.get("OBP"),
                "slg": r.get("SLG"),
                "source": "PROFILE",
            }

            if dialect == "sqlite":
                stmt = sqlite_insert(PlayerSeasonBatting).values(**values)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["player_id", "season", "league", "level"],
                    set_={
                        k: stmt.excluded[k]
                        for k in [
                            "games", "at_bats", "runs", "hits", "doubles", "triples",
                            "home_runs", "rbi", "walks", "hbp", "strikeouts",
                            "stolen_bases", "avg", "obp", "slg", "source"
                        ]
                    }
                )
            else:  # MySQL
                stmt = mysql_insert(PlayerSeasonBatting).values(**values)
                stmt = stmt.on_duplicate_key_update(
                    **{
                        k: stmt.inserted[k]
                        for k in [
                            "games", "at_bats", "runs", "hits", "doubles", "triples",
                            "home_runs", "rbi", "walks", "hbp", "strikeouts",
                            "stolen_bases", "avg", "obp", "slg", "source"
                        ]
                    }
                )

            session.execute(stmt)
            saved += 1

        session.commit()

    return saved
