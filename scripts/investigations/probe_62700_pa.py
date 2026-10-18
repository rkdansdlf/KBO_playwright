"""Temporary diagnostic probe: duplicated player season rows and the PA mismatch."""

from __future__ import annotations

from sqlalchemy import create_engine, text

from src.db.engine import DATABASE_URL

engine = create_engine(DATABASE_URL, connect_args={"connect_timeout": 10})

with engine.connect() as conn:
    print("=== 62700 raw rows in player_season_batting (2026) ===")
    rows = conn.execute(
        text(
            "SELECT player_id, league, level, team_code, plate_appearances, source, updated_at "
            "FROM player_season_batting WHERE player_id = 62700 AND season = 2026 ORDER BY source"
        )
    ).all()
    for player_id, league, level, team, pa, source, updated in rows:
        print(f"  league={league:<8} level={str(level):<6} team={str(team):<5} PA={pa:<5} source={source:<15} updated={updated}")

    print()
    print("=== transaction-side PA from game rows ===")
    total, count = conn.execute(
        text("SELECT sum(plate_appearances), count(*) FROM game_batting_stats WHERE player_id = 62700")
    ).one()
    print(f"  sum(PA)={total}  rows={count}")

    print()
    print("=== how widespread is the duplication? ===")
    dupes = conn.execute(
        text(
            "SELECT count(*) FROM ("
            "  SELECT player_id, season, league FROM player_season_batting"
            "  GROUP BY player_id, season, league HAVING count(*) > 1"
            ") t"
        )
    ).scalar()
    print(f"  player/season/league groups holding more than one row: {dupes}")

    print()
    print("=== distinct source values in that duplication ===")
    sources = conn.execute(
        text(
            "SELECT source, count(*) FROM ("
            "  SELECT player_id, season, league, count(*) AS n, min(source) AS source"
            "  FROM player_season_batting GROUP BY player_id, season, league HAVING count(*) > 1"
            ") t GROUP BY source ORDER BY 2 DESC"
        )
    ).all()
    for source, n in sources:
        print(f"  {str(source):<15} {n}")
