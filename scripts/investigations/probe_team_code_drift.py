"""Temporary diagnostic probe: duplicated player season rows and team-code drift."""

from __future__ import annotations

from sqlalchemy import create_engine, text

from src.db.engine import DATABASE_URL

engine = create_engine(DATABASE_URL, connect_args={"connect_timeout": 10})

with engine.connect() as conn:
    print("=== duplicated (player, season, league) groups: detail ===")
    rows = conn.execute(
        text(
            "SELECT player_id, season, league, count(*) AS n, "
            "       string_agg(source || ':' || COALESCE(team_code, 'NULL'), ', ' ORDER BY source) "
            "FROM player_season_batting "
            "GROUP BY player_id, season, league "
            "HAVING count(*) > 1 "
            "ORDER BY n DESC, player_id LIMIT 20"
        )
    ).all()
    for player_id, season, league, n, sources in rows:
        print(f"  pid={player_id:<7} season={season} league={league:<11} n={n}  {sources}")

    print()
    print("=== do the duplicated groups disagree on team_code? ===")
    drifted = conn.execute(
        text(
            "SELECT count(*) FROM ("
            "  SELECT player_id, season, league FROM player_season_batting "
            "  WHERE team_code IS NOT NULL "
            "  GROUP BY player_id, season, league "
            "  HAVING count(*) > 1 AND count(DISTINCT team_code) > 1"
            ") t"
        )
    ).scalar()
    same_team = conn.execute(
        text(
            "SELECT count(*) FROM ("
            "  SELECT player_id, season, league FROM player_season_batting "
            "  WHERE team_code IS NOT NULL "
            "  GROUP BY player_id, season, league "
            "  HAVING count(*) > 1 AND count(DISTINCT team_code) = 1"
            ") t"
        )
    ).scalar()
    print(f"  groups with MORE THAN ONE team_code (drift): {drifted}")
    print(f"  groups with one team_code repeated (dup)  : {same_team}")

    print()
    print("=== team_code values seen for these duplicated players ===")
    codes = conn.execute(
        text(
            "SELECT team_code, count(*) FROM player_season_batting "
            "WHERE player_id IN (SELECT player_id FROM ("
            "  SELECT player_id, season, league FROM player_season_batting "
            "  WHERE team_code IS NOT NULL GROUP BY player_id, season, league "
            "  HAVING count(*) > 1 AND count(DISTINCT team_code) > 1) d) "
            "GROUP BY team_code ORDER BY 2 DESC"
        )
    ).all()
    for code, n in codes:
        print(f"  {str(code):<8} {n}")
