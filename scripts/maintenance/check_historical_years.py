from sqlalchemy import text

from src.db.engine import Engine


def main():
    with Engine.connect() as conn:
        years_batting = conn.execute(
            text("SELECT DISTINCT season FROM player_season_batting WHERE season < 2001 ORDER BY season")
        ).fetchall()
        years_pitching = conn.execute(
            text("SELECT DISTINCT season FROM player_season_pitching WHERE season < 2001 ORDER BY season")
        ).fetchall()

        print("\n🏏 Batting Years present:")
        print([y[0] for y in years_batting])

        print("\n⚾ Pitching Years present:")
        print([y[0] for y in years_pitching])


if __name__ == "__main__":
    main()
