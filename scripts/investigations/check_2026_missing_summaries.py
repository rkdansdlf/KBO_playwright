from sqlalchemy import text

from src.db.engine import SessionLocal


def check_missing_summaries():
    with SessionLocal() as session:
        query = text("""
            SELECT g.game_id, g.game_date, g.away_team, g.home_team
            FROM game g
            LEFT JOIN game_summary gs ON g.game_id = gs.game_id
            WHERE gs.game_id IS NULL
              AND g.game_status = 'COMPLETED'
        """)
        results = session.execute(query).fetchall()

        print(f"Found {len(results)} completed games in 2026 missing summaries.")
        for r in results:
            print(f"Missing: {r.game_id} ({r.game_date}) - {r.away_team} @ {r.home_team}")


if __name__ == "__main__":
    check_missing_summaries()
