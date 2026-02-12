from src.db.engine import Engine
from sqlalchemy import text

def check_teams():
    with Engine.connect() as conn:
        result = conn.execute(text("SELECT team_id, team_name FROM teams ORDER BY team_id"))
        teams = result.fetchall()
        print(f"Found {len(teams)} teams:")
        for t in teams:
            print(f" - {t.team_id}: {t.team_name}")

if __name__ == "__main__":
    check_teams()
