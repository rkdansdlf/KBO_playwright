from src.db.engine import Engine
from sqlalchemy import text

def check_counts():
    with Engine.connect() as conn:
        print("--- 1990 Season Stats ---")
        batting_count = conn.execute(text("SELECT COUNT(*) FROM player_season_batting WHERE season = 1990")).scalar()
        pitching_count = conn.execute(text("SELECT COUNT(*) FROM player_season_pitching WHERE season = 1990")).scalar()
        print(f"Batting: {batting_count} (Expected ~36)")
        print(f"Pitching: {pitching_count}")

        print("\n--- 2001 Season Games ---")
        game_count = conn.execute(text("SELECT COUNT(*) FROM game WHERE game_id LIKE '2001%'")).scalar()
        print(f"Games Saved: {game_count}")
        
        # Check for errors in game_inning_scores
        score_count = conn.execute(text("SELECT COUNT(*) FROM game_inning_scores WHERE game_id LIKE '2001%'")).scalar()
        print(f"Inning Scores Saved: {score_count}")

if __name__ == "__main__":
    check_counts()
