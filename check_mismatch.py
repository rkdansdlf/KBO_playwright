import sqlite3

import pandas as pd


def check_player_stats(player_id):
    conn = sqlite3.connect("data/kbo_dev.db")

    # Check player basic info
    player_basic = pd.read_sql_query(f"SELECT * FROM player_basic WHERE player_id = {player_id}", conn)
    print(f"\n--- Player Basic Info ({player_id}) ---")
    print(player_basic)

    if player_basic.empty:
        print("Player not found in player_basic.")
        return

    # Check season batting
    season_batting = pd.read_sql_query(
        f"SELECT season, league, games, hits, home_runs, rbi FROM player_season_batting WHERE player_id = {player_id}",
        conn,
    )
    if not season_batting.empty:
        print("\n--- Season Batting ---")
        print(season_batting)

        # Check game batting sum
        game_batting_sum = pd.read_sql_query(
            f"""
            SELECT SUBSTR(game_id, 1, 4) as season, COUNT(*) as games, SUM(hits) as hits, SUM(home_runs) as home_runs, SUM(rbi) as rbi
            FROM game_batting_stats
            WHERE player_id = {player_id}
            GROUP BY season
        """,
            conn,
        )
        print("\n--- Game Batting Sum ---")
        print(game_batting_sum)

    # Check season pitching
    season_pitching = pd.read_sql_query(
        f"SELECT season, league, games, wins, losses, saves, holds, innings_pitched FROM player_season_pitching WHERE player_id = {player_id}",
        conn,
    )
    if not season_pitching.empty:
        print("\n--- Season Pitching ---")
        print(season_pitching)

        # Check game pitching sum
        game_pitching_sum = pd.read_sql_query(
            f"""
            SELECT SUBSTR(game_id, 1, 4) as season, COUNT(*) as games, SUM(wins) as wins, SUM(losses) as losses, SUM(saves) as saves, SUM(holds) as holds, SUM(innings_pitched) as innings_pitched
            FROM game_pitching_stats
            WHERE player_id = {player_id}
            GROUP BY season
        """,
            conn,
        )
        print("\n--- Game Pitching Sum ---")
        print(game_pitching_sum)

    conn.close()


if __name__ == "__main__":
    check_player_stats(54097)
    check_player_stats(277)
