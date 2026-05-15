import sqlite3

from scripts.verification.check_orphan_data import collect_report


def test_check_orphan_data_reports_missing_profiles_and_unknown_stubs(tmp_path):
    db_path = tmp_path / "integrity.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE game (game_id TEXT PRIMARY KEY);
            CREATE TABLE teams (team_id TEXT PRIMARY KEY);
            CREATE TABLE player_basic (player_id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE player_season_batting (player_id INTEGER, team_code TEXT);
            CREATE TABLE player_season_pitching (player_id INTEGER, team_code TEXT);
            CREATE TABLE game_metadata (game_id TEXT PRIMARY KEY REFERENCES game(game_id));

            INSERT INTO player_basic (player_id, name) VALUES (1001, 'Unknown 1001');
            INSERT INTO player_season_batting (player_id, team_code) VALUES (1001, 'LG');
            INSERT INTO player_season_pitching (player_id, team_code) VALUES (2001, 'SS');
            INSERT INTO game_metadata (game_id) VALUES ('20250101LGSS0');
            """
        )
        conn.commit()
    finally:
        conn.close()

    report = collect_report(db_path, sample_limit=5)
    checks = {row["name"]: row for row in report["checks"]}

    assert report["ok"] is False
    assert checks["player_season_pitching -> player_basic"]["row_count"] == 1
    assert checks["player_season_batting -> Unknown player_basic stubs"]["row_count"] == 1
    assert checks["player_basic Unknown ID stubs"]["distinct_count"] == 1
    assert checks["game_metadata -> game"]["row_count"] == 1
