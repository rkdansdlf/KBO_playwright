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
            """,
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


def test_check_orphan_data_reports_roster_and_movement_integrity_gaps(tmp_path):
    db_path = tmp_path / "roster_movement.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE teams (team_id TEXT PRIMARY KEY);
            CREATE TABLE player_basic (player_id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE team_daily_roster (
                id INTEGER PRIMARY KEY,
                roster_date DATE NOT NULL,
                team_code TEXT NOT NULL,
                player_id INTEGER NOT NULL,
                player_basic_id INTEGER,
                person_type TEXT NOT NULL,
                player_name TEXT NOT NULL,
                position TEXT NOT NULL
            );
            CREATE TABLE player_movements (
                id INTEGER PRIMARY KEY,
                movement_date DATE NOT NULL,
                section TEXT NOT NULL,
                team_code TEXT NOT NULL,
                canonical_team_id TEXT,
                player_basic_id INTEGER,
                resolution_status TEXT NOT NULL,
                player_name TEXT NOT NULL
            );

            INSERT INTO teams (team_id) VALUES ('LG');
            INSERT INTO player_basic (player_id, name) VALUES (1001, '홍길동');
            INSERT INTO team_daily_roster
                (id, roster_date, team_code, player_id, player_basic_id, person_type, player_name, position)
            VALUES
                (1, '2025-04-01', 'LG', 1001, 1001, 'player', '홍길동', '외야수'),
                (2, '2025-04-01', 'LG', 9001, NULL, 'staff', '코치A', '코치'),
                (3, '2025-04-01', 'LG', 2001, NULL, 'player', '미확정', '투수'),
                (4, '2025-04-01', 'LG', 9999, NULL, 'unknown', '헤더', '포지션');
            INSERT INTO player_movements
                (id, movement_date, section, team_code, canonical_team_id, player_basic_id, resolution_status, player_name)
            VALUES
                (1, '2025-04-01', '등록', 'LG', 'LG', 1001, 'resolved', '홍길동'),
                (2, '2025-04-01', '등록', 'LG', 'LG', NULL, 'unresolved_player', '동명이인'),
                (3, '2025-04-01', '등록', '미상', NULL, NULL, 'unresolved_team', '미상');
            """,
        )
        conn.commit()
    finally:
        conn.close()

    report = collect_report(db_path, sample_limit=5)
    checks = {row["name"]: row for row in report["checks"]}

    assert report["ok"] is False
    assert checks["team_daily_roster parser artifact positions"]["row_count"] == 1
    assert checks["team_daily_roster player rows require canonical player"]["row_count"] == 1
    assert checks["player_movements require canonical team"]["row_count"] == 1
    assert checks["player_movements unresolved player links"]["status"] == "WARN"
    assert checks["player_movements unresolved player links"]["row_count"] == 1
