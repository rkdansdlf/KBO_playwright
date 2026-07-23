from __future__ import annotations

import sqlite3

from scripts.maintenance import fix_2020_pitching_nulls as repair


def _seed_database(path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE player_basic (player_id INTEGER, name TEXT, team TEXT)")
        conn.execute(
            "CREATE TABLE game_pitching_stats ("
            "player_id INTEGER, player_name TEXT, team_code TEXT, uniform_no TEXT, game_id TEXT)"
        )
        for table in ("game_batting_stats", "game_lineups"):
            conn.execute(f"CREATE TABLE {table} (player_id INTEGER, player_name TEXT, team_code TEXT, game_id TEXT)")
        conn.execute("INSERT INTO player_basic VALUES (2188, '박세진', 'KT')")
        conn.execute("INSERT INTO game_pitching_stats VALUES (NULL, '박세진', 'KT', NULL, '20200519HHKT0')")


def test_dry_run_then_apply_updates_only_unique_match(tmp_path, capsys) -> None:
    database = tmp_path / "kbo.db"
    _seed_database(database)
    repair.DB_PATH = database

    repair.main(apply=False)
    assert "예상 업데이트 총 1행" in capsys.readouterr().out
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT player_id FROM game_pitching_stats").fetchone()[0] is None

    repair.main(apply=True)
    assert "총 1행 업데이트" in capsys.readouterr().out
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT player_id FROM game_pitching_stats").fetchone()[0] == 2188
