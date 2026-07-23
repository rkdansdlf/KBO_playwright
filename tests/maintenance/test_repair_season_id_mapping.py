"""Tests for the bounded season-id mapping repair."""

from __future__ import annotations

from sqlalchemy import create_engine, text

from scripts.maintenance.repair_season_id_mapping import apply_repair, build_repair_plan


def _engine():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE kbo_seasons ("
                "season_id INTEGER PRIMARY KEY, season_year INTEGER, league_type_code INTEGER)"
            ),
        )
        conn.execute(
            text("CREATE TABLE game (game_id TEXT PRIMARY KEY, season_id INTEGER, game_date TEXT, game_status TEXT)"),
        )
        conn.execute(text("INSERT INTO kbo_seasons VALUES (229, 2020, 0), (230, 2020, 1)"))
        conn.execute(
            text(
                "INSERT INTO game VALUES "
                "('g1', 230, '2020-05-05', 'COMPLETED'), "
                "('g2', 230, '2020-10-31', 'UNRESOLVED_MISSING'), "
                "('g3', 230, '2020-03-14', 'CANCELLED'), "
                "('g4', 230, '2020-11-01', 'COMPLETED')"
            ),
        )
    return engine


def test_plan_is_bounded_and_read_only() -> None:
    engine = _engine()
    with engine.connect() as conn:
        plan = build_repair_plan(conn, 2020)
        assert plan["candidate_rows"] == 2
        assert plan["candidate_terminal_rows"] == 1
        assert conn.execute(text("SELECT COUNT(*) FROM game WHERE season_id = 230")).scalar_one() == 4


def test_apply_is_idempotent() -> None:
    engine = _engine()
    with engine.begin() as conn:
        first = apply_repair(conn, 2020)
        second = apply_repair(conn, 2020)
        counts = conn.execute(
            text("SELECT season_id, COUNT(*) FROM game GROUP BY season_id ORDER BY season_id"),
        ).fetchall()

    assert first["updated_rows"] == 2
    assert second["updated_rows"] == 0
    assert counts == [(229, 2), (230, 2)]
