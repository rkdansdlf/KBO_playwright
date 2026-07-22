"""Tests for the read-only duplicate-player audit (legacy vs modern ids)."""

from __future__ import annotations

from sqlalchemy import create_engine, text

from scripts.maintenance.audit_player_duplicates import (
    LEGACY_MAX_ID,
    _classify_player_id,
    audit_player_duplicates,
)


def _make_engine() -> object:
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE player_basic (player_id INTEGER PRIMARY KEY, name TEXT, birth_date TEXT)"))
        conn.execute(text("CREATE TABLE player_season_pitching (player_id INTEGER, season INTEGER)"))
    return engine


def _seed(engine: object) -> None:
    rows = [
        (3850, "장민재", None),
        (79764, "장민재", "1990-01-01"),
        (3597, "장시환", None),
        (77318, "장시환", "1991-05-05"),
        (50001, "김철수", None),
        (50002, "김철수", None),
        (60001, "이영희", "1992-02-02"),
    ]
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO player_basic (player_id, name, birth_date) VALUES (:pid, :n, :b)"),
            [{"pid": r[0], "n": r[1], "b": r[2]} for r in rows],
        )
        conn.execute(
            text("INSERT INTO player_season_pitching (player_id, season) VALUES (:pid, :s)"),
            [{"pid": 3850, "s": 2021}, {"pid": 79764, "s": 2021}],
        )


def test_classify_player_id() -> None:
    assert _classify_player_id(3850) == "legacy"
    assert _classify_player_id(79764) == "modern"
    assert _classify_player_id(900001) == "pseudo"


def test_audit_player_duplicates_groups() -> None:
    engine = _make_engine()
    _seed(engine)
    with engine.connect() as conn:
        report = audit_player_duplicates(conn)

    assert report["total_players"] == 7
    assert report["duplicate_name_count"] == 3
    assert report["clean_pair_count"] == 2
    assert report["ambiguous_count"] == 1
    assert report["mergeable_legacy_season_pitching_rows"] == 1

    clean_names = {c["name"] for c in report["clean_pairs"]}
    assert clean_names == {"장민재", "장시환"}
    ambiguous = report["ambiguous_names"]
    assert len(ambiguous) == 1
    assert ambiguous[0]["name"] == "김철수"
    assert sorted(ambiguous[0]["modern_ids"]) == [50001, 50002]


def test_audit_player_duplicates_no_duplicates() -> None:
    engine = _make_engine()
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO player_basic (player_id, name, birth_date) VALUES (:pid, :n, :b)"),
            [
                {"pid": 50001, "n": "가", "b": "2000-01-01"},
                {"pid": 50002, "n": "나", "b": "2000-01-01"},
            ],
        )
    with engine.connect() as conn:
        report = audit_player_duplicates(conn)
    assert report["duplicate_name_count"] == 0
    assert report["clean_pair_count"] == 0
    assert report["ambiguous_count"] == 0
    assert report["mergeable_legacy_season_pitching_rows"] == 0
