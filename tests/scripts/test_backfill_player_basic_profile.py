"""Tests for the ``player_basic`` profile backfill."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from scripts.maintenance import backfill_player_basic_profile as module
from scripts.maintenance.backfill_player_basic_profile import resolve_lineup_position, run_backfill

DDL = (
    "CREATE TABLE teams (team_id VARCHAR(10) PRIMARY KEY)",
    (
        "CREATE TABLE player_basic ("
        "  player_id INTEGER PRIMARY KEY,"
        "  name VARCHAR(100),"
        "  team VARCHAR(50),"
        "  position VARCHAR(50),"
        "  updated_at DATETIME"
        ")"
    ),
    (
        "CREATE TABLE player_season_batting ("
        "  id INTEGER PRIMARY KEY, player_id INTEGER, season INTEGER, team_code VARCHAR(10),"
        "  plate_appearances INTEGER"
        ")"
    ),
    (
        "CREATE TABLE player_season_pitching ("
        "  id INTEGER PRIMARY KEY, player_id INTEGER, season INTEGER, team_code VARCHAR(10)"
        ")"
    ),
    (
        "CREATE TABLE game_lineups ("
        "  id INTEGER PRIMARY KEY, game_id VARCHAR(20), player_id INTEGER,"
        "  team_code VARCHAR(10), position VARCHAR(20)"
        ")"
    ),
)


@pytest.fixture
def session_factory(monkeypatch: pytest.MonkeyPatch) -> sessionmaker:
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        for statement in DDL:
            connection.execute(text(statement))
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(module, "SessionLocal", factory)
    return factory


def _seed_reference_teams(factory: sessionmaker) -> None:
    with factory() as session:
        session.execute(
            text(
                "INSERT INTO teams (team_id) VALUES ('SS'), ('LT'), ('HT'), ('KIA'), ('HH'), ('OB'), ('DB'),"
                " ('OT'), ('EA'), ('WE')"
            )
        )
        session.commit()


def _add_player(
    factory: sessionmaker,
    player_id: int,
    *,
    team: str | None = None,
    position: str | None = None,
    name: str = "홍길동",
) -> None:
    with factory() as session:
        session.execute(
            text("INSERT INTO player_basic (player_id, name, team, position) VALUES (:pid, :name, :team, :position)"),
            {"pid": player_id, "name": name, "team": team, "position": position},
        )
        session.commit()


def _read_player(factory: sessionmaker, player_id: int) -> dict[str, object]:
    with factory() as session:
        row = session.execute(
            text("SELECT team, position FROM player_basic WHERE player_id = :pid"),
            {"pid": player_id},
        ).fetchone()
    return {"team": row[0], "position": row[1]}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("포", "포수"),
        ("유", "유격수"),
        ("一", "1루수"),
        ("二", "2루수"),
        ("三", "3루수"),
        ("중", "중견수"),
        ("좌", "좌익수"),
        ("우", "우익수"),
        ("지", "지명타자"),
        ("투", "투수"),
        ("중좌", "중견수"),
        ("주二", "2루수"),
        ("타좌", "좌익수"),
        ("三一", "3루수"),
        ("3루수", "3루수"),
        ("타", None),
        ("주", None),
        ("UNKNOWN", None),
        ("", None),
    ],
)
def test_resolve_lineup_position(raw: str, expected: str | None) -> None:
    assert resolve_lineup_position(raw) == expected


def test_dry_run_reports_candidates_without_writing(session_factory: sessionmaker) -> None:
    _seed_reference_teams(session_factory)
    _add_player(session_factory, 1)
    with session_factory() as session:
        session.execute(
            text("INSERT INTO player_season_batting (id, player_id, season, team_code) VALUES (1, 1, 2024, 'SS')")
        )
        session.commit()

    report = run_backfill(apply=False)

    assert report.team_candidates == 1
    assert report.team_written == 0
    assert _read_player(session_factory, 1) == {"team": None, "position": None}


def test_apply_uses_the_newest_season_team(session_factory: sessionmaker) -> None:
    _seed_reference_teams(session_factory)
    _add_player(session_factory, 1)
    with session_factory() as session:
        session.execute(
            text(
                "INSERT INTO player_season_batting (id, player_id, season, team_code) VALUES "
                "(1, 1, 2020, 'LT'), (2, 1, 2024, 'SS')"
            )
        )
        session.commit()

    report = run_backfill(apply=True)

    assert report.team_written == 1
    assert _read_player(session_factory, 1)["team"] == "SS"


def test_team_falls_back_to_pitching_then_lineups(session_factory: sessionmaker) -> None:
    _seed_reference_teams(session_factory)
    _add_player(session_factory, 1)
    _add_player(session_factory, 2)
    with session_factory() as session:
        session.execute(
            text("INSERT INTO player_season_pitching (id, player_id, season, team_code) VALUES (1, 1, 2024, 'HT')")
        )
        session.execute(
            text(
                "INSERT INTO game_lineups (id, game_id, player_id, team_code, position) "
                "VALUES (1, '20240401HHLG0', 2, 'HH', '포')"
            )
        )
        session.commit()

    run_backfill(apply=True)

    assert _read_player(session_factory, 1)["team"] == "HT"
    assert _read_player(session_factory, 2)["team"] == "HH"


def test_unknown_team_code_is_rejected(session_factory: sessionmaker) -> None:
    _seed_reference_teams(session_factory)
    _add_player(session_factory, 1)
    with session_factory() as session:
        session.execute(
            text("INSERT INTO player_season_batting (id, player_id, season, team_code) VALUES (1, 1, 2024, 'ZZ')")
        )
        session.commit()

    report = run_backfill(apply=True)

    assert report.team_candidates == 0
    assert _read_player(session_factory, 1)["team"] is None


def test_pseudo_team_code_falls_back_to_an_older_season(session_factory: sessionmaker) -> None:
    """``OT`` (경찰/상무/독립) is not a club, so the previous season wins."""
    _seed_reference_teams(session_factory)
    _add_player(session_factory, 1)
    with session_factory() as session:
        session.execute(
            text(
                "INSERT INTO player_season_batting (id, player_id, season, team_code) VALUES "
                "(1, 1, 2024, 'SS'), (2, 1, 2026, 'OT')"
            )
        )
        session.commit()

    run_backfill(apply=True)

    assert _read_player(session_factory, 1)["team"] == "SS"


def test_pseudo_team_code_is_never_written(session_factory: sessionmaker) -> None:
    _seed_reference_teams(session_factory)
    _add_player(session_factory, 1)
    with session_factory() as session:
        session.execute(
            text("INSERT INTO player_season_batting (id, player_id, season, team_code) VALUES (1, 1, 2026, 'EA')")
        )
        session.commit()

    report = run_backfill(apply=True)

    assert report.team_candidates == 0
    assert _read_player(session_factory, 1)["team"] is None


def test_existing_values_are_never_overwritten(session_factory: sessionmaker) -> None:
    _seed_reference_teams(session_factory)
    _add_player(session_factory, 1, team="OB", position="투수")
    with session_factory() as session:
        session.execute(
            text("INSERT INTO player_season_batting (id, player_id, season, team_code) VALUES (1, 1, 2024, 'SS')")
        )
        session.execute(
            text(
                "INSERT INTO game_lineups (id, game_id, player_id, team_code, position) "
                "VALUES (1, '20240401HHLG0', 1, 'SS', '중')"
            )
        )
        session.commit()

    report = run_backfill(apply=True)

    assert report.team_written == 0
    assert report.position_written == 0
    assert _read_player(session_factory, 1) == {"team": "OB", "position": "투수"}


def test_position_prefers_fielding_over_pinch_hitting(session_factory: sessionmaker) -> None:
    _seed_reference_teams(session_factory)
    _add_player(session_factory, 1)
    with session_factory() as session:
        session.execute(
            text(
                "INSERT INTO game_lineups (id, game_id, player_id, team_code, position) VALUES "
                "(1, '20240401HHLG0', 1, 'HH', '타'),"
                "(2, '20240402HHLG0', 1, 'HH', '타'),"
                "(3, '20240403HHLG0', 1, 'HH', '중'),"
                "(4, '20240404HHLG0', 1, 'HH', '중좌')"
            )
        )
        session.commit()

    run_backfill(apply=True)

    assert _read_player(session_factory, 1)["position"] == "중견수"


def test_position_falls_back_to_pitcher_when_only_pitching(session_factory: sessionmaker) -> None:
    _seed_reference_teams(session_factory)
    _add_player(session_factory, 1)
    with session_factory() as session:
        session.execute(
            text("INSERT INTO player_season_pitching (id, player_id, season, team_code) VALUES (1, 1, 2024, 'SS')")
        )
        session.commit()

    run_backfill(apply=True)

    assert _read_player(session_factory, 1)["position"] == "투수"


def test_pinch_runner_only_gets_no_position(session_factory: sessionmaker) -> None:
    _seed_reference_teams(session_factory)
    _add_player(session_factory, 1)
    with session_factory() as session:
        session.execute(
            text(
                "INSERT INTO game_lineups (id, game_id, player_id, team_code, position) "
                "VALUES (1, '20240401HHLG0', 1, 'HH', '주')"
            )
        )
        session.commit()

    report = run_backfill(apply=True)

    assert report.position_candidates == 0
    assert _read_player(session_factory, 1)["position"] is None


def test_pitcher_fallback_is_blocked_for_a_hitter(session_factory: sessionmaker) -> None:
    """A hitter who merely pitched once must not be labelled ``투수``."""
    _seed_reference_teams(session_factory)
    _add_player(session_factory, 1, name="김성한")
    _add_player(session_factory, 2, name="정상투수")
    with session_factory() as session:
        session.execute(
            text(
                "INSERT INTO player_season_pitching (id, player_id, season, team_code) VALUES "
                "(1, 1, 1986, 'HT'), (2, 2, 2024, 'SS')"
            )
        )
        session.execute(
            text(
                "INSERT INTO player_season_batting (id, player_id, season, team_code, plate_appearances) "
                "VALUES (1, 1, 1986, 'HT', 480)"
            )
        )
        session.commit()

    run_backfill(apply=True)

    assert _read_player(session_factory, 1)["position"] is None
    assert _read_player(session_factory, 2)["position"] == "투수"


def test_minor_batting_usage_still_resolves_the_pitcher(session_factory: sessionmaker) -> None:
    """Below the gate a player is still treated as a pitcher."""
    _seed_reference_teams(session_factory)
    _add_player(session_factory, 1)
    with session_factory() as session:
        session.execute(
            text("INSERT INTO player_season_pitching (id, player_id, season, team_code) VALUES (1, 1, 2024, 'SS')")
        )
        session.execute(
            text(
                "INSERT INTO player_season_batting (id, player_id, season, team_code, plate_appearances) "
                "VALUES (1, 1, 2024, 'SS', 3)"
            )
        )
        session.commit()

    run_backfill(apply=True)

    assert _read_player(session_factory, 1)["position"] == "투수"


def test_normalize_existing_is_opt_in(session_factory: sessionmaker) -> None:
    _seed_reference_teams(session_factory)
    _add_player(session_factory, 1, team="HT")
    _add_player(session_factory, 2, team="OB")

    run_backfill(apply=True)
    assert _read_player(session_factory, 1)["team"] == "HT"

    report = run_backfill(apply=True, normalize_existing=True)
    assert report.existing_team_normalized == 2
    assert _read_player(session_factory, 1)["team"] == "KIA"
    assert _read_player(session_factory, 2)["team"] == "DB"


def test_limit_caps_the_write(session_factory: sessionmaker) -> None:
    _seed_reference_teams(session_factory)
    _add_player(session_factory, 1)
    _add_player(session_factory, 2)
    with session_factory() as session:
        session.execute(
            text(
                "INSERT INTO player_season_batting (id, player_id, season, team_code) VALUES "
                "(1, 1, 2024, 'SS'), (2, 2, 2024, 'LT')"
            )
        )
        session.commit()

    report = run_backfill(apply=True, limit=1)

    assert report.team_written == 1
    teams = {_read_player(session_factory, pid)["team"] for pid in (1, 2)}
    assert teams in ({"SS", None}, {"LT", None})
