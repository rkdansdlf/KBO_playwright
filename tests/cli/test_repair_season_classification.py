"""Season classification repair CLI (issue #40)."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from src.cli.repair_season_classification import (
    ALLOW_ENV,
    EXIT_GUARD_DENIED,
    EXIT_OK,
    build_plan,
    run,
)
from src.models.game import Game
from src.models.season import KboSeason


@pytest.fixture
def factory() -> sessionmaker:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Game.__table__.create(engine)
    KboSeason.__table__.create(engine)
    make = sessionmaker(bind=engine, expire_on_commit=False)
    with make() as db:
        # The broken state described in issue #40.
        db.add_all(
            [
                KboSeason(
                    season_id=202600,
                    season_year=2026,
                    league_type_code=0,
                    league_type_name="정규시즌",
                    start_date=date(2026, 3, 12),
                    end_date=date(2026, 10, 31),
                ),
                KboSeason(
                    season_id=202602,
                    season_year=2026,
                    league_type_code=2,
                    league_type_name="와일드카드",
                    start_date=date(2026, 10, 6),
                    end_date=date(2026, 10, 7),
                ),
                KboSeason(
                    season_id=202603,
                    season_year=2026,
                    league_type_code=3,
                    league_type_name="준플레이오프",
                    start_date=date(2026, 10, 9),
                    end_date=date(2026, 10, 14),
                ),
                KboSeason(
                    season_id=202605,
                    season_year=2026,
                    league_type_code=5,
                    league_type_name="한국시리즈",
                    start_date=date(2026, 10, 26),
                    end_date=date(2026, 11, 1),
                ),
            ],
        )
        rows = [
            ("20260320HTHH0", date(2026, 3, 20), 202600, "COMPLETED"),  # exhibition stored as regular
            ("20260328KTLG0", date(2026, 3, 28), 202600, "COMPLETED"),  # correct
            ("20260816HHSS0", date(2026, 8, 16), None, "CANCELLED"),  # no season at all
            ("20261006KTWO0", date(2026, 10, 6), 202605, "COMPLETED"),  # regular stored as KS
            ("20261012LGHT0", date(2026, 10, 12), 202603, "COMPLETED"),  # regular stored as 준PO
            ("20261014OBLG0", date(2026, 10, 14), 202600, "COMPLETED"),  # WC stored as regular
            ("20261015OBLG0", date(2026, 10, 15), 202600, "CANCELLED"),  # unneeded WC game 2
            ("20261017LGHT0", date(2026, 10, 17), 202603, "COMPLETED"),  # correct
        ]
        db.add_all(
            Game(game_id=gid, game_date=day, season_id=sid, game_status=status, is_primary=True)
            for gid, day, sid, status in rows
        )
        db.commit()
    return make


def _season_ids(make: sessionmaker) -> dict[str, int | None]:
    with make() as db:
        return {game.game_id: game.season_id for game in db.query(Game).all()}


def test_plan_matches_the_issue(factory: sessionmaker) -> None:
    with factory() as db:
        plan = build_plan(db, 2026)
    changes = {change.game_id: (change.old_season_id, change.new_season_id) for change in plan.games}
    assert changes == {
        "20260320HTHH0": (202600, 202601),
        "20260816HHSS0": (None, 202600),
        "20261006KTWO0": (202605, 202600),
        "20261012LGHT0": (202603, 202600),
        "20261014OBLG0": (202600, 202602),
    }
    assert [item["game_id"] for item in plan.review] == ["20261015OBLG0"]
    actions = {(row.season_id, row.action) for row in plan.season_rows}
    assert actions == {
        (202600, "update"),
        (202601, "create"),
        (202602, "update"),
        (202603, "update"),
        (202604, "create"),
        (202605, "update"),
    }
    regular = next(row for row in plan.season_rows if row.season_id == 202600)
    assert regular.new == {"start_date": "2026-03-28", "end_date": "2026-10-12"}


def test_dry_run_writes_nothing(factory: sessionmaker, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(ALLOW_ENV, "1")
    before = _season_ids(factory)
    assert run(["--year", "2026", "--backup-dir", str(tmp_path)], session_factory=factory) == EXIT_OK
    assert _season_ids(factory) == before
    assert not list(tmp_path.iterdir())


def test_apply_without_the_env_guard_is_refused(
    factory: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv(ALLOW_ENV, raising=False)
    before = _season_ids(factory)
    code = run(["--year", "2026", "--apply", "--backup-dir", str(tmp_path)], session_factory=factory)
    assert code == EXIT_GUARD_DENIED
    assert _season_ids(factory) == before


def test_apply_is_idempotent_and_rollback_restores(
    factory: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv(ALLOW_ENV, "1")
    before = _season_ids(factory)
    assert run(["--year", "2026", "--apply", "--backup-dir", str(tmp_path)], session_factory=factory) == EXIT_OK

    after = _season_ids(factory)
    assert after["20260320HTHH0"] == 202601
    assert after["20260816HHSS0"] == 202600
    assert after["20261006KTWO0"] == 202600
    assert after["20261015OBLG0"] == 202600  # review item left alone
    with factory() as db:
        assert build_plan(db, 2026).empty
        assert db.get(KboSeason, 202605).start_date == date(2026, 11, 2)

    backups = list(tmp_path.glob("season_classification_2026_*.json"))
    assert len(backups) == 1
    payload = json.loads(backups[0].read_text(encoding="utf-8"))
    assert payload["kind"] == "season_classification_repair"

    code = run(["--rollback", str(backups[0]), "--apply"], session_factory=factory)
    assert code == EXIT_OK
    assert _season_ids(factory) == before
    with factory() as db:
        assert db.get(KboSeason, 202601) is None  # created by the repair, removed again
        assert db.get(KboSeason, 202605).start_date == date(2026, 10, 26)
        assert db.get(KboSeason, 202600).start_date == date(2026, 3, 12)


def test_rollback_does_not_clobber_later_changes(
    factory: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv(ALLOW_ENV, "1")
    run(["--year", "2026", "--apply", "--backup-dir", str(tmp_path)], session_factory=factory)
    with factory() as db:
        db.query(Game).filter(Game.game_id == "20261006KTWO0").one().season_id = 202604
        db.commit()
    backup = next(tmp_path.glob("*.json"))
    run(["--rollback", str(backup), "--apply"], session_factory=factory)
    assert _season_ids(factory)["20261006KTWO0"] == 202604


def test_include_unplayed_postseason(factory: sessionmaker) -> None:
    with factory() as db:
        plan = build_plan(db, 2026, include_unplayed_postseason=True)
    assert "20261015OBLG0" in {change.game_id for change in plan.games}
    assert not plan.review


def test_year_without_calendar_is_an_error(factory: sessionmaker) -> None:
    assert run(["--year", "2031"], session_factory=factory) == 1
