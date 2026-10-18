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
from src.models.game import Game, GameIdAlias
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
        # QA fix 2/3: 10/12 is before the first postseason day; postseason games
        # are never played earlier than planned, so it is demoted.
        "20261012LGHT0": (202603, 202600),
    }
    # 10/14 and 10/15 are stored regular inside the WC window: decisions needed.
    assert [item["game_id"] for item in plan.review] == ["20261014OBLG0", "20261015OBLG0"]
    assert all(item["blocking"] for item in plan.review)
    assert plan.unresolved == 2
    actions = {(row.season_id, row.action) for row in plan.season_rows}
    # Date-hold mode: WC (stored 10/6~7) and KS (stored 10/26~11/1) lie outside
    # their calendar windows, so they take the window instead of NULL.
    assert actions == {
        (202600, "update"),
        (202601, "create"),
        (202602, "update"),
        (202603, "update"),
        (202605, "update"),
    }
    rows = {row.season_id: row.new for row in plan.season_rows}
    assert rows[202602] == {"start_date": "2026-10-14", "end_date": "2026-10-15"}
    assert rows[202605] == {"start_date": "2026-11-02", "end_date": "2026-11-10"}
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
    assert after["20261014OBLG0"] == 202600  # review item left alone
    assert after["20261015OBLG0"] == 202600  # review item left alone
    with factory() as db:
        assert build_plan(db, 2026).empty
        # KS has not started and review rows remain: stale 10/26 replaced by the window, never NULL.
        assert db.get(KboSeason, 202605).start_date == date(2026, 11, 2)
        assert db.get(KboSeason, 202603).start_date == date(2026, 10, 17)

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
    assert "20261015OBLG0" not in {item["game_id"] for item in plan.review}


def test_year_without_calendar_is_an_error(factory: sessionmaker) -> None:
    assert run(["--year", "2031"], session_factory=factory) == 1


def _add_games(make: sessionmaker, rows: list[tuple[str, date, int | None, str]], **extra: str) -> None:
    with make() as db:
        db.add_all(
            Game(game_id=gid, game_date=day, season_id=sid, game_status=status, is_primary=True, **extra)
            for gid, day, sid, status in rows
        )
        db.commit()


def _review_items(plan) -> dict[str, dict]:
    return {item["game_id"]: item for item in plan.review}


def _review_notes(plan) -> dict[str, str]:
    return {item["game_id"]: item["note"] for item in plan.review}


def test_delayed_postseason_game_outside_every_window_keeps_its_stage(factory: sessionmaker) -> None:
    """10/24 준PO game 5 (rain-delayed past the 준PO window) must not become regular."""
    _add_games(factory, [("20261024LGHT0", date(2026, 10, 24), 202603, "COMPLETED")])
    with factory() as db:
        plan = build_plan(db, 2026)
    assert "20261024LGHT0" not in {change.game_id for change in plan.games}
    assert "delayed series" in _review_notes(plan)["20261024LGHT0"]
    # Informational: the stored type is already the plausible one.
    assert not _review_items(plan)["20261024LGHT0"]["blocking"]


def test_unknown_game_outside_every_window_goes_to_review(factory: sessionmaker) -> None:
    _add_games(factory, [("20261024SSHT0", date(2026, 10, 24), None, "COMPLETED")])
    with factory() as db:
        plan = build_plan(db, 2026)
    assert "20261024SSHT0" not in {change.game_id for change in plan.games}
    assert "type unknown" in _review_notes(plan)["20261024SSHT0"]
    assert "clean it up by hand" in _review_notes(plan)["20261024SSHT0"]


def test_later_to_earlier_stage_move_next_to_a_window_boundary_goes_to_review(factory: sessionmaker) -> None:
    """KS stored on the last 준PO day: an earlier-stage move, but next to a window edge."""
    _add_games(factory, [("20261023LGHT0", date(2026, 10, 23), 202605, "COMPLETED")])
    with factory() as db:
        plan = build_plan(db, 2026)
    assert "20261023LGHT0" not in {change.game_id for change in plan.games}
    assert "window boundary" in _review_notes(plan)["20261023LGHT0"]


def test_earlier_stored_stage_on_the_next_window_edge_goes_to_review(factory: sessionmaker) -> None:
    _add_games(factory, [("20261025LGHT0", date(2026, 10, 25), 202603, "COMPLETED")])
    with factory() as db:
        plan = build_plan(db, 2026)
    assert "20261025LGHT0" not in {change.game_id for change in plan.games}
    assert "earlier than the calendar stage" in _review_notes(plan)["20261025LGHT0"]
    assert _review_items(plan)["20261025LGHT0"]["blocking"]


def test_delayed_semi_playoff_game_inside_the_po_window_is_not_promoted(
    factory: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Regression (round 2): 10/27 준PO game, mid PO window, must not become PO.

    It goes to review, no PO kbo_seasons row is created and no PO start_date is
    written (which would make LeagueStageResolver show 준PO games as PO).
    """
    _add_games(factory, [("20261027LGHT0", date(2026, 10, 27), 202603, "COMPLETED")])
    with factory() as db:
        plan = build_plan(db, 2026)
    assert "20261027LGHT0" not in {change.game_id for change in plan.games}
    assert "earlier than the calendar stage" in _review_notes(plan)["20261027LGHT0"]
    assert 202604 not in {row.season_id for row in plan.season_rows}
    semi = next(row for row in plan.season_rows if row.season_id == 202603)
    assert semi.new["start_date"] == "2026-10-17"

    monkeypatch.setenv(ALLOW_ENV, "1")
    assert run(["--year", "2026", "--apply", "--backup-dir", str(tmp_path)], session_factory=factory) == EXIT_OK
    with factory() as db:
        assert db.get(KboSeason, 202604) is None
        assert db.query(KboSeason).filter(KboSeason.league_type_code == 4).count() == 0
        assert db.query(Game).filter(Game.game_id == "20261027LGHT0").one().season_id == 202603


def test_stage_move_inside_a_window_is_still_repaired(factory: sessionmaker) -> None:
    _add_games(factory, [("20261020LGHT0", date(2026, 10, 20), 202605, "COMPLETED")])
    with factory() as db:
        plan = build_plan(db, 2026)
    changes = {change.game_id: change.new_type for change in plan.games}
    assert changes["20261020LGHT0"] == "semi_playoff"


def test_stored_regular_game_in_a_postseason_window_goes_to_review(factory: sessionmaker) -> None:
    """QA fix 1/3: a regular make-up game can be played on a WC date; the crawler confirms it as regular."""
    with factory() as db:
        plan = build_plan(db, 2026)
    assert "20261014OBLG0" not in {change.game_id for change in plan.games}
    assert "stored regular game inside a postseason window" in _review_notes(plan)["20261014OBLG0"]


def test_null_season_game_in_a_postseason_window_is_still_assigned(factory: sessionmaker) -> None:
    _add_games(factory, [("20261020SSLG0", date(2026, 10, 20), None, "COMPLETED")])
    with factory() as db:
        plan = build_plan(db, 2026)
    assert {change.game_id: change.new_type for change in plan.games}["20261020SSLG0"] == "semi_playoff"


def test_postseason_game_before_the_first_postseason_day_is_demoted(factory: sessionmaker) -> None:
    """QA fix 2/3: 10/12 stored as WC (first WC is 10/14) is safely regular; 10/13 too."""
    _add_games(
        factory,
        [
            ("20261012SSKT0", date(2026, 10, 12), 202602, "COMPLETED"),
            ("20261013SSKT0", date(2026, 10, 13), 202602, "COMPLETED"),
        ],
    )
    with factory() as db:
        plan = build_plan(db, 2026)
    changes = {change.game_id: change.new_type for change in plan.games}
    assert changes["20261012SSKT0"] == "regular"
    assert changes["20261013SSKT0"] == "regular"
    assert changes["20261006KTWO0"] == "regular"


def test_postseason_game_at_the_start_of_the_regular_season_goes_to_review(factory: sessionmaker) -> None:
    _add_games(factory, [("20260329SSKT0", date(2026, 3, 29), 202602, "COMPLETED")])
    with factory() as db:
        plan = build_plan(db, 2026)
    assert "20260329SSKT0" not in {change.game_id for change in plan.games}
    item = _review_items(plan)["20260329SSKT0"]
    assert "start of the regular season" in item["note"]
    assert item["blocking"]


def test_allstar_game_is_never_repaired(factory: sessionmaker) -> None:
    """2026 has no all-star window and no all-star league code: the game must be left alone."""
    _add_games(factory, [("20260711EAWE0", date(2026, 7, 11), 202600, "COMPLETED")], home_team="WE", away_team="EA")
    with factory() as db:
        db.add(KboSeason(season_id=202609, season_year=2026, league_type_code=9, league_type_name="올스타전"))
        db.commit()
    _add_games(factory, [("20260712EAWE0", date(2026, 7, 12), 202609, "COMPLETED")])
    with factory() as db:
        plan = build_plan(db, 2026)
    # Stored as regular: flagged for review (QA fix 1/3), never moved.
    assert "20260711EAWE0" not in {change.game_id for change in plan.games}
    assert "All-Star" in _review_notes(plan)["20260711EAWE0"]
    # Stored under an all-star season: nothing to do.
    touched = {change.game_id for change in plan.games} | {item["game_id"] for item in plan.review}
    assert "20260712EAWE0" not in touched


def test_postseason_start_date_is_the_actual_first_game(factory: sessionmaker) -> None:
    """PO opens late (10/26, not the planned 10/25); a scheduled KS game does not start KS."""
    _add_games(
        factory,
        [
            ("20261026SSLG0", date(2026, 10, 26), None, "COMPLETED"),
            ("20261102SSLG0", date(2026, 11, 2), None, "SCHEDULED"),
        ],
    )
    with factory() as db:
        plan = build_plan(db, 2026)
    rows = {row.season_id: row for row in plan.season_rows}
    assert rows[202604].new["start_date"] == "2026-10-26"
    # Review rows remain in the fixture: KS (not started, stored 10/26 is
    # outside its window) takes the calendar window rather than NULL.
    assert rows[202605].new == {"start_date": "2026-11-02", "end_date": "2026-11-10"}


def _resolve_review_rows(make: sessionmaker) -> None:
    """Drop the fixture rows that land in review, as an operator would after cleanup."""
    with make() as db:
        db.query(Game).filter(Game.game_id.in_(["20261012LGHT0", "20261014OBLG0", "20261015OBLG0"])).delete()
        db.commit()


def test_not_started_postseason_rows_get_no_planned_dates(factory: sessionmaker) -> None:
    """With nothing left in review and no NULL season ids, not-started stages get no dates."""
    _resolve_review_rows(factory)
    with factory() as db:
        plan = build_plan(db, 2026)
    assert not plan.review
    rows = {row.season_id: row for row in plan.season_rows}
    assert 202604 not in rows  # PO has not started and nothing moves into it: no row, no 10/25
    assert rows[202605].new == {"start_date": None, "end_date": None}  # KS 10/26 cleared, not set to 11/2
    assert rows[202602].new == {"start_date": None, "end_date": None}  # WC never confirmed
    assert not plan.warnings


def _set_postseason_rows(make: sessionmaker, po: tuple[date | None, date | None], ks: tuple[date | None, date | None]):
    with make() as db:
        db.add(KboSeason(season_id=202604, season_year=2026, league_type_code=4, league_type_name="플레이오프"))
        db.flush()
        for season_id, (start, end) in ((202604, po), (202605, ks)):
            row = db.get(KboSeason, season_id)
            row.start_date, row.end_date = start, end
        db.commit()


def _po_ks_dates(make: sessionmaker) -> tuple[tuple, tuple]:
    with make() as db:
        po, ks = db.get(KboSeason, 202604), db.get(KboSeason, 202605)
        return (po.start_date, po.end_date), (ks.start_date, ks.end_date)


PO_WINDOW = (date(2026, 10, 25), date(2026, 10, 31))
KS_WINDOW = (date(2026, 11, 2), date(2026, 11, 10))


def test_hold_mode_replaces_stale_dates_with_the_window(
    factory: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """QA fix 2/3 repro 1: PO 10/18 and KS 10/26 are wrong; review rows remain; --apply must fix them."""
    _set_postseason_rows(factory, (date(2026, 10, 18), date(2026, 10, 24)), (date(2026, 10, 26), date(2026, 11, 1)))
    with factory() as db:
        plan = build_plan(db, 2026)
    assert plan.unresolved > 0
    rows = {row.season_id: row.new for row in plan.season_rows}
    assert rows[202604] == {"start_date": "2026-10-25", "end_date": "2026-10-31"}
    assert rows[202605] == {"start_date": "2026-11-02", "end_date": "2026-11-10"}

    monkeypatch.setenv(ALLOW_ENV, "1")
    assert run(["--year", "2026", "--apply", "--backup-dir", str(tmp_path)], session_factory=factory) == EXIT_OK
    assert _po_ks_dates(factory) == (PO_WINDOW, KS_WINDOW)


def test_hold_mode_keeps_dates_already_inside_the_window(factory: sessionmaker) -> None:
    _set_postseason_rows(factory, PO_WINDOW, KS_WINDOW)
    with factory() as db:
        plan = build_plan(db, 2026)
    assert {202604, 202605}.isdisjoint({row.season_id for row in plan.season_rows})
    assert any(w.startswith("playoff: not started") and "kept the stored dates" in w for w in plan.warnings)


def test_null_dates_plus_a_null_season_game_get_the_window(
    factory: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """QA fix 2/3 repro 2: PO/KS start NULL and a NULL season_id game (10/13) left: apply writes the windows."""
    _resolve_review_rows(factory)
    _set_postseason_rows(factory, (None, None), (None, None))
    _add_games(factory, [("20261013SSHT0", date(2026, 10, 13), None, "COMPLETED")])
    with factory() as db:
        plan = build_plan(db, 2026)
    assert [item["game_id"] for item in plan.review] == ["20261013SSHT0"]
    assert plan.unresolved == 1
    rows = {row.season_id: row.new for row in plan.season_rows}
    assert rows[202604] == {"start_date": "2026-10-25", "end_date": "2026-10-31"}
    assert rows[202605] == {"start_date": "2026-11-02", "end_date": "2026-11-10"}

    monkeypatch.setenv(ALLOW_ENV, "1")
    assert run(["--year", "2026", "--apply", "--backup-dir", str(tmp_path)], session_factory=factory) == EXIT_OK
    assert _po_ks_dates(factory) == (PO_WINDOW, KS_WINDOW)


def test_unexpected_error_is_reported_cleanly(
    factory: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from src.cli.backfill import repair_season_classification as module

    def boom(*_args: object, **_kwargs: object) -> None:
        msg = "disk full"
        raise OSError(msg)

    monkeypatch.setenv(ALLOW_ENV, "1")
    monkeypatch.setattr(module, "apply_plan", boom)
    before = _season_ids(factory)
    code = run(["--year", "2026", "--apply", "--backup-dir", str(tmp_path)], session_factory=factory)
    out = capsys.readouterr()
    assert code == 1
    assert "error: unexpected OSError: disk full" in out.out
    assert "backup JSON:" in out.out
    assert str(next(tmp_path.glob("*.json"))) in out.out
    assert "Traceback" not in out.out + out.err
    assert _season_ids(factory) == before


def test_review_game_does_not_date_its_stored_stage(
    factory: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Regression (round 3): a 준PO-stored game on the WC date (10/14) is a review item.

    Its stored type is unconfirmed, so 준PO must still start on its real first
    game (10/17), in the plan and after a real --apply.
    """
    _add_games(factory, [("20261014SSHT0", date(2026, 10, 14), 202603, "COMPLETED")])
    with factory() as db:
        plan = build_plan(db, 2026)
    assert "window boundary" in _review_notes(plan)["20261014SSHT0"]
    assert "20261014SSHT0" not in {change.game_id for change in plan.games}
    rows = {row.season_id: row for row in plan.season_rows}
    assert rows[202603].new["start_date"] == "2026-10-17"
    # WC has no confirmed game (10/14 rows are in review): hold mode, stale 10/6 -> window.
    assert rows[202602].new == {"start_date": "2026-10-14", "end_date": "2026-10-15"}

    monkeypatch.setenv(ALLOW_ENV, "1")
    assert run(["--year", "2026", "--apply", "--backup-dir", str(tmp_path)], session_factory=factory) == EXIT_OK
    with factory() as db:
        assert db.get(KboSeason, 202603).start_date == date(2026, 10, 17)
        assert db.get(KboSeason, 202602).start_date == date(2026, 10, 14)  # window, not NULL
        assert db.query(Game).filter(Game.game_id == "20261014SSHT0").one().season_id == 202603
        assert build_plan(db, 2026).empty


def test_review_game_does_not_start_a_stage_that_has_not_started(
    factory: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Regression (round 3): a PO-stored game on 10/18 (준PO window edge) is a review item.

    PO has not started, so the game must not date PO (no 10/18). Review rows
    remain, so (QA fix 2/3) the NULL PO row takes its calendar window, never 10/18.
    """
    with factory() as db:
        db.add(KboSeason(season_id=202604, season_year=2026, league_type_code=4, league_type_name="플레이오프"))
        db.commit()
    _add_games(factory, [("20261018SSLG0", date(2026, 10, 18), 202604, "COMPLETED")])
    with factory() as db:
        plan = build_plan(db, 2026)
    assert "window boundary" in _review_notes(plan)["20261018SSLG0"]
    assert "20261018SSLG0" not in {change.game_id for change in plan.games}
    rows = {row.season_id: row.new for row in plan.season_rows}
    assert rows[202604] == {"start_date": "2026-10-25", "end_date": "2026-10-31"}
    semi = next(row for row in plan.season_rows if row.season_id == 202603)
    assert semi.new["start_date"] == "2026-10-17"

    monkeypatch.setenv(ALLOW_ENV, "1")
    assert run(["--year", "2026", "--apply", "--backup-dir", str(tmp_path)], session_factory=factory) == EXIT_OK
    with factory() as db:
        po = db.get(KboSeason, 202604)
        assert (po.start_date, po.end_date) == PO_WINDOW
        assert db.get(KboSeason, 202603).start_date == date(2026, 10, 17)
        assert db.query(Game).filter(Game.game_id == "20261018SSLG0").one().season_id == 202604


@pytest.fixture
def recrawled() -> sessionmaker:
    """2026 after a correct Naver re-crawl: every row is stored under its real type."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Game.__table__.create(engine)
    KboSeason.__table__.create(engine)
    make = sessionmaker(bind=engine, expire_on_commit=False)
    spans = {
        202600: (0, "정규시즌", date(2026, 3, 28), date(2026, 10, 12)),
        202601: (1, "시범경기", date(2026, 3, 12), date(2026, 3, 24)),
        202602: (2, "와일드카드", date(2026, 10, 14), date(2026, 10, 15)),
        202603: (3, "준플레이오프", date(2026, 10, 17), date(2026, 10, 23)),
        202604: (4, "플레이오프", date(2026, 10, 18), date(2026, 10, 24)),  # stale
        202605: (5, "한국시리즈", date(2026, 10, 26), date(2026, 11, 1)),  # stale
    }
    with make() as db:
        db.add_all(
            KboSeason(
                season_id=sid, season_year=2026, league_type_code=code, league_type_name=name, start_date=a, end_date=b
            )
            for sid, (code, name, a, b) in spans.items()
        )
        rows = [
            ("20260320HTHH0", date(2026, 3, 20), 202601, "COMPLETED"),
            ("20261012LGHT0", date(2026, 10, 12), 202600, "COMPLETED"),
            ("20261013SSKT0", date(2026, 10, 13), 202600, "COMPLETED"),  # regular game scheduled 10/13
            ("20261014OBLG0", date(2026, 10, 14), 202602, "COMPLETED"),
            ("20261016OBLG0", date(2026, 10, 16), 202602, "COMPLETED"),  # WC game 2 rained out to 10/16
            ("20261017LGHT0", date(2026, 10, 17), 202603, "COMPLETED"),
            ("20261024LGHT0", date(2026, 10, 24), 202603, "COMPLETED"),  # 준PO game 5 postponed to 10/24
        ]
        db.add_all(
            Game(game_id=gid, game_date=day, season_id=sid, game_status=status, is_primary=True)
            for gid, day, sid, status in rows
        )
        db.commit()
    return make


def test_after_a_correct_recrawl_nothing_needs_a_decision(
    recrawled: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """QA fix 2/3: plausible kept rows are informational; unresolved reaches 0 and date-hold turns off."""
    with recrawled() as db:
        plan = build_plan(db, 2026)
    assert not plan.games
    assert {item["game_id"] for item in plan.review} == {"20261013SSKT0", "20261016OBLG0", "20261024LGHT0"}
    assert not any(item["blocking"] for item in plan.review)
    assert plan.unresolved == 0
    assert not plan.warnings
    rows = {row.season_id: row.new for row in plan.season_rows}
    # Hold mode is off: not-started PO/KS are cleared, not set to the planned window.
    assert rows == {
        202604: {"start_date": None, "end_date": None},
        202605: {"start_date": None, "end_date": None},
    }

    monkeypatch.setenv(ALLOW_ENV, "1")
    assert run(["--year", "2026", "--apply", "--backup-dir", str(tmp_path)], session_factory=recrawled) == EXIT_OK
    with recrawled() as db:
        assert (db.get(KboSeason, 202604).start_date, db.get(KboSeason, 202605).start_date) == (None, None)
        assert db.get(KboSeason, 202602).start_date == date(2026, 10, 14)
        assert db.get(KboSeason, 202603).start_date == date(2026, 10, 17)
        assert build_plan(db, 2026).empty


def test_regular_make_up_game_beside_a_postseason_game_is_informational(recrawled: sessionmaker) -> None:
    _add_games(recrawled, [("20261017SSKT0", date(2026, 10, 17), 202600, "COMPLETED")])
    with recrawled() as db:
        plan = build_plan(db, 2026)
    item = _review_items(plan)["20261017SSKT0"]
    assert "make-up" in item["note"]
    assert not item["blocking"]
    assert plan.unresolved == 0


def test_warning_does_not_call_a_played_stage_not_started(factory: sessionmaker) -> None:
    """QA fix 2/3: WC has a played game (10/14, in review) -- it is not 'not started'."""
    with factory() as db:
        plan = build_plan(db, 2026)
    wildcard = [w for w in plan.warnings if w.startswith("wildcard")]
    assert wildcard
    assert "not started" not in wildcard[0]
    assert "has played games" in wildcard[0]
    korean = [w for w in plan.warnings if w.startswith("korean_series")]
    assert korean
    assert "not started" in korean[0]


def _update_games(make: sessionmaker, season_id: int, game_ids: list[str]) -> None:
    with make() as db:
        db.query(Game).filter(Game.game_id.in_(game_ids)).update({Game.season_id: season_id}, synchronize_session=False)
        db.commit()


def test_semi_playoff_games_stored_as_wildcard_keep_the_semi_playoff_dates(
    recrawled: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """QA fix 3/3 repro: 준PO games 10/17-21 stored as WC are [decide]; 준PO dates are kept, not cleared."""
    _add_games(
        recrawled,
        [
            ("20261018LGHT0", date(2026, 10, 18), 202603, "COMPLETED"),
            ("20261020HTLG0", date(2026, 10, 20), 202603, "COMPLETED"),
            ("20261021HTLG0", date(2026, 10, 21), 202603, "COMPLETED"),
        ],
    )
    semi_ids = ["20261017LGHT0", "20261018LGHT0", "20261020HTLG0", "20261021HTLG0"]
    _update_games(recrawled, 202602, semi_ids)
    with recrawled() as db:
        plan = build_plan(db, 2026)
    items = _review_items(plan)
    assert all(items[gid]["blocking"] for gid in semi_ids)
    assert plan.unresolved >= len(semi_ids)
    assert not {change.game_id for change in plan.games} & set(semi_ids)
    assert 202603 not in {row.season_id for row in plan.season_rows}  # 10/17~10/23 is inside the window: kept

    monkeypatch.setenv(ALLOW_ENV, "1")
    assert run(["--year", "2026", "--apply", "--backup-dir", str(tmp_path)], session_factory=recrawled) == EXIT_OK
    with recrawled() as db:
        semi = db.get(KboSeason, 202603)
        assert (semi.start_date, semi.end_date) == (date(2026, 10, 17), date(2026, 10, 23))
        again = build_plan(db, 2026)
        assert again.unresolved > 0
        assert all(_review_items(again)[gid]["blocking"] for gid in semi_ids)


def test_confirmed_stage_does_not_start_after_its_review_games(recrawled: sessionmaker) -> None:
    """준PO 10/17-21 stored as WC (review) plus a confirmed 10/22 준PO game: 준PO still starts 10/17."""
    _add_games(recrawled, [("20261022HTLG0", date(2026, 10, 22), 202603, "COMPLETED")])
    _update_games(recrawled, 202602, ["20261017LGHT0"])
    with recrawled() as db:
        plan = build_plan(db, 2026)
    assert 202603 not in {row.season_id for row in plan.season_rows}


def test_informational_review_row_alone_keeps_its_stage_dates(recrawled: sessionmaker) -> None:
    """Safeguard: with nothing to decide, a stage an [info] row relates to is not cleared."""
    _update_games(recrawled, 202603, ["20261024LGHT0"])  # unchanged: 10/24 준PO stays [info]
    _add_games(recrawled, [("20261016SSKT0", date(2026, 10, 16), 202600, "COMPLETED")])  # beside WC 10/16: [info]
    with recrawled() as db:
        db.query(Game).filter(Game.game_id == "20261017LGHT0").delete()
        db.commit()
        plan = build_plan(db, 2026)
    assert plan.unresolved == 0
    # 준PO has no confirmed game now; the 10/24 [info] row relates to it, so its dates are kept.
    assert 202603 not in {row.season_id for row in plan.season_rows}
    assert any(w.startswith("semi_playoff") and "relate to this stage" in w for w in plan.warnings)


def test_regular_game_after_the_first_postseason_day_needs_a_decision(recrawled: sessionmaker) -> None:
    """QA fix 3/3 repro: the 10/16 WC game stored as regular, with no other postseason game that day."""
    _update_games(recrawled, 202600, ["20261016OBLG0"])
    with recrawled() as db:
        plan = build_plan(db, 2026)
    item = _review_items(plan)["20261016OBLG0"]
    assert item["blocking"]
    assert plan.unresolved == 1
    assert not _review_items(plan)["20261013SSKT0"]["blocking"]  # before the first postseason day: [info]


def test_regular_make_up_game_after_the_span_beside_a_postseason_game_is_informational(
    recrawled: sessionmaker,
) -> None:
    _add_games(recrawled, [("20261016SSKT0", date(2026, 10, 16), 202600, "COMPLETED")])
    with recrawled() as db:
        plan = build_plan(db, 2026)
    assert not _review_items(plan)["20261016SSKT0"]["blocking"]
    assert plan.unresolved == 0


def test_postseason_id_evidence_blocks_the_pre_postseason_demotion(recrawled: sessionmaker) -> None:
    """QA fix 3/3 (optional): a Naver postseason id (alias) on 10/12 goes to [decide], not to regular."""
    with recrawled() as db:
        GameIdAlias.__table__.create(db.get_bind())
    _update_games(recrawled, 202603, ["20261012LGHT0", "20261013SSKT0"])
    with recrawled() as db:
        db.add(GameIdAlias(alias_game_id="33331012LGHT02026", canonical_game_id="20261012LGHT0", source="naver"))
        db.commit()
        plan = build_plan(db, 2026)
    changes = {change.game_id: change.new_type for change in plan.games}
    assert "20261012LGHT0" not in changes
    assert _review_items(plan)["20261012LGHT0"]["blocking"]
    assert changes["20261013SSKT0"] == "regular"  # no postseason evidence: still demoted


def test_unexpected_database_error_hides_bind_parameters() -> None:
    from sqlalchemy.exc import IntegrityError

    from src.cli.backfill.repair_season_classification import _safe_error_text

    exc = IntegrityError(
        "UPDATE game SET season_id=? WHERE game_id=?", (202603, "SECRET-ROW"), Exception("constraint failed")
    )
    text = _safe_error_text(exc)
    assert "SECRET-ROW" not in text
    assert "parameters" not in text
    assert "constraint failed" in text
    assert len(_safe_error_text(OSError("x" * 1000))) <= 300
