"""Backfill for legacy team codes already stored on game rows (issue #39)."""

from __future__ import annotations

import json
from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from scripts.maintenance.normalize_game_team_codes import (
    main,
    normalize_game_team_codes,
    rollback_from_backup,
)
from src.models.game import Game


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(engine)
    db = sessionmaker(bind=engine)()
    db.add_all(
        [
            Game(
                game_id="20260926LGHT0", game_date=date(2026, 9, 26), away_team="LG", home_team="HT", winning_team="HT"
            ),
            Game(
                game_id="20260921WOOB0", game_date=date(2026, 9, 21), away_team="WO", home_team="OB", winning_team="WO"
            ),
            Game(
                game_id="20260915HTSK0", game_date=date(2026, 9, 15), away_team="HT", home_team="SK", winning_team=None
            ),
            Game(
                game_id="20260501HTLG0", game_date=date(2026, 5, 1), away_team="KIA", home_team="LG", winning_team="LG"
            ),
            Game(
                game_id="20260712EAWE0", game_date=date(2026, 7, 12), away_team="EA", home_team="WE", winning_team="EA"
            ),
            Game(
                game_id="19950501HTOB0", game_date=date(1995, 5, 1), away_team="HT", home_team="OB", winning_team="HT"
            ),
        ],
    )
    db.commit()
    yield db
    db.close()


def _codes(session, game_id):
    game = session.query(Game).filter_by(game_id=game_id).one()
    return game.away_team, game.home_team, game.winning_team


def test_dry_run_reports_but_writes_nothing(session, tmp_path):
    report = normalize_game_team_codes(2026, session=session, backup_dir=tmp_path)

    assert report.scanned == 5
    assert report.games_changed == 3
    assert len(report.changes) == 7
    assert report.backup_path is None
    assert _codes(session, "20260926LGHT0") == ("LG", "HT", "HT")
    assert not list(tmp_path.iterdir())


def test_apply_rewrites_only_legacy_codes_of_the_year(session, tmp_path):
    report = normalize_game_team_codes(2026, apply=True, session=session, backup_dir=tmp_path)

    assert _codes(session, "20260926LGHT0") == ("LG", "KIA", "KIA")
    assert _codes(session, "20260921WOOB0") == ("KH", "DB", "KH")
    assert _codes(session, "20260915HTSK0") == ("KIA", "SSG", None)
    assert _codes(session, "20260501HTLG0") == ("KIA", "LG", "LG")
    assert _codes(session, "20260712EAWE0") == ("EA", "WE", "EA")
    assert _codes(session, "19950501HTOB0") == ("HT", "OB", "HT")
    backup = json.loads(report.backup_path.read_text(encoding="utf-8"))
    assert len(backup["changes"]) == 7

    again = normalize_game_team_codes(2026, apply=True, session=session, backup_dir=tmp_path)
    assert again.changes == []


def test_rollback_restores_the_backup(session, tmp_path):
    report = normalize_game_team_codes(2026, apply=True, session=session, backup_dir=tmp_path)

    restored = rollback_from_backup(report.backup_path, session=session)

    assert restored == 7
    assert _codes(session, "20260926LGHT0") == ("LG", "HT", "HT")
    assert _codes(session, "20260921WOOB0") == ("WO", "OB", "WO")
    assert _codes(session, "20260915HTSK0") == ("HT", "SK", None)


def test_rollback_skips_when_current_value_conflicts(session, tmp_path):
    report = normalize_game_team_codes(2026, apply=True, session=session, backup_dir=tmp_path)

    # Simulate an external concurrent update on one of the normalized rows
    game = session.query(Game).filter(Game.game_id == "20260926LGHT0").one()
    game.home_team = "MODIFIED_AFTER_APPLY"
    session.flush()

    restored = rollback_from_backup(report.backup_path, session=session)

    # 1 column skipped due to conflict (home_team on 20260926LGHT0), 6 columns restored
    assert restored == 6
    assert _codes(session, "20260926LGHT0")[1] == "MODIFIED_AFTER_APPLY"


def test_cli_defaults_to_dry_run(monkeypatch, capsys):
    calls = {}

    def fake_normalize(year, *, apply, backup_dir):
        calls.update(year=year, apply=apply)
        from scripts.maintenance.normalize_game_team_codes import NormalizeReport

        return NormalizeReport(year=year)

    monkeypatch.setattr("scripts.maintenance.normalize_game_team_codes.normalize_game_team_codes", fake_normalize)

    assert main(["--year", "2026"]) == 0

    assert calls == {"year": 2026, "apply": False}
    assert "DRY RUN" in capsys.readouterr().out
