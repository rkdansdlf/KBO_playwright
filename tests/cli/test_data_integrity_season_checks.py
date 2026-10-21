"""Season id / season type integrity checks (issue #40)."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.cli.data_integrity_checker import CHECKS, check_game_season_id, check_season_classification
from src.models.game import Game
from src.models.season import KboSeason


@pytest.fixture
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(engine)
    KboSeason.__table__.create(engine)
    db = sessionmaker(bind=engine)()
    for code, name in enumerate(["정규시즌", "시범경기", "와일드카드", "준플레이오프", "플레이오프", "한국시리즈"]):
        db.add(KboSeason(season_id=202600 + code, season_year=2026, league_type_code=code, league_type_name=name))
    db.add(KboSeason(season_id=202500, season_year=2025, league_type_code=0, league_type_name="정규시즌"))
    db.commit()
    return db


def _game(db: Session, game_id: str, day: date, season_id: int | None, status: str = "COMPLETED") -> None:
    db.add(Game(game_id=game_id, game_date=day, season_id=season_id, game_status=status, is_primary=True))
    db.commit()


def test_new_checks_run_in_the_daily_job() -> None:
    assert check_game_season_id in CHECKS
    assert check_season_classification in CHECKS


class TestGameSeasonId:
    def test_passes_when_every_game_has_a_valid_season(self, session: Session) -> None:
        _game(session, "20260815HHSS0", date(2026, 8, 15), 202600)
        result = check_game_season_id(session, date(2026, 10, 18))
        assert result.passed, result.message

    def test_null_season_id_anywhere_in_the_season_fails(self, session: Session) -> None:
        _game(session, "20261017LGHT0", date(2026, 10, 17), 202603)
        _game(session, "20260816HHSS0", date(2026, 8, 16), None, status="CANCELLED")
        result = check_game_season_id(session, date(2026, 10, 18))
        assert not result.passed
        assert result.details["null_season_id"] == 1
        assert result.details["sample_null"] == ["20260816HHSS0"]
        assert "repair_season_classification" in result.details["remediation"]

    def test_dangling_and_wrong_year_season_ids_fail(self, session: Session) -> None:
        _game(session, "20260901HHKT0", date(2026, 9, 1), 999999)
        _game(session, "20260902HHKT0", date(2026, 9, 2), 202500)
        result = check_game_season_id(session, date(2026, 9, 3))
        assert not result.passed
        assert result.details["dangling_season_id"] == 1
        assert result.details["wrong_year_season_id"] == 1


class TestSeasonClassification:
    def test_five_korean_series_games_in_one_day_fail(self, session: Session) -> None:
        for gid in ("20261006KTWO0", "20261006NCLG0", "20261006OBLT0", "20261006SKHH0", "20261006SSHT0"):
            _game(session, gid, date(2026, 10, 6), 202605)
        result = check_season_classification(session, date(2026, 10, 6))
        assert not result.passed
        assert set(result.details["violations"]) >= {"postseason_overflow", "postseason_before_start"}

    def test_exhibition_window_game_stored_as_regular_fails(self, session: Session) -> None:
        _game(session, "20260320HTHH0", date(2026, 3, 20), 202600)
        result = check_season_classification(session, date(2026, 3, 20))
        assert not result.passed
        assert result.details["violations"]["exhibition_mismatch"] == ["20260320HTHH0"]
        assert "regular_outside_span" in result.details["violations"]

    def test_regular_game_after_the_regular_season_fails(self, session: Session) -> None:
        _game(session, "20261017LGHT0", date(2026, 10, 17), 202600)
        result = check_season_classification(session, date(2026, 10, 17))
        assert not result.passed
        assert "regular_outside_span" in result.details["violations"]

    def test_correct_days_pass(self, session: Session) -> None:
        _game(session, "20260320HTHH0", date(2026, 3, 20), 202601)
        _game(session, "20261006KTWO0", date(2026, 10, 6), 202600)
        _game(session, "20261006NCLG0", date(2026, 10, 6), 202600)
        _game(session, "20261017LGHT0", date(2026, 10, 17), 202603)
        for day in (date(2026, 3, 20), date(2026, 10, 6), date(2026, 10, 17)):
            result = check_season_classification(session, day)
            assert result.passed, (day, result.details)

    def test_unplayed_placeholders_are_ignored(self, session: Session) -> None:
        _game(session, "20261015OBLG0", date(2026, 10, 15), 202600, status="CANCELLED")
        assert check_season_classification(session, date(2026, 10, 15)).passed

    def test_rained_out_postseason_game_past_its_window_is_not_flagged(self, session: Session) -> None:
        _game(session, "20261024LGHT0", date(2026, 10, 24), 202603)
        assert check_season_classification(session, date(2026, 10, 24)).passed
