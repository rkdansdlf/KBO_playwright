"""Tests for ParkFactorCalculator — stadium park factor calculation."""

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.aggregators.park_factor_calculator import ParkFactorCalculator
from src.models.game import Game
from src.models.season import KboSeason


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(bind=engine)
    KboSeason.__table__.create(bind=engine)
    sess = sessionmaker(bind=engine)()
    yield sess
    sess.close()


def _add_season(session, season_id=1, year=2025, league_type="정규시즌"):
    session.add(KboSeason(season_id=season_id, season_year=year, league_type_code=1, league_type_name=league_type))
    session.commit()


def _add_game(
    session, game_id="20250101", stadium="잠실", home_score=5, away_score=3, game_status="COMPLETED", season_id=1
):
    session.add(
        Game(
            game_id=game_id,
            stadium=stadium,
            home_score=home_score,
            away_score=away_score,
            game_status=game_status,
            season_id=season_id,
            game_date=date(2025, 1, 1),
        )
    )
    session.commit()


class TestParkFactorCalculator:
    def test_returns_empty_when_no_games(self, session):
        _add_season(session)
        calc = ParkFactorCalculator(session)
        assert calc.calculate(2025) == []

    def test_single_stadium(self, session):
        _add_season(session)
        _add_game(session)
        calc = ParkFactorCalculator(session)
        results = calc.calculate(2025)
        assert len(results) == 1
        r = results[0]
        assert r["stadium"] == "잠실"
        assert r["total_runs"] == 8
        assert r["games"] == 1
        assert r["runs_per_game"] == 8.0
        assert r["park_factor"] == 1.0

    def test_two_stadiums_different_pf(self, session):
        _add_season(session)
        # 잠실: 5+3=8 runs, 1 game
        _add_game(session, game_id="20250101", stadium="잠실", home_score=5, away_score=3)
        # 사직: 1+1=2 runs, 1 game
        _add_game(session, game_id="20250102", stadium="사직", home_score=1, away_score=1)

        calc = ParkFactorCalculator(session)
        results = calc.calculate(2025)
        assert len(results) == 2
        # league_avg_rpg = (8 + 2) / 2 = 5.0
        # 잠실 PF = 8.0 / 5.0 = 1.6
        # 사직 PF = 2.0 / 5.0 = 0.4
        by_stadium = {r["stadium"]: r for r in results}
        assert by_stadium["잠실"]["park_factor"] == pytest.approx(1.6, abs=0.001)
        assert by_stadium["사직"]["park_factor"] == pytest.approx(0.4, abs=0.001)

    def test_filters_non_regular_season(self, session):
        _add_season(session, league_type="POSTSEASON")
        _add_game(session)
        calc = ParkFactorCalculator(session)
        results = calc.calculate(2025)
        assert results == []

    def test_filters_incomplete_games(self, session):
        _add_season(session)
        _add_game(session, game_id="20250101", game_status="INPROGRESS")
        calc = ParkFactorCalculator(session)
        assert calc.calculate(2025) == []

    def test_handles_null_scores(self, session):
        _add_season(session)
        _add_game(session, game_id="20250101", home_score=None)
        calc = ParkFactorCalculator(session)
        assert calc.calculate(2025) == []

    def test_label_hitter_friendly(self, session):
        calc = ParkFactorCalculator(session)
        assert calc._label(1.15) == "타자친화"

    def test_label_slightly_hitter(self, session):
        calc = ParkFactorCalculator(session)
        assert calc._label(1.07) == "약간 타자친화"

    def test_label_neutral(self, session):
        calc = ParkFactorCalculator(session)
        assert calc._label(1.0) == "중립"

    def test_label_slightly_pitcher(self, session):
        calc = ParkFactorCalculator(session)
        assert calc._label(0.93) == "약간 투수친화"

    def test_label_pitcher_friendly(self, session):
        calc = ParkFactorCalculator(session)
        assert calc._label(0.85) == "투수친화"

    def test_label_boundaries(self, session):
        calc = ParkFactorCalculator(session)
        assert calc._label(1.1001) == "타자친화"
        assert calc._label(1.10) == "약간 타자친화"  # boundary
        assert calc._label(1.0401) == "약간 타자친화"
        assert calc._label(1.04) == "중립"  # boundary
        assert calc._label(0.9601) == "중립"
        assert calc._label(0.96) == "약간 투수친화"  # boundary
        assert calc._label(0.9001) == "약간 투수친화"
        assert calc._label(0.90) == "투수친화"  # boundary


class TestParkFactorEdgeCases:
    def test_returns_empty_when_no_games(self, session):
        _add_season(session)
        calc = ParkFactorCalculator(session)
        assert calc.calculate(2025) == []

    def test_handles_null_stadium(self, session):
        _add_season(session)
        _add_game(session, game_id="20250101", stadium=None)
        calc = ParkFactorCalculator(session)
        results = calc.calculate(2025)
        assert len(results) == 1
        assert results[0]["stadium"] == "UNKNOWN"

    def test_label_neutral_boundary(self, session):
        calc = ParkFactorCalculator(session)
        assert calc._label(1.04) == "중립"
        assert calc._label(0.96) == "약간 투수친화"


class TestPrintReport:
    def test_returns_early_when_empty(self, session, caplog):
        _add_season(session)
        calc = ParkFactorCalculator(session)
        with caplog.at_level("INFO"):
            calc.print_report(2025)
        assert "파크팩터" not in caplog.text

    def test_outputs_report_when_results(self, session, caplog):
        _add_season(session)
        _add_game(session)
        calc = ParkFactorCalculator(session)
        with caplog.at_level("INFO"):
            calc.print_report(2025)
        assert "파크팩터" in caplog.text or "구장" in caplog.text
