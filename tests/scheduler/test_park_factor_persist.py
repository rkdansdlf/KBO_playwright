"""Tests for persist_homerun_factors (weekly park factor job helper)."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.game import Game, GameBattingStat
from src.models.season import KboSeason
from src.models.stadium import Stadium
from src.scheduler.jobs.maintenance import persist_homerun_factors


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    with factory() as sess:
        yield sess


def _seed(session) -> None:
    session.add(KboSeason(season_id=1, season_year=2025, league_type_code=1, league_type_name="정규시즌"))
    session.add(
        Game(
            game_id="20250101",
            stadium="잠실",
            home_score=5,
            away_score=3,
            game_status="COMPLETED",
            season_id=1,
            game_date=date(2025, 1, 1),
        )
    )
    session.add(Stadium(stadium_id="JAMSIL", stadium_name="잠실야구장"))
    session.add(
        GameBattingStat(game_id="20250101", team_side="home", player_name="테스트", appearance_seq=1, home_runs=2)
    )
    session.commit()


def test_persists_homerun_factor(session) -> None:
    _seed(session)
    assert persist_homerun_factors(session, 2025) == 1
    session.commit()
    row = session.get(Stadium, "JAMSIL")
    assert row is not None
    assert row.homerun_park_factor == pytest.approx(1.0, abs=0.001)


def test_returns_zero_without_homers(session) -> None:
    session.add(KboSeason(season_id=1, season_year=2025, league_type_code=1, league_type_name="정규시즌"))
    session.add(Stadium(stadium_id="JAMSIL", stadium_name="잠실야구장"))
    session.commit()
    assert persist_homerun_factors(session, 2025) == 0
