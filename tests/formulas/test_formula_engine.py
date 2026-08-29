"""Tests for FormulaEngine evaluation and math invariance."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.formulas.engine import FormulaEngine
from src.models.base import Base
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def mem_engine() -> Generator[Any, None, None]:
    """Provide isolated in-memory SQLite engine for tests."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as s:
        # Seed test player and season batting
        p = PlayerBasic(player_id=75847, name="최정", team="SSG", position="내야수")
        s.add(p)
        bat = PlayerSeasonBatting(
            player_id=75847,
            season=2024,
            team_code="SSG",
            level="1군",
            plate_appearances=500,
            at_bats=400,
            hits=120,
            doubles=25,
            triples=1,
            home_runs=35,
            walks=60,
            hbp=10,
            strikeouts=90,
            sacrifice_flies=5,
            avg=0.300,
            obp=0.400,
            slg=0.630,
            ops=1.030,
        )
        s.add(bat)

        # Pitcher
        p2 = PlayerBasic(player_id=60558, name="류현진", team="한화", position="투수")
        s.add(p2)
        pit = PlayerSeasonPitching(
            player_id=60558,
            season=2024,
            team_code="HH",
            level="1군",
            earned_runs=60,
            innings_outs=450,
            hits_allowed=140,
            walks_allowed=30,
            hit_batters=4,
            strikeouts=130,
            home_runs_allowed=12,
            era=3.60,
            whip=1.13,
        )
        s.add(pit)
        s.commit()

    yield engine
    engine.dispose()


def test_evaluate_player_metric_batting(mem_engine) -> None:
    """Evaluate player metric on in-memory database."""
    engine = FormulaEngine(engine=mem_engine)

    res_avg = engine.evaluate_player_metric("최정", 2024, "AVG")
    assert res_avg.calculated_value == 0.300
    assert res_avg.is_reproducible is True
    assert res_avg.invariants_passed is True

    res_slg = engine.evaluate_player_metric(75847, 2024, "SLG")
    assert res_slg.calculated_value == 0.630
    assert res_slg.is_reproducible is True


def test_evaluate_player_metric_pitching(mem_engine) -> None:
    """Evaluate pitching metric on in-memory database."""
    engine = FormulaEngine(engine=mem_engine)

    res_era = engine.evaluate_player_metric("류현진", 2024, "ERA")
    assert res_era.calculated_value == 3.60
    assert res_era.is_reproducible is True
    assert res_era.invariants_passed is True
