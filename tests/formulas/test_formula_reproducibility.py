"""Tests for System-Wide Formula Reproducibility Audit."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.formulas.engine import FormulaEngine
from src.formulas.models import MetricCategory
from src.models.base import Base
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def test_db() -> Generator[Any, None, None]:
    """Create populated in-memory test database."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as s:
        p1 = PlayerBasic(player_id=101, name="타자1", team="LG")
        p2 = PlayerBasic(player_id=102, name="투수1", team="OB")
        s.add_all([p1, p2])

        bat = PlayerSeasonBatting(
            player_id=101,
            season=2024,
            team_code="LG",
            level="1군",
            plate_appearances=100,
            at_bats=80,
            hits=24,
            doubles=4,
            triples=0,
            home_runs=2,
            walks=15,
            hbp=3,
            strikeouts=10,
            sacrifice_flies=2,
            avg=0.300,
            obp=0.420,
            slg=0.425,
            ops=0.845,
        )
        pit = PlayerSeasonPitching(
            player_id=102,
            season=2024,
            team_code="OB",
            level="1군",
            earned_runs=10,
            innings_outs=90,
            hits_allowed=25,
            walks_allowed=8,
            hit_batters=1,
            strikeouts=30,
            home_runs_allowed=2,
            era=3.00,
            whip=1.10,
        )
        s.add_all([bat, pit])
        s.commit()

    yield engine
    engine.dispose()


def test_audit_reproducibility_batting(test_db) -> None:
    """Audit reproducibility on batting category."""
    engine = FormulaEngine(engine=test_db)
    report = engine.audit_reproducibility(season=2024, category=MetricCategory.BATTING, sample=10)

    assert report.total_entities_checked > 0
    assert report.reproducible_count > 0
    assert report.is_compliant is True
    assert report.git_sha != ""
    assert report.sha256_checksum != ""
