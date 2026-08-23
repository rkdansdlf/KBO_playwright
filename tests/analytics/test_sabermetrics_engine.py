"""Unit tests for src.analytics.sabermetrics."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.analytics.dto import LeagueConstants
from src.analytics.sabermetrics import SabermetricsEngine
from src.models.base import Base
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    sess = session_factory()
    try:
        yield sess
    finally:
        sess.close()


def test_calculate_batting_metrics() -> None:
    consts = LeagueConstants(
        year=2025,
        level="KBO1",
        woba_scale=1.25,
        league_woba=0.330,
        runs_per_pa=0.12,
        runs_per_win=10.0,
    )
    engine = SabermetricsEngine(session=None)  # type: ignore[arg-type]

    stats = SimpleNamespace(
        player_id=12345,
        season=2025,
        plate_appearances=100,
        at_bats=85,
        hits=25,
        doubles=5,
        triples=0,
        home_runs=5,
        walks=12,
        intentional_walks=1,
        hbp=2,
        sacrifice_flies=1,
        strikeouts=15,
    )

    metrics = engine.calculate_batting_metrics(stats, consts)
    assert metrics.player_id == 12345
    assert metrics.plate_appearances == 100
    assert metrics.woba > 0.350
    assert metrics.wraa > 0
    assert metrics.wrc_plus > 100
    assert metrics.iso == pytest.approx(20 / 85, rel=1e-2)
    assert metrics.war > 0


def test_calculate_pitching_metrics() -> None:
    consts = LeagueConstants(
        year=2025,
        level="KBO1",
        league_era=4.00,
        fip_constant=3.80,
        runs_per_win=10.0,
    )
    engine = SabermetricsEngine(session=None)  # type: ignore[arg-type]

    stats = SimpleNamespace(
        player_id=67890,
        season=2025,
        innings_outs=150,  # 50.0 IP
        earned_runs=15,
        hits_allowed=40,
        home_runs_allowed=3,
        walks_allowed=15,
        intentional_walks_allowed=0,
        hit_batters=2,
        strikeouts=50,
    )

    metrics = engine.calculate_pitching_metrics(stats, consts)
    assert metrics.player_id == 67890
    assert metrics.innings_pitched == 50.0
    assert metrics.era == pytest.approx(2.70, rel=1e-2)
    assert metrics.fip < 4.00
    assert metrics.whip == pytest.approx(1.10, rel=1e-2)
    assert metrics.war > 0


def test_calculate_season_sabermetrics(db_session) -> None:
    # Seed 2 players in test DB
    b1 = PlayerSeasonBatting(
        player_id=10001,
        season=2025,
        level="KBO1",
        team_code="LG",
        plate_appearances=100,
        at_bats=90,
        hits=30,
        doubles=5,
        triples=1,
        home_runs=4,
        walks=8,
        strikeouts=12,
        runs=15,
    )
    p1 = PlayerSeasonPitching(
        player_id=10002,
        season=2025,
        level="KBO1",
        team_code="LG",
        innings_outs=90,  # 30 IP
        earned_runs=10,
        home_runs_allowed=2,
        walks_allowed=8,
        strikeouts=25,
        runs_allowed=12,
    )
    db_session.add_all([b1, p1])
    db_session.flush()

    engine = SabermetricsEngine(db_session)
    res = engine.calculate_season_sabermetrics(2025, level="KBO1")

    assert res["batting_updated"] == 1
    assert res["pitching_updated"] == 1

    db_session.refresh(b1)
    db_session.refresh(p1)
    assert b1.woba is not None
    assert p1.fip is not None
