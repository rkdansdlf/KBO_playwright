"""Unit tests for src.analytics.matchup."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.analytics.matchup import MatchupAnalyticsEngine
from src.models.base import Base
from src.models.game import Game, GameEvent


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


def test_calculate_bvp_matchups(db_session) -> None:
    g = Game(
        game_id="20250501LGSS0",
        game_date=date(2025, 5, 1),
        home_team="SS",
        away_team="LG",
        game_status="COMPLETED",
    )
    ev1 = GameEvent(
        game_id="20250501LGSS0",
        inning=1,
        inning_half="T",
        event_seq=1,
        batter_id=101,
        pitcher_id=201,
        description="우전 안타",
    )
    ev2 = GameEvent(
        game_id="20250501LGSS0",
        inning=3,
        inning_half="T",
        event_seq=2,
        batter_id=101,
        pitcher_id=201,
        description="좌월 홈런",
    )
    ev3 = GameEvent(
        game_id="20250501LGSS0",
        inning=5,
        inning_half="T",
        event_seq=3,
        batter_id=101,
        pitcher_id=201,
        description="헛스윙 삼진",
    )
    db_session.add_all([g, ev1, ev2, ev3])
    db_session.flush()

    engine = MatchupAnalyticsEngine(db_session)
    results = engine.calculate_bvp_matchups(2025)

    assert len(results) == 1
    m = results[0]
    assert m.batter_id == 101
    assert m.pitcher_id == 201
    assert m.plate_appearances == 3
    assert m.at_bats == 3
    assert m.hits == 2
    assert m.home_runs == 1
    assert m.strikeouts == 1
    assert m.avg == pytest.approx(2 / 3, rel=1e-2)
    assert m.slg == pytest.approx(5 / 3, rel=1e-2)


def test_calculate_situational_splits(db_session) -> None:
    g = Game(
        game_id="20250502LGSS0",
        game_date=date(2025, 5, 2),
        home_team="SS",
        away_team="LG",
        game_status="COMPLETED",
    )
    # RISP event (runner on 2B)
    ev1 = GameEvent(
        game_id="20250502LGSS0",
        inning=2,
        inning_half="B",
        event_seq=1,
        batter_id=102,
        pitcher_id=202,
        bases_before="010",
        description="우전 2루타 (1타점)",
        rbi=1,
    )
    # Non-RISP event (bases empty)
    ev2 = GameEvent(
        game_id="20250502LGSS0",
        inning=4,
        inning_half="B",
        event_seq=2,
        batter_id=102,
        pitcher_id=202,
        description="유격수 땅볼",
    )
    db_session.add_all([g, ev1, ev2])
    db_session.flush()

    engine = MatchupAnalyticsEngine(db_session)
    splits = engine.calculate_situational_splits(2025)

    assert len(splits) == 1
    s = splits[0]
    assert s.category == "risp"
    assert s.entity_id == 102
    assert s.sample_size == 1
    assert s.stats["h"] == 1
    assert s.stats["rbi"] == 1
