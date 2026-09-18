"""Regression tests for WpaChartService timeline assembly."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.game import Game, GameEvent
from src.services.wpa_chart_service import WpaChartService


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


def test_timeline_from_events_preserves_values(db_session) -> None:
    db_session.add(
        Game(
            game_id="20250504LGSS0",
            game_date=date(2025, 5, 4),
            home_team="SS",
            away_team="LG",
            game_status="COMPLETED",
        )
    )
    db_session.add(
        GameEvent(
            game_id="20250504LGSS0",
            inning=1,
            inning_half="T",
            event_seq=1,
            batter_name="김도영",
            pitcher_name="양현종",
            description="우전 안타",
            win_expectancy_after=0.55,
            wpa=0.05,
            home_score=0,
            away_score=0,
        )
    )
    db_session.flush()

    chart = WpaChartService(db_session).get_game_wpa_chart("20250504LGSS0")
    assert chart is not None
    assert len(chart["timeline"]) == 1
    item = chart["timeline"][0]
    assert item["event_seq"] == 1
    assert item["inning"] == 1
    assert item["batter_name"] == "김도영"
    assert item["home_win_prob"] == pytest.approx(0.55)
    assert item["wpa"] == pytest.approx(0.05)


def test_missing_game_returns_none(db_session) -> None:
    assert WpaChartService(db_session).get_game_wpa_chart("20990101XXXX0") is None
