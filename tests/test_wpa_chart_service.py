"""Unit tests for WPA Chart Service."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

from src.models.game import Game, GameEvent, GameHighlight
from src.services.wpa_chart_service import WpaChartService


def test_wpa_chart_from_events() -> None:
    """Test generating WPA timeline from GameEvent rows."""
    mock_session = MagicMock()
    service = WpaChartService(mock_session)

    mock_game = Game(
        game_id="20260809LGKIA0",
        game_date=date(2026, 8, 9),
        home_team="KIA",
        away_team="LG",
        home_score=6,
        away_score=3,
        game_status="FINAL",
    )
    event1 = GameEvent(
        game_id="20260809LGKIA0",
        event_seq=1,
        inning=1,
        inning_half="top",
        batter_name="홍창기",
        pitcher_name="양현종",
        description="우전 안타",
        win_expectancy_before=0.50,
        win_expectancy_after=0.47,
        wpa=0.03,
        home_score=0,
        away_score=0,
    )
    event2 = GameEvent(
        game_id="20260809LGKIA0",
        event_seq=2,
        inning=7,
        inning_half="bottom",
        batter_name="김도영",
        pitcher_name="켈리",
        description="역전 3점 홈런",
        win_expectancy_before=0.45,
        win_expectancy_after=0.82,
        wpa=0.37,
        home_score=6,
        away_score=3,
    )

    mock_session.execute.side_effect = [
        MagicMock(scalar_one_or_none=MagicMock(return_value=mock_game)),
        MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[event1, event2])))),
    ]

    chart = service.get_game_wpa_chart("20260809LGKIA0", top_turning_points=2)

    assert chart is not None
    assert chart["game_id"] == "20260809LGKIA0"
    assert chart["home_team"] == "KIA"
    assert len(chart["timeline"]) == 2
    assert chart["timeline"][0]["home_win_prob"] == 0.47
    assert chart["timeline"][1]["home_win_prob"] == 0.82

    # Turning points verification
    assert len(chart["turning_points"]) == 2
    assert chart["turning_points"][1]["impact_type"] == "GAME_CHANGER"
    assert chart["turning_points"][1]["batter_name"] == "김도영"


def test_get_game_highlights() -> None:
    """Test retrieving game highlights."""
    mock_session = MagicMock()
    service = WpaChartService(mock_session)

    mock_highlight = GameHighlight(
        id=1,
        game_id="20260809LGKIA0",
        event_seq=2,
        inning=7,
        inning_half="bottom",
        highlight_type="LEAD_CHANGE",
        description="김도영 역전 홈런",
        wpa=0.37,
        importance_score=0.95,
        tags=["홈런", "역전"],
    )

    mock_session.execute.return_value.scalars.return_value.all.return_value = [mock_highlight]

    highlights = service.get_game_highlights("20260809LGKIA0")
    assert len(highlights) == 1
    assert highlights[0]["highlight_type"] == "LEAD_CHANGE"
    assert highlights[0]["wpa"] == 0.37
    assert "홈런" in highlights[0]["tags"]
