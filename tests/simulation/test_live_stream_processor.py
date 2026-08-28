"""Unit tests for LiveStreamProcessor."""

from __future__ import annotations

from unittest.mock import MagicMock

from src.notifications.dispatcher import NotificationDispatcher
from src.services.wpa_calculator import WPACalculator
from src.simulation.dto import SimulationEvent
from src.simulation.live_stream_processor import LiveStreamProcessor


def test_process_stream_wpa_and_hot_moment() -> None:
    """Test real-time stream processing with WPA calculation and hot moment detection."""
    events = [
        SimulationEvent(
            event_seq=1,
            inning=9,
            is_bottom=True,
            batter_name="김도영",
            pitcher_name="유영찬",
            result_type="HOMERUN",
            description="끝내기 역전 투런 홈런!",
            outs_before=2,
            runners_before=1,
            outs_after=2,
            runners_after=0,
            runs_scored=2,
            home_score=4,
            away_score=3,
        )
    ]

    mock_wpa_calc = MagicMock(spec=WPACalculator)
    mock_wpa_calc.calculate_wpa.return_value = 0.80
    mock_wpa_calc.get_win_probability.side_effect = [0.20, 1.00]

    mock_dispatcher = MagicMock(spec=NotificationDispatcher)

    processor = LiveStreamProcessor(
        wpa_calculator=mock_wpa_calc,
        notification_dispatcher=mock_dispatcher,
    )

    received_events = []
    summary = processor.process_stream(
        events=events,
        game_id="20260401LGHT0",
        home_team="KIA",
        away_team="LG",
        notify_hot_moments=True,
        event_callback=received_events.append,
    )

    assert len(received_events) == 1
    assert received_events[0].wpa == 0.80
    assert received_events[0].is_hot_moment is True

    # Notification dispatched for hot moment
    mock_dispatcher.dispatch.assert_called_once()

    assert summary.winner == "KIA"
    assert summary.hot_moments_count == 1
    assert summary.hero_player == "김도영"
    assert summary.hero_wpa == 0.80


def test_compute_leverage_index() -> None:
    """Test leverage index heuristic calculations."""
    # Late inning close game with runners in scoring position
    li_high = LiveStreamProcessor._compute_leverage_index(
        inning=9,
        score_diff=0,
        runners=6,  # 2nd and 3rd
        outs=2,
    )
    assert li_high >= 3.0

    # Early inning blowout
    li_low = LiveStreamProcessor._compute_leverage_index(
        inning=2,
        score_diff=8,
        runners=0,
        outs=0,
    )
    assert li_low <= 1.5
