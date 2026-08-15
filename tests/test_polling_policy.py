"""Unit tests for AdaptivePollingEngine."""

from __future__ import annotations

from src.utils.polling_policy import AdaptivePollingEngine, PollingPolicyConfig


def test_polling_decision_before_game() -> None:
    """Before game should poll at long interval."""
    engine = AdaptivePollingEngine()
    decision = engine.evaluate("before")
    assert decision.should_poll is True
    assert decision.interval_seconds == 300
    assert decision.is_high_leverage is False


def test_polling_decision_running_standard() -> None:
    """Standard running game should poll at 15s."""
    engine = AdaptivePollingEngine()
    context = {"inning": 3, "home_score": 1, "away_score": 0}
    decision = engine.evaluate("running", game_context=context)
    assert decision.should_poll is True
    assert decision.interval_seconds == 15
    assert decision.is_high_leverage is False


def test_polling_decision_running_high_leverage() -> None:
    """Late inning (8th) with 1-run difference should trigger high leverage (5s)."""
    engine = AdaptivePollingEngine()
    context = {"inning": 8, "home_score": 3, "away_score": 2}
    decision = engine.evaluate("running", game_context=context)
    assert decision.should_poll is True
    assert decision.interval_seconds == 5
    assert decision.is_high_leverage is True
    assert "High-leverage" in decision.reason


def test_polling_decision_rain_delay() -> None:
    """Rain delay or suspension should relax interval to 120s."""
    engine = AdaptivePollingEngine()
    decision = engine.evaluate("delayed")
    assert decision.should_poll is True
    assert decision.interval_seconds == 120
    assert decision.is_high_leverage is False


def test_polling_decision_terminal_states() -> None:
    """Final and cancelled games should cease polling."""
    engine = AdaptivePollingEngine()
    for terminal in ("final", "cancelled"):
        decision = engine.evaluate(terminal)
        assert decision.should_poll is False
        assert decision.interval_seconds == 0


def test_polling_custom_config() -> None:
    """Custom configuration overrides default intervals."""
    custom = PollingPolicyConfig(interval_running=10, interval_high_leverage=3)
    engine = AdaptivePollingEngine(custom)
    decision = engine.evaluate("running", game_context={"inning": 9, "home_score": 4, "away_score": 4})
    assert decision.interval_seconds == 3
