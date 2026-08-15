"""Adaptive Smart Polling Policy for KBO Crawlers.

Dynamically manages crawl intervals based on game lifecycle and leverage state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from src.utils.game_state import (
    RELAY_ACTIVE_STATES,
    TERMINAL_STATES,
    GameLifecycleState,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

_HIGH_LEVERAGE_MIN_INNING = 7
_HIGH_LEVERAGE_MAX_SCORE_DIFF = 2


@dataclass(frozen=True, slots=True)
class PollingPolicyConfig:
    """Configurable polling intervals in seconds for each state."""

    interval_before: int = 300  # 5 min
    interval_running: int = 15  # 15 sec
    interval_high_leverage: int = 5  # 5 sec (late inning close game)
    interval_delayed: int = 120  # 2 min (rain delay / suspension)
    interval_stabilization: int = 30  # 30 sec
    interval_terminal: int = 0  # no polling


@dataclass(frozen=True, slots=True)
class PollingDecision:
    """Calculated polling decision for a given game."""

    state: GameLifecycleState | str
    interval_seconds: int
    should_poll: bool
    is_high_leverage: bool
    reason: str


class AdaptivePollingEngine:
    """Engine that calculates next crawl interval dynamically based on game state."""

    def __init__(self, config: PollingPolicyConfig | None = None) -> None:
        """Initialize with optional custom configuration."""
        self.config = config or PollingPolicyConfig()

    def evaluate(
        self,
        state: GameLifecycleState | str,
        *,
        game_context: Mapping[str, Any] | None = None,
    ) -> PollingDecision:
        """Evaluate the given lifecycle state and game context to decide next crawl timing."""
        normalized_state = str(state).strip().lower()

        if normalized_state in TERMINAL_STATES:
            return PollingDecision(
                state=normalized_state,
                interval_seconds=self.config.interval_terminal,
                should_poll=False,
                is_high_leverage=False,
                reason="Game has concluded or was cancelled",
            )

        if normalized_state == "before":
            return PollingDecision(
                state=normalized_state,
                interval_seconds=self.config.interval_before,
                should_poll=True,
                is_high_leverage=False,
                reason="Pre-game waiting for lineups or first pitch",
            )

        if normalized_state in ("delayed", "suspended"):
            return PollingDecision(
                state=normalized_state,
                interval_seconds=self.config.interval_delayed,
                should_poll=True,
                is_high_leverage=False,
                reason="Game temporarily halted (rain/suspension); polling relaxed",
            )

        if normalized_state == "result_pending_stabilization":
            return PollingDecision(
                state=normalized_state,
                interval_seconds=self.config.interval_stabilization,
                should_poll=True,
                is_high_leverage=False,
                reason="Game finished but waiting for official boxscore stabilization",
            )

        # In-progress / Running states
        if normalized_state == "running" or normalized_state in RELAY_ACTIVE_STATES:
            high_leverage = self._is_high_leverage(game_context)
            interval = self.config.interval_high_leverage if high_leverage else self.config.interval_running
            reason = "High-leverage late inning close game" if high_leverage else "Standard live game progress"
            return PollingDecision(
                state=normalized_state,
                interval_seconds=interval,
                should_poll=True,
                is_high_leverage=high_leverage,
                reason=reason,
            )

        # Fallback for unknown state
        return PollingDecision(
            state=normalized_state,
            interval_seconds=self.config.interval_running,
            should_poll=True,
            is_high_leverage=False,
            reason=f"Unrecognized state '{normalized_state}', using running fallback",
        )

    def _is_high_leverage(self, context: Mapping[str, Any] | None) -> bool:
        """Determine if context indicates a close late-inning situation (7th+ inning, score diff <= 2)."""
        if not context:
            return False

        inning = context.get("inning") or context.get("current_inning")
        home_score = context.get("home_score")
        away_score = context.get("away_score")

        try:
            inn_val = int(inning) if inning is not None else 0
            if inn_val >= _HIGH_LEVERAGE_MIN_INNING and home_score is not None and away_score is not None:
                score_diff = abs(int(home_score) - int(away_score))
                return score_diff <= _HIGH_LEVERAGE_MAX_SCORE_DIFF
        except (ValueError, TypeError):
            return False

        return False


__all__ = ["AdaptivePollingEngine", "PollingDecision", "PollingPolicyConfig"]
