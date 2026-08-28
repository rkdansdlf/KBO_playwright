"""Real-time live stream event processor and WPA/Leverage tracker."""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import TYPE_CHECKING

from src.notifications.dispatcher import NotificationDispatcher
from src.services.wpa_calculator import WPACalculator, WpaInput
from src.simulation.dto import SimulationEvent, SimulationSummary
from src.simulation.stream_generator import GameStreamGenerator

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

logger = logging.getLogger(__name__)

LATE_INNING_THRESHOLD = 7
MID_INNING_THRESHOLD = 4
HOT_MOMENT_WPA_THRESHOLD = 0.15
HOT_MOMENT_LI_THRESHOLD = 3.0
HIGH_LEVERAGE_BASE = 2.5
MED_LEVERAGE_BASE = 1.5
EARLY_LEVERAGE_BASE = 1.3
TWO_OUTS = 2
ONE_OUT = 1
RUNNER_1ST = 1
RUNNER_2ND = 2
RUNNER_3RD = 4


DIFF_TIER_1 = 1
DIFF_TIER_2 = 2
DIFF_TIER_3 = 3


class LiveStreamProcessor:
    """Processes simulated or real game streams in real-time with WPA and hot-moment detection."""

    def __init__(
        self,
        *,
        wpa_calculator: WPACalculator | None = None,
        notification_dispatcher: NotificationDispatcher | None = None,
    ) -> None:
        """Initialize live stream processor."""
        self.wpa_calc = wpa_calculator or WPACalculator()
        self.dispatcher = notification_dispatcher or NotificationDispatcher()

    @staticmethod
    def _compute_leverage_index(inning: int, score_diff: int, runners: int, outs: int) -> float:
        """Compute base leverage index heuristic from game context."""
        base_li = 1.0
        # High leverage late in close games
        if inning >= LATE_INNING_THRESHOLD:
            if abs(score_diff) <= DIFF_TIER_1:
                base_li = HIGH_LEVERAGE_BASE
            elif abs(score_diff) <= DIFF_TIER_3:
                base_li = MED_LEVERAGE_BASE
        elif inning >= MID_INNING_THRESHOLD and abs(score_diff) <= DIFF_TIER_2:
            base_li = EARLY_LEVERAGE_BASE

        # Multiplier based on runners in scoring position and outs
        runner_multiplier = 1.0
        if runners & RUNNER_3RD:
            runner_multiplier += 0.8
        if runners & RUNNER_2ND:
            runner_multiplier += 0.5
        if runners & RUNNER_1ST:
            runner_multiplier += 0.2

        out_factor = 1.2 if outs == TWO_OUTS else (1.0 if outs == ONE_OUT else 0.9)
        return round(base_li * runner_multiplier * out_factor, 2)

    def process_stream(  # noqa: PLR0913
        self,
        events: Sequence[SimulationEvent] | None = None,
        *,
        game_id: str = "20260401LGHT0",
        home_team: str = "KIA",
        away_team: str = "LG",
        speed_multiplier: float = 0.0,
        notify_hot_moments: bool = False,
        event_callback: Callable[[SimulationEvent], None] | None = None,
    ) -> SimulationSummary:
        """Process event stream, compute live WPA/LI, detect hot moments, and return summary."""
        t0 = time.perf_counter()

        if events is None:
            generator = GameStreamGenerator(home_team=home_team, away_team=away_team)
            events = generator.generate_game_stream(game_id=game_id)

        processed_events: list[SimulationEvent] = []
        player_wpa: dict[str, float] = defaultdict(float)
        hot_moments_count = 0

        logger.info(
            "Processing live simulation stream for game %s (%d events, speed=%.1fx)",
            game_id,
            len(events),
            speed_multiplier,
        )

        for ev in events:
            # Calculate WPA
            score_diff_before = ev.home_score - ev.away_score
            score_diff_after = (
                (ev.home_score + ev.runs_scored - ev.away_score)
                if ev.is_bottom
                else (ev.home_score - (ev.away_score + ev.runs_scored))
            )

            wpa_in = WpaInput(
                inning=ev.inning,
                is_bottom=ev.is_bottom,
                outs_before=ev.outs_before,
                runners_before=ev.runners_before,
                score_diff_before=score_diff_before,
                outs_after=ev.outs_after,
                runners_after=ev.runners_after,
                score_diff_after=score_diff_after,
            )

            p_before = self.wpa_calc.get_win_probability(
                ev.inning,
                is_bottom=ev.is_bottom,
                outs=ev.outs_before,
                runners=ev.runners_before,
                score_diff=score_diff_before,
            )
            p_after = self.wpa_calc.get_win_probability(
                ev.inning,
                is_bottom=ev.is_bottom,
                outs=ev.outs_after,
                runners=ev.runners_after,
                score_diff=score_diff_after,
            )
            delta_wpa = self.wpa_calc.calculate_wpa(data=wpa_in)
            li = self._compute_leverage_index(
                ev.inning,
                score_diff_before,
                ev.runners_before,
                ev.outs_before,
            )

            # Assign metrics to event
            ev.win_prob_before = p_before
            ev.win_prob_after = p_after
            ev.wpa = delta_wpa
            ev.leverage_index = li

            # Hot moment threshold: WPA shift >= 15% OR LI >= 3.0 OR walk-off
            is_hot = (
                abs(delta_wpa) >= HOT_MOMENT_WPA_THRESHOLD
                or li >= HOT_MOMENT_LI_THRESHOLD
                or ("끝내기" in ev.description)
            )
            ev.is_hot_moment = is_hot

            if is_hot:
                hot_moments_count += 1
                if notify_hot_moments:
                    half_text = "말" if ev.is_bottom else "초"
                    notif_body = (
                        f"{ev.description} (WPA: {delta_wpa:+.3f}, LI: {li:.2f}) | "
                        f"{away_team} {ev.away_score} - {ev.home_score} {home_team}"
                    )
                    self.dispatcher.dispatch(
                        title=f"🔥 [KBO 핫모먼트] {ev.inning}회{half_text} {ev.batter_name}",
                        body=notif_body,
                        channel="console",
                        priority="high",
                    )

            # Track cumulative WPA per player
            player_wpa[ev.batter_name] += delta_wpa

            if event_callback is not None:
                event_callback(ev)

            processed_events.append(ev)

            # Simulation pacing delay
            if speed_multiplier > 0:
                time.sleep(max(0.001, 0.05 / speed_multiplier))

        duration_sec = time.perf_counter() - t0

        # Calculate Hero and Goat
        hero_name = "N/A"
        hero_score = 0.0
        goat_name = "N/A"
        goat_score = 0.0

        if player_wpa:
            hero_name, hero_score = max(player_wpa.items(), key=lambda x: x[1])
            goat_name, goat_score = min(player_wpa.items(), key=lambda x: x[1])

        last_event = processed_events[-1] if processed_events else None
        final_home = last_event.home_score if last_event else 0
        final_away = last_event.away_score if last_event else 0
        final_score_str = f"{away_team} {final_away} - {final_home} {home_team}"

        winner = home_team if final_home > final_away else (away_team if final_away > final_home else "무승부")

        summary = SimulationSummary(
            game_id=game_id,
            home_team=home_team,
            away_team=away_team,
            final_score=final_score_str,
            winner=winner,
            total_innings=last_event.inning if last_event else 9,
            total_events=len(processed_events),
            hot_moments_count=hot_moments_count,
            hero_player=hero_name,
            hero_wpa=hero_score,
            goat_player=goat_name,
            goat_wpa=goat_score,
            duration_seconds=duration_sec,
            events=processed_events,
        )

        logger.info(
            "Simulation finished: %s (Winner: %s) | Hot moments: %d | Hero: %s (%.3f)",
            final_score_str,
            winner,
            hot_moments_count,
            hero_name,
            hero_score,
        )

        return summary


__all__ = ["LiveStreamProcessor"]
