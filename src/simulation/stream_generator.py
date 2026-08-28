"""Game stream generator for producing realistic live KBO game events."""

from __future__ import annotations

import logging
import random
from typing import TYPE_CHECKING

from src.simulation.dto import SimulationEvent, SimulationGameState

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

DEFAULT_HOME_LINEUP = ["박찬호", "김선빈", "김도영", "나성범", "소크라테스", "최형우", "이우성", "김태군", "최원준"]
DEFAULT_AWAY_LINEUP = ["홍창기", "신민재", "오스틴", "문보경", "오지환", "김현수", "박동원", "박해민", "문성주"]

OUTCOME_PROBABILITIES: list[tuple[float, str]] = [
    (0.22, "STRIKEOUT"),
    (0.44, "GROUNDOUT"),
    (0.62, "FLYOUT"),
    (0.72, "WALK"),
    (0.88, "SINGLE"),
    (0.95, "DOUBLE"),
    (0.96, "TRIPLE"),
    (1.00, "HOMERUN"),
]

RUNNER_1ST = 1
RUNNER_2ND = 2
RUNNER_3RD = 4
BASES_LOADED = 7
REGULATION_INNINGS = 9
OUTS_PER_HALF = 3
TWO_OUTS = 2
STARTER_INNING_LIMIT = 6
SETUP_INNING_LIMIT = 8
DOUBLE_PLAY_CHANCE = 0.08


class GameStreamGenerator:
    """Generates a realistic stream of baseball play-by-play events."""

    def __init__(
        self,
        *,
        home_team: str = "KIA",
        away_team: str = "LG",
        home_lineup: Sequence[str] | None = None,
        away_lineup: Sequence[str] | None = None,
        seed: int | None = None,
    ) -> None:
        """Initialize game stream generator with team names, lineups, and RNG seed."""
        self.home_team = home_team
        self.away_team = away_team
        self.home_lineup = list(home_lineup or DEFAULT_HOME_LINEUP)
        self.away_lineup = list(away_lineup or DEFAULT_AWAY_LINEUP)
        self.rng = random.Random(seed)  # noqa: S311

    def _resolve_play_outcome(self, runners: int, outs: int) -> str:
        """Sample a realistic plate appearance outcome."""
        r = self.rng.random()

        # Opportunity for double play if runner on 1st and < 2 outs
        if (runners & RUNNER_1ST) and outs < TWO_OUTS and r < DOUBLE_PLAY_CHANCE:
            return "DOUBLE_PLAY"

        for threshold, outcome in OUTCOME_PROBABILITIES:
            if r < threshold:
                return outcome
        return "HOMERUN"

    @staticmethod
    def _advance_runners(  # noqa: C901, PLR0912, PLR0915
        runners_before: int,
        outs_before: int,
        outcome: str,
        batter_name: str,
    ) -> tuple[int, int, int, str]:
        """Compute runner advancement, outs, runs, and description."""
        r1 = bool(runners_before & RUNNER_1ST)
        r2 = bool(runners_before & RUNNER_2ND)
        r3 = bool(runners_before & RUNNER_3RD)

        runners_after = 0
        outs_after = outs_before
        runs_scored = 0
        desc = ""

        if outcome == "STRIKEOUT":
            outs_after = outs_before + 1
            runners_after = runners_before
            desc = f"{batter_name} 헛스윙 삼진 아웃"

        elif outcome == "FLYOUT":
            outs_after = outs_before + 1
            if r3 and outs_before < TWO_OUTS:
                runs_scored += 1
                runners_after = (RUNNER_2ND if r2 else 0) | (RUNNER_1ST if r1 else 0)
                desc = f"{batter_name} 우익수 희생플라이 (3루 주자 득점)"
            else:
                runners_after = runners_before
                desc = f"{batter_name} 외야 뜬공 아웃"

        elif outcome == "GROUNDOUT":
            outs_after = outs_before + 1
            if outs_before < TWO_OUTS:
                if r3:
                    runs_scored += 1
                runners_after = (RUNNER_3RD if r2 else 0) | (RUNNER_2ND if r1 else 0)
                desc = f"{batter_name} 내야 땅볼 아웃 (주자 진루)"
            else:
                runners_after = runners_before
                desc = f"{batter_name} 내야 땅볼 아웃"

        elif outcome == "DOUBLE_PLAY":
            outs_after = min(OUTS_PER_HALF, outs_before + 2)
            if r3 and outs_before == 0:
                runs_scored += 1
            runners_after = RUNNER_3RD if r2 else 0
            desc = f"{batter_name} 6-4-3 병살타 아웃"

        elif outcome == "WALK":
            outs_after = outs_before
            if r1 and r2 and r3:
                runs_scored += 1
                runners_after = BASES_LOADED
            elif r1 and r2:
                runners_after = BASES_LOADED
            elif r1:
                runners_after = (RUNNER_1ST | RUNNER_2ND) | (RUNNER_3RD if r3 else 0)
            else:
                runners_after = RUNNER_1ST | (RUNNER_2ND if r2 else 0) | (RUNNER_3RD if r3 else 0)
            desc = f"{batter_name} 볼넷 출루"

        elif outcome == "SINGLE":
            outs_after = outs_before
            if r3:
                runs_scored += 1
            if r2:
                runs_scored += 1
            runners_after = RUNNER_1ST | (RUNNER_2ND if r1 else 0)
            desc = f"{batter_name} 중전 1루타 안타"

        elif outcome == "DOUBLE":
            outs_after = outs_before
            runs_scored += int(r3) + int(r2) + int(r1)
            runners_after = RUNNER_2ND
            desc = f"{batter_name} 좌중간 2루타"

        elif outcome == "TRIPLE":
            outs_after = outs_before
            runs_scored += int(r1) + int(r2) + int(r3)
            runners_after = RUNNER_3RD
            desc = f"{batter_name} 우익선상 3루타"

        elif outcome == "HOMERUN":
            outs_after = outs_before
            runs_scored = 1 + int(r1) + int(r2) + int(r3)
            runners_after = 0
            desc = f"{batter_name} 비거리 125m 홈런!"

        return runners_after, outs_after, runs_scored, desc

    def generate_game_stream(
        self,
        game_id: str = "20260401LGHT0",
        max_innings: int = 9,
    ) -> list[SimulationEvent]:
        """Generate a sequential list of simulation events for a full game."""
        state = SimulationGameState(
            game_id=game_id,
            home_team=self.home_team,
            away_team=self.away_team,
        )

        events: list[SimulationEvent] = []
        home_idx = 0
        away_idx = 0
        event_seq = 1

        for inning in range(1, max_innings + 1):
            # Top half (Away team batting)
            state.current_inning = inning
            state.is_bottom = False
            state.outs = 0
            state.runners = 0

            pitcher_home = (
                "양현종" if inning <= STARTER_INNING_LIMIT else ("전상현" if inning <= SETUP_INNING_LIMIT else "정해영")
            )

            while state.outs < OUTS_PER_HALF:
                batter_away = self.away_lineup[away_idx % len(self.away_lineup)]
                away_idx += 1

                outcome = self._resolve_play_outcome(state.runners, state.outs)
                runners_after, outs_after, runs, desc = self._advance_runners(
                    state.runners,
                    state.outs,
                    outcome,
                    batter_away,
                )

                state.away_score += runs

                ev = SimulationEvent(
                    event_seq=event_seq,
                    inning=inning,
                    is_bottom=False,
                    batter_name=batter_away,
                    pitcher_name=pitcher_home,
                    result_type=outcome,
                    description=desc,
                    outs_before=state.outs,
                    runners_before=state.runners,
                    outs_after=outs_after,
                    runners_after=runners_after,
                    runs_scored=runs,
                    home_score=state.home_score,
                    away_score=state.away_score,
                )
                events.append(ev)
                event_seq += 1

                state.outs = outs_after
                state.runners = runners_after if state.outs < OUTS_PER_HALF else 0

            # Check if 9th inning top is done and home team is winning (No bottom 9th needed)
            if inning == REGULATION_INNINGS and state.home_score > state.away_score:
                state.is_finished = True
                break

            # Bottom half (Home team batting)
            state.is_bottom = True
            state.outs = 0
            state.runners = 0

            pitcher_away = (
                "엔스" if inning <= STARTER_INNING_LIMIT else ("김진성" if inning <= SETUP_INNING_LIMIT else "유영찬")
            )

            while state.outs < OUTS_PER_HALF:
                batter_home = self.home_lineup[home_idx % len(self.home_lineup)]
                home_idx += 1

                outcome = self._resolve_play_outcome(state.runners, state.outs)
                runners_after, outs_after, runs, desc = self._advance_runners(
                    state.runners,
                    state.outs,
                    outcome,
                    batter_home,
                )

                state.home_score += runs

                ev = SimulationEvent(
                    event_seq=event_seq,
                    inning=inning,
                    is_bottom=True,
                    batter_name=batter_home,
                    pitcher_name=pitcher_away,
                    result_type=outcome,
                    description=desc,
                    outs_before=state.outs,
                    runners_before=state.runners,
                    outs_after=outs_after,
                    runners_after=runners_after,
                    runs_scored=runs,
                    home_score=state.home_score,
                    away_score=state.away_score,
                )
                events.append(ev)
                event_seq += 1

                state.outs = outs_after
                state.runners = runners_after if state.outs < OUTS_PER_HALF else 0

                # Walk-off check in 9th+ inning
                if inning >= REGULATION_INNINGS and state.home_score > state.away_score:
                    ev.description += " [끝내기 승리!]"
                    state.is_finished = True
                    break

            if state.is_finished:
                break

        logger.info(
            "Generated simulation game %s: %d innings, %d plays, final %s %d - %d %s",
            game_id,
            inning,
            len(events),
            self.away_team,
            state.away_score,
            state.home_score,
            self.home_team,
        )
        return events


__all__ = ["DEFAULT_AWAY_LINEUP", "DEFAULT_HOME_LINEUP", "GameStreamGenerator"]
