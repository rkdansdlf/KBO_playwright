"""CLI command for running KBO live game event simulations."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import TYPE_CHECKING

from src.simulation.live_stream_processor import LiveStreamProcessor
from src.simulation.stream_generator import GameStreamGenerator

if TYPE_CHECKING:
    from collections.abc import Sequence

    from src.simulation.dto import SimulationEvent

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    """Execute live game simulation CLI."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Simulate KBO Live Game Event Stream & WPA Calculation")
    parser.add_argument("--game-id", type=str, default="20260401LGHT0", help="Game ID identifier")
    parser.add_argument("--home-team", type=str, default="KIA", help="Home team code/name")
    parser.add_argument("--away-team", type=str, default="LG", help="Away team code/name")
    parser.add_argument("--innings", type=int, default=9, help="Max regulation innings")
    parser.add_argument("--speed", type=float, default=0.0, help="Simulation speed multiplier (0 = instant)")
    parser.add_argument("--notify", action="store_true", help="Dispatch hot moment alerts")
    parser.add_argument("--seed", type=int, default=None, help="RNG seed for deterministic playback")
    parser.add_argument("--json", action="store_true", help="Output summary in JSON format")

    args = parser.parse_args(argv)

    generator = GameStreamGenerator(
        home_team=args.home_team,
        away_team=args.away_team,
        seed=args.seed,
    )
    events = generator.generate_game_stream(game_id=args.game_id, max_innings=args.innings)

    if not args.json:
        print("=" * 75)  # noqa: T201
        print(f"⚾ [KBO 라이브 경기 시뮬레이션 시작]: {args.away_team} vs {args.home_team} ({args.game_id})")  # noqa: T201
        print("=" * 75)  # noqa: T201

    def _on_event(ev: SimulationEvent) -> None:
        if not args.json:
            half_str = "말" if ev.is_bottom else "초"
            hot_badge = " 🔥[HOT]" if ev.is_hot_moment else ""
            print(  # noqa: T201
                f"[{ev.inning}회{half_str}] {ev.outs_before}아웃 | 주자:{ev.runners_before} -> "
                f"{ev.batter_name} (vs {ev.pitcher_name}): {ev.description} "
                f"(WPA: {ev.wpa:+.3f}, LI: {ev.leverage_index:.2f}){hot_badge} | "
                f"스코어: {args.away_team} {ev.away_score} - {ev.home_score} {args.home_team}"
            )

    processor = LiveStreamProcessor()
    summary = processor.process_stream(
        events=events,
        game_id=args.game_id,
        home_team=args.home_team,
        away_team=args.away_team,
        speed_multiplier=args.speed,
        notify_hot_moments=args.notify,
        event_callback=_on_event if not args.json else None,
    )

    if args.json:
        print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))  # noqa: T201
        return 0

    print("\n" + "=" * 75)  # noqa: T201
    print(f"🏁 [시뮬레이션 경기 종료]: 최종 스코어 {summary.final_score} (승리팀: {summary.winner})")  # noqa: T201
    print("=" * 75)  # noqa: T201
    print(f"• 총 진행 타석:    {summary.total_events}타석 ({summary.total_innings}이닝)")  # noqa: T201
    print(f"• 핫 모먼트 발생:  {summary.hot_moments_count}회")  # noqa: T201
    print(f"• 오늘의 영웅(MVP): {summary.hero_player} (WPA: {summary.hero_wpa:+.4f})")  # noqa: T201
    print(f"• 오늘의 역적:     {summary.goat_player} (WPA: {summary.goat_wpa:+.4f})")  # noqa: T201
    print(f"• 시뮬레이션 소요: {summary.duration_seconds:.2f}초")  # noqa: T201
    print("=" * 75)  # noqa: T201

    return 0


if __name__ == "__main__":
    sys.exit(main())
