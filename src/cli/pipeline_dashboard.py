"""CLI dashboard for comprehensive pipeline status and telemetry monitoring."""

from __future__ import annotations

import argparse
import contextlib
import sys
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from src.cli.auto_healer import _find_season_stat_discrepancies, _find_stuck_games
from src.db.engine import get_db_session, init_db
from src.models.game import Game, GameEvent, GamePlayByPlay
from src.models.player import PlayerBasic
from src.utils.lock import ProcessLock

if TYPE_CHECKING:
    from collections.abc import Sequence


def _check_lock(name: str) -> str:
    lock = ProcessLock(name, blocking=False)
    if lock.acquire():
        lock.release()
        return "FREE (Idle)"
    return "HELD (Running)"


def render_dashboard() -> str:
    """Generate a formatted terminal telemetry report."""
    with contextlib.suppress(Exception):
        init_db()

    lines = [
        "==================================================",
        "   KBO PLAYWRIGHT PIPELINE TELEMETRY DASHBOARD    ",
        "==================================================",
    ]

    # 1. DB Record Counts
    try:
        with get_db_session() as session:
            game_count = session.query(Game).count()
            player_count = session.query(PlayerBasic).count()
            event_count = session.query(GameEvent).count()
            pbp_count = session.query(GamePlayByPlay).count()
        lines.append("[Database Records]")
        lines.append(f"  - Games: {game_count:,}")
        lines.append(f"  - Players: {player_count:,}")
        lines.append(f"  - Events: {event_count:,}")
        lines.append(f"  - Play-By-Play: {pbp_count:,}")
    except (SQLAlchemyError, OSError) as e:
        lines.append(f"[Database Error]: {e}")

    lines.append("")

    # 2. Lock Status
    lines.append("[Process Locks]")
    lines.append(f"  - live_refresh lock: {_check_lock('live_refresh')}")
    lines.append(f"  - daily_update lock: {_check_lock('daily_update')}")
    lines.append(f"  - maintenance  lock: {_check_lock('maintenance')}")

    lines.append("")

    # 3. Auto-Healer Integrity
    try:
        stuck = _find_stuck_games()
        discrepant = _find_season_stat_discrepancies()
        lines.append("[Auto-Healer Integrity Status]")
        lines.append(f"  - Stuck Games: {len(stuck)}")
        lines.append(f"  - Discrepant Seasons: {len(discrepant)} ({discrepant or 'None'})")
        lines.append(
            "  - Integrity Rating: "
            f"{'HEALTHY (Clean)' if not stuck and not discrepant else 'ATTENTION NEEDED'}"
        )
    except (SQLAlchemyError, OSError) as e:
        lines.append(f"[Integrity Check Error]: {e}")

    lines.append("==================================================")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the main CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Pipeline telemetry dashboard")
    parser.parse_args(argv)

    report = render_dashboard()
    sys.stdout.write(report + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
