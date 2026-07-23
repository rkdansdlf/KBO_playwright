#!/usr/bin/env python3
"""Recover unresolved 2020 games in supervised ten-game batches."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import func, select

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.constants import KST
from src.db.engine import SessionLocal
from src.models.game import Game, GameBattingStat, GameLineup, GamePitchingStat

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)
DEFAULT_BATCH_SIZE = 10
DEFAULT_GAME_TIMEOUT_SECONDS = 300
MAX_ERROR_OUTPUT = 4000


@dataclass(frozen=True)
class GameSnapshot:
    """Capture the persisted data needed to validate one recovery."""

    game_id: str
    game_status: str | None
    away_score: int | None
    home_score: int | None
    batting_rows: int
    pitching_rows: int
    lineup_rows: int
    event_rows: int
    pbp_rows: int


@dataclass(frozen=True)
class GameRunResult:
    """Capture one recovery subprocess result."""

    game_id: str
    ok: bool
    return_code: int | None
    error: str | None
    before: GameSnapshot | None
    after: GameSnapshot | None


def load_unresolved_game_ids() -> list[str]:
    """Load unresolved 2020 game IDs in chronological order."""
    with SessionLocal() as session:
        rows = session.execute(
            select(Game.game_id)
            .where(
                Game.game_date >= "2020-01-01",
                Game.game_date < "2021-01-01",
                Game.game_status == "UNRESOLVED_MISSING",
            )
            .order_by(Game.game_date, Game.game_id),
        ).all()
    return [str(row[0]) for row in rows]


def load_game_snapshot(game_id: str) -> GameSnapshot | None:
    """Load persisted score and child-row counts for one game."""
    with SessionLocal() as session:
        game = session.execute(select(Game).where(Game.game_id == game_id)).scalar_one_or_none()
        if game is None:
            return None
        counts = {
            "batting_rows": session.scalar(
                select(func.count()).select_from(GameBattingStat).where(GameBattingStat.game_id == game_id),
            ),
            "pitching_rows": session.scalar(
                select(func.count()).select_from(GamePitchingStat).where(GamePitchingStat.game_id == game_id),
            ),
            "lineup_rows": session.scalar(
                select(func.count()).select_from(GameLineup).where(GameLineup.game_id == game_id),
            ),
        }
        from src.models.game import GameEvent, GamePlayByPlay

        counts["event_rows"] = session.scalar(
            select(func.count()).select_from(GameEvent).where(GameEvent.game_id == game_id),
        )
        counts["pbp_rows"] = session.scalar(
            select(func.count()).select_from(GamePlayByPlay).where(GamePlayByPlay.game_id == game_id),
        )
    return GameSnapshot(
        game_id=game_id,
        game_status=game.game_status,
        away_score=game.away_score,
        home_score=game.home_score,
        batting_rows=int(counts["batting_rows"] or 0),
        pitching_rows=int(counts["pitching_rows"] or 0),
        lineup_rows=int(counts["lineup_rows"] or 0),
        event_rows=int(counts["event_rows"] or 0),
        pbp_rows=int(counts["pbp_rows"] or 0),
    )


def recovery_succeeded(snapshot: GameSnapshot | None) -> bool:
    """Return whether a recovered game has score and core detail rows."""
    return bool(
        snapshot
        and snapshot.game_status != "UNRESOLVED_MISSING"
        and snapshot.away_score is not None
        and snapshot.home_score is not None
        and snapshot.batting_rows > 0
        and snapshot.pitching_rows > 0,
    )


def _run_compose(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", *arguments],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def scheduler_is_running() -> bool:
    """Return whether the Docker scheduler service is currently running."""
    result = _run_compose("ps", "--status", "running", "--services", "scheduler")
    return result.returncode == 0 and "scheduler" in result.stdout.split()


def pause_scheduler() -> None:
    """Stop the Docker scheduler before direct SQLite writes."""
    result = _run_compose("stop", "scheduler")
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "docker compose stop failed"
        raise RuntimeError(message)


def start_scheduler() -> None:
    """Start the Docker scheduler after direct SQLite writes."""
    result = _run_compose("start", "scheduler")
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "docker compose start failed"
        raise RuntimeError(message)


def _run_collect_games(game_id: str, timeout: int) -> tuple[int | None, str | None]:
    command = [
        sys.executable,
        "-m",
        "src.cli.collect_games",
        "--year",
        "2020",
        "--game-ids",
        game_id,
        "--concurrency",
        "1",
    ]
    process = subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        text=True,
    )
    try:
        output, _ = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            output, _ = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            output, _ = process.communicate()
        timeout_output = output or str(exc.output or "")
        return None, f"timeout after {timeout}s\n{timeout_output[-MAX_ERROR_OUTPUT:]}"
    if process.returncode == 0:
        return 0, None
    return process.returncode, (output or "")[-MAX_ERROR_OUTPUT:]


def run_one_game(game_id: str, timeout: int) -> GameRunResult:
    """Collect and validate one game."""
    before = load_game_snapshot(game_id)
    return_code, error = _run_collect_games(game_id, timeout)
    after = load_game_snapshot(game_id)
    ok = return_code == 0 and recovery_succeeded(after)
    if not ok and error is None:
        error = "post-run validation failed"
    return GameRunResult(game_id, ok, return_code, error, before, after)


def _write_report(output_dir: Path, report: dict[str, object]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"auto_recover_2020_{timestamp}.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def _process_game_batches(
    game_ids: list[str],
    *,
    batch_size: int,
    timeout: int,
) -> tuple[list[GameRunResult], str]:
    results: list[GameRunResult] = []
    consecutive_failures = 0
    stop_reason = "completed"
    for batch_start in range(0, len(game_ids), batch_size):
        batch = game_ids[batch_start : batch_start + batch_size]
        logger.info("Processing batch %s-%s/%s", batch_start + 1, batch_start + len(batch), len(game_ids))
        for game_id in batch:
            result = run_one_game(game_id, timeout)
            results.append(result)
            if result.ok:
                consecutive_failures = 0
                logger.info("[OK] %s", game_id)
            else:
                consecutive_failures += 1
                logger.warning("[FAIL %s/3] %s: %s", consecutive_failures, game_id, result.error)
                if consecutive_failures >= 3:
                    return results, "three_consecutive_failures"
    return results, stop_reason


def run_recovery(
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_games: int | None = None,
    timeout: int = DEFAULT_GAME_TIMEOUT_SECONDS,
    pause_docker_scheduler: bool = True,
    restart_docker_scheduler: bool = True,
    output_dir: Path = PROJECT_ROOT / "data/audit/auto_recover_2020",
) -> dict[str, object]:
    """Run supervised 2020 recovery and return a JSON-serializable report."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if timeout < 1:
        raise ValueError("timeout must be positive")

    scheduler_was_running = pause_docker_scheduler and scheduler_is_running()
    stop_reason = "completed"
    results: list[GameRunResult] = []
    try:
        if scheduler_was_running:
            logger.info("Pausing Docker scheduler before recovery")
            pause_scheduler()
        game_ids = load_unresolved_game_ids()
        if max_games is not None:
            game_ids = game_ids[:max_games]
        results, stop_reason = _process_game_batches(game_ids, batch_size=batch_size, timeout=timeout)
    finally:
        if scheduler_was_running and restart_docker_scheduler:
            logger.info("Restarting Docker scheduler after recovery")
            start_scheduler()

    report = {
        "generated_at": datetime.now(KST).isoformat(),
        "year": 2020,
        "batch_size": batch_size,
        "timeout_seconds": timeout,
        "scheduler_was_running": scheduler_was_running,
        "scheduler_restarted": scheduler_was_running and restart_docker_scheduler,
        "stop_reason": stop_reason,
        "total_attempted": len(results),
        "succeeded": sum(result.ok for result in results),
        "failed": sum(not result.ok for result in results),
        "results": [asdict(result) for result in results],
    }
    report_path = _write_report(output_dir, report)
    report["report_path"] = str(report_path)
    return report


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the automated 2020 recovery argument parser."""
    parser = argparse.ArgumentParser(description="Recover unresolved 2020 games in supervised ten-game batches.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--max-games", type=int, help="Limit the number of targets for a controlled pilot")
    parser.add_argument("--timeout", type=int, default=DEFAULT_GAME_TIMEOUT_SECONDS)
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "data/audit/auto_recover_2020")
    parser.add_argument("--no-pause-scheduler", action="store_true", help="Do not pause Docker scheduler")
    parser.add_argument("--keep-scheduler-paused", action="store_true", help="Do not restart a paused scheduler")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Run the automated recovery CLI."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    args = build_arg_parser().parse_args(argv)
    report = run_recovery(
        batch_size=args.batch_size,
        max_games=args.max_games,
        timeout=args.timeout,
        pause_docker_scheduler=not args.no_pause_scheduler,
        restart_docker_scheduler=not args.keep_scheduler_paused,
        output_dir=args.output_dir,
    )
    logger.info(
        "Recovery finished: attempted=%s succeeded=%s failed=%s stop_reason=%s report=%s",
        report["total_attempted"],
        report["succeeded"],
        report["failed"],
        report["stop_reason"],
        report["report_path"],
    )


if __name__ == "__main__":
    main()
