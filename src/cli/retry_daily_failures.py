"""Retry daily finalize soft-failure candidates from the stability summary."""

from __future__ import annotations

import argparse
import json
import logging
import shlex
import subprocess
import sys
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from src.cli.run_daily_update import DEFAULT_DAILY_SUMMARY_DIR
from src.constants import DATE_STR_LEN
from src.utils.team_codes import normalize_kbo_game_id

logger = logging.getLogger(__name__)

Command = list[str]
Runner = Callable[[Sequence[str]], None]
MAX_MONTH = 12


def _summary_path(target_date: str, summary_dir: str | Path | None = None) -> Path:
    output_dir = Path(summary_dir) if summary_dir is not None else DEFAULT_DAILY_SUMMARY_DIR
    return output_dir / f"{target_date}.json"


def _validate_summary_payload(payload: object, summary_path: Path) -> dict[str, Any]:
    if not isinstance(payload, dict):
        msg = f"Daily summary must be a JSON object: {summary_path}"
        raise TypeError(msg)
    if not isinstance(payload.get("stability"), dict):
        msg = f"Daily summary missing stability payload: {summary_path}"
        raise TypeError(msg)
    return payload


def load_daily_summary(path: str | Path) -> dict[str, Any]:
    """Load daily summary.

    Args:
        path: Path.
        path: Path.

    Returns:
        Dictionary result.

    """
    summary_path = Path(path)

    if not summary_path.exists():
        msg = f"Daily summary not found: {summary_path}"
        raise FileNotFoundError(msg)

    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        msg = f"Malformed daily summary JSON: {summary_path}"
        raise ValueError(msg) from exc

    return _validate_summary_payload(payload, summary_path)


def _dedupe_game_ids(values: Iterable[object]) -> list[str]:
    normalized: list[str] = []
    for value in values:
        game_id = normalize_kbo_game_id(str(value or "").strip())
        if game_id:
            normalized.append(game_id)
    return sorted(dict.fromkeys(normalized))


def retry_candidates(summary: Mapping[str, Any]) -> tuple[list[str], list[str]]:
    """Handle the retry candidates operation.

    Args:
        summary: Summary.
        summary: Summary.

    Returns:
        Tuple result.

    """
    stability = summary.get("stability")

    if not isinstance(stability, Mapping):
        return [], []

    retry = stability.get("retry_candidates")
    if not isinstance(retry, Mapping):
        return [], []

    detail_ids = retry.get("detail") if isinstance(retry.get("detail"), list) else []
    relay_ids = retry.get("relay") if isinstance(retry.get("relay"), list) else []
    return _dedupe_game_ids(detail_ids), _dedupe_game_ids(relay_ids)  # type: ignore[arg-type]


def _detail_groups(game_ids: Sequence[str]) -> dict[tuple[int, int], list[str]]:
    groups: dict[tuple[int, int], list[str]] = defaultdict(list)
    for game_id in game_ids:
        if len(game_id) < DATE_STR_LEN or not game_id[:DATE_STR_LEN].isdigit():
            msg = f"Invalid KBO game_id date prefix: {game_id}"
            raise ValueError(msg)
        year = int(game_id[:4])
        month = int(game_id[4:6])
        if not 1 <= month <= MAX_MONTH:
            msg = f"Invalid KBO game_id month: {game_id}"
            raise ValueError(msg)
        groups[(year, month)].append(game_id)
    return {key: sorted(set(values)) for key, values in sorted(groups.items())}


def build_retry_commands(
    summary: Mapping[str, Any],
    *,
    sync: bool = False,
    python_bin: str = sys.executable,
) -> list[Command]:
    """Build retry commands.

    Args:
        summary: Summary.
        sync: Whether to sync to remote database.
        python_bin: Python Bin.
        summary: Summary.

    Returns:
        List of results.

    """
    detail_ids, relay_ids = retry_candidates(summary)

    commands: list[Command] = []

    for (year, month), game_ids in _detail_groups(detail_ids).items():
        commands.append(
            [
                python_bin,
                "-m",
                "src.cli.collect_games",
                "--year",
                str(year),
                "--month",
                str(month),
                "--game-ids",
                ",".join(game_ids),
                "--force",
                "--concurrency",
                "1",
            ],
        )

    if relay_ids:
        commands.append(
            [
                python_bin,
                "scripts/fetch_kbo_pbp.py",
                "--game-ids",
                ",".join(relay_ids),
                "--force",
            ],
        )

    if sync:
        sync_ids = sorted(set(detail_ids) | set(relay_ids))
        if sync_ids:
            commands.append(
                [
                    python_bin,
                    "-m",
                    "src.cli.sync_oci",
                    "--game-details",
                    "--game-ids",
                    ",".join(sync_ids),
                ],
            )

    return commands


def _default_runner(command: Sequence[str]) -> None:
    subprocess.run(list(command), check=True)


def run_retry(  # noqa: PLR0913
    *,
    target_date: str,
    summary_dir: str | Path | None = None,
    apply: bool = False,
    sync: bool = False,
    runner: Runner | None = None,
    python_bin: str = sys.executable,
) -> int:
    """Run retry.

    Args:
        target_date: Target date for the operation.
        summary_dir: Summary Dir.
        apply: Apply.
        sync: Whether to sync to remote database.
        runner: Runner.
        python_bin: Python Bin.

    Returns:
        Integer result.

    """
    summary_file = _summary_path(target_date, summary_dir)

    summary = load_daily_summary(summary_file)
    commands = build_retry_commands(summary, sync=sync, python_bin=python_bin)

    if not commands:
        logger.info("No retry candidates found in %s", summary_file)
        return 0

    action = "apply" if apply else "dry-run"
    logger.info("Daily failure retry %s: date=%s commands=%s", action, target_date, len(commands))
    for command in commands:
        logger.info("  $ %s", shlex.join(command))

    if not apply:
        logger.info("Dry run only. Re-run with --apply to execute these commands.")
        return 0

    command_runner = runner or _default_runner
    for command in commands:
        command_runner(command)
    logger.info("Retry commands completed.")
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    """Build arg parser.

    Returns:
        The result of the operation.

    """
    parser = argparse.ArgumentParser(description="Retry daily stability summary failure candidates")

    parser.add_argument("--date", required=True, help="Target date in YYYYMMDD format")
    parser.add_argument(
        "--summary-dir",
        default=str(DEFAULT_DAILY_SUMMARY_DIR),
        help="Directory containing daily summary JSON files",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Print retry commands without executing them")
    mode.add_argument("--apply", action="store_true", help="Execute retry commands")
    parser.add_argument("--sync", action="store_true", help="Sync retried game_ids to OCI after retry commands succeed")
    return parser


def main(argv: Sequence[str] | None = None, *, runner: Runner | None = None) -> int:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.
        runner: Runner.

    """
    parser = build_arg_parser()

    args = parser.parse_args(argv)
    if len(args.date) != DATE_STR_LEN or not args.date.isdigit():
        parser.error("--date must be YYYYMMDD")

    try:
        return run_retry(
            target_date=args.date,
            summary_dir=args.summary_dir,
            apply=args.apply,
            sync=args.sync,
            runner=runner,
        )
    except (FileNotFoundError, ValueError):
        logger.exception("Retry command configuration error")
        return 2
    except subprocess.CalledProcessError as exc:
        logger.exception("Retry command failed with exit code %s", exc.returncode)
        logger.exception("Retry command failed with exit code %s", exc.returncode)
        return exc.returncode or 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
