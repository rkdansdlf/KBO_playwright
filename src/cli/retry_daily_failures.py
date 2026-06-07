"""Retry daily finalize soft-failure candidates from the stability summary."""

from __future__ import annotations

import argparse
import json
import logging
import shlex
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from src.cli.run_daily_update import DEFAULT_DAILY_SUMMARY_DIR

from src.utils.team_codes import normalize_kbo_game_id

logger = logging.getLogger(__name__)

Command = list[str]
Runner = Callable[[Sequence[str]], None]


def _summary_path(target_date: str, summary_dir: str | Path | None = None) -> Path:
    output_dir = Path(summary_dir) if summary_dir is not None else DEFAULT_DAILY_SUMMARY_DIR
    return output_dir / f"{target_date}.json"


def load_daily_summary(path: str | Path) -> dict[str, Any]:
    summary_path = Path(path)
    if not summary_path.exists():
        raise FileNotFoundError(f"Daily summary not found: {summary_path}")

    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Malformed daily summary JSON: {summary_path}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"Daily summary must be a JSON object: {summary_path}")
    if not isinstance(payload.get("stability"), dict):
        raise ValueError(f"Daily summary missing stability payload: {summary_path}")
    return payload


def _dedupe_game_ids(values: Iterable[object]) -> list[str]:
    normalized: list[str] = []
    for value in values:
        game_id = normalize_kbo_game_id(str(value or "").strip())
        if game_id:
            normalized.append(game_id)
    return sorted(dict.fromkeys(normalized))


def retry_candidates(summary: Mapping[str, Any]) -> tuple[list[str], list[str]]:
    stability = summary.get("stability")
    if not isinstance(stability, Mapping):
        return [], []

    retry = stability.get("retry_candidates")
    if not isinstance(retry, Mapping):
        return [], []

    detail_ids = retry.get("detail") if isinstance(retry.get("detail"), list) else []
    relay_ids = retry.get("relay") if isinstance(retry.get("relay"), list) else []
    return _dedupe_game_ids(detail_ids), _dedupe_game_ids(relay_ids)


def _detail_groups(game_ids: Sequence[str]) -> dict[tuple[int, int], list[str]]:
    groups: dict[tuple[int, int], list[str]] = defaultdict(list)
    for game_id in game_ids:
        if len(game_id) < 8 or not game_id[:8].isdigit():
            raise ValueError(f"Invalid KBO game_id date prefix: {game_id}")
        year = int(game_id[:4])
        month = int(game_id[4:6])
        if not 1 <= month <= 12:
            raise ValueError(f"Invalid KBO game_id month: {game_id}")
        groups[(year, month)].append(game_id)
    return {key: sorted(set(values)) for key, values in sorted(groups.items())}


def build_retry_commands(
    summary: Mapping[str, Any],
    *,
    sync: bool = False,
    python_bin: str = sys.executable,
) -> list[Command]:
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
            ]
        )

    if relay_ids:
        commands.append(
            [
                python_bin,
                "scripts/fetch_kbo_pbp.py",
                "--game-ids",
                ",".join(relay_ids),
                "--force",
            ]
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
                ]
            )

    return commands


def _default_runner(command: Sequence[str]) -> None:
    subprocess.run(list(command), check=True)


def run_retry(
    *,
    target_date: str,
    summary_dir: str | Path | None = None,
    apply: bool = False,
    sync: bool = False,
    runner: Runner | None = None,
    python_bin: str = sys.executable,
) -> int:
    summary_file = _summary_path(target_date, summary_dir)
    summary = load_daily_summary(summary_file)
    commands = build_retry_commands(summary, sync=sync, python_bin=python_bin)

    if not commands:
        logger.info(f"No retry candidates found in {summary_file}")
        return 0

    action = "apply" if apply else "dry-run"
    logger.info(f"Daily failure retry {action}: date={target_date} commands={len(commands)}")
    for command in commands:
        logger.info(f"  $ {shlex.join(command)}")

    if not apply:
        logger.info("Dry run only. Re-run with --apply to execute these commands.")
        return 0

    command_runner = runner or _default_runner
    for command in commands:
        command_runner(command)
    logger.info("Retry commands completed.")
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
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
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if len(args.date) != 8 or not args.date.isdigit():
        parser.error("--date must be YYYYMMDD")

    try:
        return run_retry(
            target_date=args.date,
            summary_dir=args.summary_dir,
            apply=args.apply,
            sync=args.sync,
            runner=runner,
        )
    except (FileNotFoundError, ValueError) as exc:
        logger.exception(f"{exc}")
        logger.error(f"{exc}")
        return 2
    except subprocess.CalledProcessError as exc:
        logger.exception(f"Retry command failed with exit code {exc.returncode}: {shlex.join(exc.cmd)}")
        logger.error(f"Retry command failed with exit code {exc.returncode}: {shlex.join(exc.cmd)}")
        return exc.returncode or 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
