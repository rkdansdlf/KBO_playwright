"""CLI for replaying stored raw snapshots through their parsers.

By default the command is read-only. ``--apply`` + ``KBO_ALLOW_SNAPSHOT_REPLAY=1``
records one ``CrawlExecutionRun`` per replay; ``--persist`` +
``KBO_ALLOW_SNAPSHOT_PERSIST=1`` writes parsed records into their domain tables.
Parsing always reads the content-addressed artifact recorded at crawl time, so
there is no network access.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import TYPE_CHECKING

from src.services.snapshot_persist import (
    SnapshotPersistResult,
    persist_recent_snapshots,
    persist_snapshot,
)
from src.services.snapshot_replay import (
    SnapshotNotFoundError,
    SnapshotReplayError,
    SnapshotReplayResult,
    SnapshotReplayRunResult,
    record_recent_snapshot_replays,
    record_snapshot_replay,
    replay_recent_snapshots,
    replay_snapshot,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

EXIT_OK = 0
EXIT_NOT_FOUND = 1
EXIT_REPLAY_ERROR = 2
EXIT_GUARD_DENIED = 3
EXIT_STRICT_FAILURE = 4


def _write(text: str) -> None:
    sys.stdout.write(text + "\n")


def _error(text: str) -> None:
    sys.stderr.write(text + "\n")


def _replay_enabled() -> bool:
    return os.getenv("KBO_ALLOW_SNAPSHOT_REPLAY") == "1"


def _persist_enabled() -> bool:
    return os.getenv("KBO_ALLOW_SNAPSHOT_PERSIST") == "1"


def build_parser() -> argparse.ArgumentParser:
    """Build the ``kbo snapshot replay`` argument parser."""
    parser = argparse.ArgumentParser(prog="kbo snapshot replay", description="Replay stored snapshots offline.")
    selector = parser.add_mutually_exclusive_group()
    selector.add_argument("--snapshot-id", dest="snapshot_id", type=int, default=None, help="Replay one snapshot id.")
    selector.add_argument("--limit", type=int, default=None, help="Replay the N most recent snapshots.")
    parser.add_argument("--apply", action="store_true", help="Record ledger runs (requires the env guard).")
    parser.add_argument("--persist", action="store_true", help="Persist parsed records (requires its env guard).")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero when any snapshot fails or is skipped.")
    parser.add_argument("--json", action="store_true", help="Emit JSON.")
    return parser


def _result_dict(result: SnapshotReplayResult) -> dict[str, object]:
    return {
        "snapshot_id": result.snapshot_id,
        "source_key": result.source_key,
        "parser_version": result.parser_version,
        "parsed_count": result.parsed_count,
        "success": result.success,
        "error": result.error,
    }


def _run_dict(result: SnapshotReplayRunResult) -> dict[str, object]:
    return {
        "snapshot_id": result.snapshot_id,
        "run_id": result.run_id,
        "status": result.status,
        "parsed_count": result.parsed_count,
        "success": result.success,
    }


def _render(results: list[SnapshotReplayResult], *, json_out: bool) -> None:
    if json_out:
        _write(json.dumps([_result_dict(result) for result in results], ensure_ascii=False, indent=2))
        return
    if not results:
        _write("(no snapshots)")
        return
    for result in results:
        status = "ok" if result.success else "failed"
        detail = f"{result.parsed_count} parsed" if result.success else (result.error or "error")
        source_key = str(result.source_key)
        parser_version = str(result.parser_version or "-")
        _write(f"{result.snapshot_id:<8} {status:<7} {source_key:<22} {parser_version:<16} {detail}")


def _render_runs(results: list[SnapshotReplayRunResult], *, json_out: bool) -> None:
    if json_out:
        _write(json.dumps([_run_dict(result) for result in results], ensure_ascii=False, indent=2))
        return
    if not results:
        _write("(no snapshots)")
        return
    for result in results:
        status = "ok" if result.success else "failed"
        _write(f"{result.snapshot_id:<8} {status:<7} {result.run_id}  {result.parsed_count} parsed")


def _persist_dict(result: SnapshotPersistResult) -> dict[str, object]:
    return {
        "snapshot_id": result.snapshot_id,
        "source_key": result.source_key,
        "target_domain": result.target_domain,
        "saved": result.saved,
        "failed_count": result.failed_count,
        "success": result.success,
        "skipped": result.skipped,
        "error": result.error,
    }


def _render_persist(results: list[SnapshotPersistResult], *, json_out: bool) -> None:
    if json_out:
        _write(json.dumps([_persist_dict(result) for result in results], ensure_ascii=False, indent=2))
        return
    if not results:
        _write("(no snapshots)")
        return
    for result in results:
        if result.skipped:
            detail = f"skipped ({result.error})"
        elif result.success:
            detail = f"saved {result.saved} to {result.target_domain}"
        else:
            detail = f"failed: {result.error or 'error'}"
        _write(f"{result.snapshot_id:<8} {detail}")


def _run_persist(args: argparse.Namespace) -> int:
    if not _persist_enabled():
        _error("refusing persist: --persist requires KBO_ALLOW_SNAPSHOT_PERSIST=1")
        return EXIT_GUARD_DENIED
    if args.snapshot_id is not None:
        try:
            results = [persist_snapshot(args.snapshot_id)]
        except SnapshotNotFoundError:
            _error(f"snapshot not found: {args.snapshot_id}")
            return EXIT_NOT_FOUND
        except SnapshotReplayError as exc:
            _error(str(exc))
            return EXIT_REPLAY_ERROR
    else:
        results = persist_recent_snapshots(limit=args.limit or 50)
    _render_persist(results, json_out=args.json)
    if args.strict and any(not result.success for result in results):
        return EXIT_STRICT_FAILURE
    return EXIT_OK


def _run_read_only(args: argparse.Namespace) -> int:
    if args.snapshot_id is not None:
        try:
            results = [replay_snapshot(args.snapshot_id)]
        except SnapshotNotFoundError:
            _error(f"snapshot not found: {args.snapshot_id}")
            return EXIT_NOT_FOUND
        except SnapshotReplayError as exc:
            _error(str(exc))
            return EXIT_REPLAY_ERROR
    else:
        results = replay_recent_snapshots(limit=args.limit or 50)
    _render(results, json_out=args.json)
    if args.strict and any(not result.success for result in results):
        return EXIT_STRICT_FAILURE
    return EXIT_OK


def _run_ledger(args: argparse.Namespace) -> int:
    if not _replay_enabled():
        _error("refusing replay: --apply requires KBO_ALLOW_SNAPSHOT_REPLAY=1")
        return EXIT_GUARD_DENIED
    if args.snapshot_id is not None:
        try:
            results = [record_snapshot_replay(args.snapshot_id)]
        except SnapshotNotFoundError:
            _error(f"snapshot not found: {args.snapshot_id}")
            return EXIT_NOT_FOUND
        except SnapshotReplayError as exc:
            _error(str(exc))
            return EXIT_REPLAY_ERROR
    else:
        results = record_recent_snapshot_replays(limit=args.limit or 50)
    _render_runs(results, json_out=args.json)
    if args.strict and any(not result.success for result in results):
        return EXIT_STRICT_FAILURE
    return EXIT_OK


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for snapshot replay (read-only by default)."""
    args = build_parser().parse_args(argv)
    if args.persist and args.apply and not (_persist_enabled() and _replay_enabled()):
        _error(
            "refusing mutation: --persist --apply requires "
            "KBO_ALLOW_SNAPSHOT_PERSIST=1 and KBO_ALLOW_SNAPSHOT_REPLAY=1",
        )
        return EXIT_GUARD_DENIED
    if args.persist:
        code = _run_persist(args)
        if code != EXIT_OK or not args.apply:
            return code
    if args.apply:
        return _run_ledger(args)
    return _run_read_only(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
