"""Read-only CLI for replaying stored raw snapshots through their parsers.

``kbo snapshot replay`` never writes to the database or the network; it re-runs
the registered parser over the content-addressed artifact recorded at crawl time
so parser changes can be re-validated offline.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import TYPE_CHECKING

from src.services.snapshot_replay import (
    SnapshotNotFoundError,
    SnapshotReplayError,
    SnapshotReplayResult,
    replay_recent_snapshots,
    replay_snapshot,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

EXIT_OK = 0
EXIT_NOT_FOUND = 1
EXIT_REPLAY_ERROR = 2


def _write(text: str) -> None:
    sys.stdout.write(text + "\n")


def _error(text: str) -> None:
    sys.stderr.write(text + "\n")


def build_parser() -> argparse.ArgumentParser:
    """Build the ``kbo snapshot replay`` argument parser."""
    parser = argparse.ArgumentParser(prog="kbo snapshot replay", description="Replay stored snapshots offline.")
    selector = parser.add_mutually_exclusive_group()
    selector.add_argument("--snapshot-id", dest="snapshot_id", type=int, default=None, help="Replay one snapshot id.")
    selector.add_argument("--limit", type=int, default=None, help="Replay the N most recent snapshots.")
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


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for read-only snapshot replay."""
    args = build_parser().parse_args(argv)

    if args.snapshot_id is not None:
        try:
            result = replay_snapshot(args.snapshot_id)
        except SnapshotNotFoundError:
            _error(f"snapshot not found: {args.snapshot_id}")
            return EXIT_NOT_FOUND
        except SnapshotReplayError as exc:
            _error(str(exc))
            return EXIT_REPLAY_ERROR
        _render([result], json_out=args.json)
        return EXIT_OK

    results = replay_recent_snapshots(limit=args.limit or 50)
    _render(results, json_out=args.json)
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
