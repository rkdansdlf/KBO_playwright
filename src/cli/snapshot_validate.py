"""Read-only CLI for validating stored snapshots against their recorded baseline.

``kbo snapshot validate`` re-parses the stored artifact and reports count drift
versus the ``parsed_records`` recorded at crawl time. No network or DB writes.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import TYPE_CHECKING

from src.services.snapshot_replay import (
    SnapshotNotFoundError,
    SnapshotReplayError,
    SnapshotValidationResult,
    validate_recent_snapshots,
    validate_snapshot,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

EXIT_OK = 0
EXIT_NOT_FOUND = 1
EXIT_REPLAY_ERROR = 2
EXIT_DRIFT = 3


def _write(text: str) -> None:
    sys.stdout.write(text + "\n")


def _error(text: str) -> None:
    sys.stderr.write(text + "\n")


def build_parser() -> argparse.ArgumentParser:
    """Build the ``kbo snapshot validate`` argument parser."""
    parser = argparse.ArgumentParser(prog="kbo snapshot validate", description="Validate stored snapshots offline.")
    selector = parser.add_mutually_exclusive_group()
    selector.add_argument("--snapshot-id", dest="snapshot_id", type=int, default=None, help="Validate one snapshot.")
    selector.add_argument("--limit", type=int, default=None, help="Validate the N most recent snapshots.")
    parser.add_argument("--fail-on-drift", action="store_true", help="Exit non-zero when any snapshot drifted.")
    parser.add_argument("--json", action="store_true", help="Emit JSON.")
    return parser


def _result_dict(result: SnapshotValidationResult) -> dict[str, object]:
    return {
        "snapshot_id": result.snapshot_id,
        "source_key": result.source_key,
        "baseline_count": result.baseline_count,
        "replayed_count": result.replayed_count,
        "delta": result.delta,
        "drifted": result.drifted,
        "success": result.success,
        "error": result.error,
    }


def _render(results: list[SnapshotValidationResult], *, json_out: bool) -> None:
    if json_out:
        _write(json.dumps([_result_dict(result) for result in results], ensure_ascii=False, indent=2))
        return
    if not results:
        _write("(no snapshots)")
        return
    for result in results:
        if not result.success:
            detail = f"failed: {result.error or 'error'}"
        elif result.baseline_count is None:
            detail = f"{result.replayed_count} parsed (no baseline)"
        elif result.drifted:
            detail = f"DRIFT {result.baseline_count} -> {result.replayed_count} (delta {result.delta:+d})"
        else:
            detail = f"{result.replayed_count} parsed (match)"
        _write(f"{result.snapshot_id:<8} {result.source_key!s:<22} {detail}")


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for read-only snapshot validation."""
    args = build_parser().parse_args(argv)

    if args.snapshot_id is not None:
        try:
            results = [validate_snapshot(args.snapshot_id)]
        except SnapshotNotFoundError:
            _error(f"snapshot not found: {args.snapshot_id}")
            return EXIT_NOT_FOUND
        except SnapshotReplayError as exc:
            _error(str(exc))
            return EXIT_REPLAY_ERROR
    else:
        results = validate_recent_snapshots(limit=args.limit or 50)

    _render(results, json_out=args.json)
    if args.fail_on_drift and any(result.drifted for result in results):
        return EXIT_DRIFT
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
