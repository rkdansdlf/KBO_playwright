"""Guarded CLI for independent replay of a past crawl execution run.

``kbo crawl replay`` creates a new execution run linked to the original; it
never mutates the original run or the dead letter queue. Mutations require
``--apply`` **and** ``KBO_ALLOW_CRAWL_REPLAY=1``.

Exit codes: 0 ok/preview, 1 not found, 2 not replayable/unsupported, 3 guard denied.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import TYPE_CHECKING

from src.db.engine import DB_SESSION_EXCEPTIONS, get_db_session
from src.models.crawl_execution import RUN_STATUS_RUNNING
from src.repositories.crawl_execution_repository import CrawlExecutionRepository
from src.services.crawl_run_replay import (
    CrawlRunNotFoundError,
    CrawlRunNotReplayableError,
    UnsupportedReplayCrawlerError,
    build_default_executors,
    replay_crawl_run,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from src.models.crawl_execution import CrawlExecutionRun

EXIT_OK = 0
EXIT_NOT_FOUND = 1
EXIT_INVALID_STATE = 2
EXIT_GUARD_DENIED = 3


def _write(text: str) -> None:
    sys.stdout.write(text + "\n")


def _error(text: str) -> None:
    sys.stderr.write(text + "\n")


def _replay_enabled() -> bool:
    return os.getenv("KBO_ALLOW_CRAWL_REPLAY") == "1"


def build_parser() -> argparse.ArgumentParser:
    """Build the ``kbo crawl replay`` argument parser."""
    parser = argparse.ArgumentParser(prog="kbo crawl replay", description="Replay a past crawl run independently.")
    parser.add_argument("--run-id", dest="run_id", required=True, help="Original execution run id (e.g. RUN-A).")
    parser.add_argument("--apply", action="store_true", help="Execute the replay.")
    parser.add_argument("--json", action="store_true", help="Emit JSON.")
    return parser


def _emit(  # noqa: PLR0913
    *,
    original_run_id: str,
    replay_run_id: str | None,
    applied: bool,
    status: str,
    success: bool | None,
    json_out: bool,
) -> int:
    if json_out:
        _write(
            json.dumps(
                {
                    "action": "replay",
                    "original_run_id": original_run_id,
                    "replay_run_id": replay_run_id,
                    "applied": applied,
                    "status": status,
                    "success": success,
                },
                ensure_ascii=False,
            ),
        )
    elif applied:
        _write(f"replayed {original_run_id} -> {replay_run_id}: {status}")
    else:
        _write(
            f"would replay {original_run_id} (status={status}; "
            "pass --apply and set KBO_ALLOW_CRAWL_REPLAY=1 to execute)",
        )
    return EXIT_OK


def _load_original(run_id: str) -> CrawlExecutionRun | None:
    with get_db_session() as session:
        run = CrawlExecutionRepository(session).get_by_run_id(run_id)
        if run is None:
            return None
        session.expunge(run)
        return run


def main(argv: Sequence[str] | None = None) -> int:  # noqa: PLR0911
    """CLI entrypoint for guarded crawl replay."""
    args = build_parser().parse_args(argv)

    original = _load_original(args.run_id)
    if original is None:
        _error(f"crawl run not found: {args.run_id}")
        return EXIT_NOT_FOUND
    if original.status == RUN_STATUS_RUNNING:
        _error(f"crawl run {args.run_id} is still running; only terminal runs can be replayed")
        return EXIT_INVALID_STATE
    if original.crawler not in build_default_executors():
        _error(f"no replay executor registered for crawler '{original.crawler}'")
        return EXIT_INVALID_STATE

    if not args.apply:
        return _emit(
            original_run_id=args.run_id,
            replay_run_id=None,
            applied=False,
            status=original.status,
            success=None,
            json_out=args.json,
        )
    if not _replay_enabled():
        _error("refusing replay: --apply requires KBO_ALLOW_CRAWL_REPLAY=1")
        return EXIT_GUARD_DENIED

    try:
        result = replay_crawl_run(args.run_id)
    except CrawlRunNotFoundError:
        _error(f"crawl run not found: {args.run_id}")
        return EXIT_NOT_FOUND
    except (CrawlRunNotReplayableError, UnsupportedReplayCrawlerError) as exc:
        _error(str(exc))
        return EXIT_INVALID_STATE
    except DB_SESSION_EXCEPTIONS as exc:
        _error(f"replay failed: {exc}")
        return EXIT_INVALID_STATE

    return _emit(
        original_run_id=result.original_run_id,
        replay_run_id=result.replay_run_id,
        applied=True,
        status=result.status,
        success=result.success,
        json_out=args.json,
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
