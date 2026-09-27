"""Guarded operator mutations for the crawl dead letter queue.

Mutations require an explicit ``--apply`` **and** ``KBO_ALLOW_DLQ_MUTATION=1`` so
an accidental invocation on a production host cannot change state. All actions
go through the service/state machine; the repository is never mutated directly.

The command flow is ``load -> validate -> preview -> guard -> mutate`` so that
dry-runs still validate existence/state/due, while only mutations are gated.

Exit codes: 0 ok/preview, 1 not found, 2 invalid state/not due, 3 guard denied.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from src.db.engine import DB_SESSION_EXCEPTIONS, get_db_session
from src.models.crawl_dead_letter import DlqStatus
from src.repositories.crawl_dead_letter_repository import CrawlDeadLetterRepository
from src.services.crawl_dead_letter_service import (
    CrawlDeadLetterService,
    DlqNotFoundError,
    retry_dead_letter,
)
from src.services.crawl_dead_letter_state import InvalidDlqTransitionError, can_requeue
from src.services.crawl_dead_letter_stats import publish_dlq_state_metrics
from src.services.crawl_replay_dispatcher import build_default_dispatcher

if TYPE_CHECKING:
    from collections.abc import Sequence

    from src.models.crawl_dead_letter import CrawlDeadLetter

EXIT_OK = 0
EXIT_NOT_FOUND = 1
EXIT_INVALID_STATE = 2
EXIT_GUARD_DENIED = 3


def _write(text: str) -> None:
    sys.stdout.write(text + "\n")


def _error(text: str) -> None:
    sys.stderr.write(text + "\n")


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _mutation_enabled() -> bool:
    return os.getenv("KBO_ALLOW_DLQ_MUTATION") == "1"


def build_parser() -> argparse.ArgumentParser:
    """Build the operator argument parser."""
    parser = argparse.ArgumentParser(prog="kbo dlq", description="Guarded dead letter operator actions.")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)
    for name, help_text in (
        ("retry", "Retry a pending, due dead letter."),
        ("requeue", "Requeue an ignored/exhausted dead letter."),
        ("ignore", "Dismiss a pending dead letter."),
    ):
        sub = subparsers.add_parser(name, help=help_text)
        sub.add_argument("dlq_id", help="Dead letter id.")
        sub.add_argument("--reason", default=None, help="Reason for the ignore action.")
        sub.add_argument("--apply", action="store_true", help="Apply the mutation.")
        sub.add_argument("--json", action="store_true", help="Emit JSON.")
    return parser


def _emit(action: str, dlq_id: str, *, applied: bool, status: str, json_out: bool) -> int:
    """Print the shared operator envelope (text or JSON)."""
    if json_out:
        _write(
            json.dumps(
                {"action": action, "dlq_id": dlq_id, "applied": applied, "status": status},
                ensure_ascii=False,
            ),
        )
    elif applied:
        _write(f"{action} {dlq_id}: {status}")
    else:
        _write(f"would {action} dlq_id={dlq_id} (status={status}; pass --apply and set KBO_ALLOW_DLQ_MUTATION=1)")
    return EXIT_OK


def _require_guard() -> bool:
    if _mutation_enabled():
        return True
    _error("refusing mutation: --apply requires KBO_ALLOW_DLQ_MUTATION=1")
    return False


def _load_letter(dlq_id: str) -> CrawlDeadLetter | None:
    with get_db_session() as session:
        letter = CrawlDeadLetterRepository(session).get_by_dlq_id(dlq_id)
        if letter is None:
            return None
        session.expunge(letter)
        return letter


def _refresh_metrics() -> None:
    try:
        publish_dlq_state_metrics()
    except DB_SESSION_EXCEPTIONS:
        sys.stderr.write("warning: failed to refresh DLQ metrics\n")


def _cmd_retry(args: argparse.Namespace) -> int:  # noqa: PLR0911
    letter = _load_letter(args.dlq_id)
    if letter is None:
        _error(f"dead letter not found: {args.dlq_id}")
        return EXIT_NOT_FOUND
    if letter.status != DlqStatus.PENDING.value:
        _error(f"retry requires pending status, found {letter.status}")
        return EXIT_INVALID_STATE
    if letter.next_retry_at is not None and letter.next_retry_at > _utcnow():
        _error(f"retry not due until {letter.next_retry_at.isoformat()}")
        return EXIT_INVALID_STATE
    if not args.apply:
        return _emit("retry", args.dlq_id, applied=False, status=letter.status, json_out=args.json)
    if not _require_guard():
        return EXIT_GUARD_DENIED

    try:
        result = retry_dead_letter(args.dlq_id, build_default_dispatcher())
    except DlqNotFoundError:
        _error(f"dead letter not found: {args.dlq_id}")
        return EXIT_NOT_FOUND
    except InvalidDlqTransitionError as exc:
        _error(str(exc))
        return EXIT_INVALID_STATE
    _refresh_metrics()
    return _emit("retry", args.dlq_id, applied=True, status=result.status.value, json_out=args.json)


def _cmd_requeue(args: argparse.Namespace) -> int:  # noqa: PLR0911
    letter = _load_letter(args.dlq_id)
    if letter is None:
        _error(f"dead letter not found: {args.dlq_id}")
        return EXIT_NOT_FOUND
    if not can_requeue(letter.status):
        _error(f"requeue requires ignored/exhausted status, found {letter.status}")
        return EXIT_INVALID_STATE
    if not args.apply:
        return _emit("requeue", args.dlq_id, applied=False, status=letter.status, json_out=args.json)
    if not _require_guard():
        return EXIT_GUARD_DENIED

    try:
        with get_db_session() as session:
            CrawlDeadLetterService(session).requeue(args.dlq_id)
            session.commit()
    except DlqNotFoundError:
        _error(f"dead letter not found: {args.dlq_id}")
        return EXIT_NOT_FOUND
    except InvalidDlqTransitionError as exc:
        _error(str(exc))
        return EXIT_INVALID_STATE
    _refresh_metrics()
    return _emit("requeue", args.dlq_id, applied=True, status=DlqStatus.PENDING.value, json_out=args.json)


def _cmd_ignore(args: argparse.Namespace) -> int:  # noqa: PLR0911
    letter = _load_letter(args.dlq_id)
    if letter is None:
        _error(f"dead letter not found: {args.dlq_id}")
        return EXIT_NOT_FOUND
    if letter.status != DlqStatus.PENDING.value:
        _error(f"ignore requires pending status, found {letter.status}")
        return EXIT_INVALID_STATE
    if not args.apply:
        return _emit("ignore", args.dlq_id, applied=False, status=letter.status, json_out=args.json)
    if not _require_guard():
        return EXIT_GUARD_DENIED

    try:
        with get_db_session() as session:
            CrawlDeadLetterService(session).mark_ignored(args.dlq_id, reason=args.reason)
            session.commit()
    except DlqNotFoundError:
        _error(f"dead letter not found: {args.dlq_id}")
        return EXIT_NOT_FOUND
    except InvalidDlqTransitionError as exc:
        _error(str(exc))
        return EXIT_INVALID_STATE
    _refresh_metrics()
    return _emit("ignore", args.dlq_id, applied=True, status=DlqStatus.IGNORED.value, json_out=args.json)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for guarded DLQ operator actions."""
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "retry": _cmd_retry,
        "requeue": _cmd_requeue,
        "ignore": _cmd_ignore,
    }
    return handlers[args.subcommand](args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
