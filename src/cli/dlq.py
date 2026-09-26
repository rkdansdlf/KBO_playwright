"""Inspect the crawl dead letter queue from the master CLI.

Read-only commands (``status``/``stats``/``list``/``show``) are always safe.
Guarded operator commands (``retry``/``requeue``/``ignore``) live in
:mod:`src.cli.dlq_operator` and require an explicit apply opt-in.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import TYPE_CHECKING

from src.db.engine import get_db_session
from src.repositories.crawl_dead_letter_repository import CrawlDeadLetterRepository
from src.repositories.crawl_execution_repository import CrawlExecutionRepository
from src.services.crawl_dead_letter_stats import collect_dlq_stats

if TYPE_CHECKING:
    from collections.abc import Sequence

    from src.models.crawl_dead_letter import CrawlDeadLetter
    from src.models.crawl_execution import CrawlExecutionRun

_STATUS_CHOICES = ["pending", "retrying", "resolved", "exhausted", "ignored"]


def build_parser() -> argparse.ArgumentParser:
    """Build the ``kbo dlq`` argument parser."""
    parser = argparse.ArgumentParser(
        prog="kbo dlq",
        description="Inspect the crawl dead letter queue and replay lineage.",
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    p_status = subparsers.add_parser("status", help="Show the current DLQ state summary.")
    p_status.add_argument("--json", action="store_true", help="Emit JSON.")

    p_stats = subparsers.add_parser("stats", help="Show detailed DLQ counters.")
    p_stats.add_argument("--json", action="store_true", help="Emit JSON.")

    p_list = subparsers.add_parser("list", help="List recent dead letters.")
    p_list.add_argument("--status", choices=_STATUS_CHOICES, default=None)
    p_list.add_argument("--crawler", default=None)
    p_list.add_argument("--error-code", dest="error_code", default=None)
    p_list.add_argument("--limit", type=int, default=50)
    p_list.add_argument("--json", action="store_true", help="Emit JSON.")

    p_show = subparsers.add_parser("show", help="Show a dead letter and its replay lineage.")
    p_show.add_argument("dlq_id", help="Dead letter id.")
    p_show.add_argument("--json", action="store_true", help="Emit JSON.")

    return parser


def _write(text: str) -> None:
    sys.stdout.write(text + "\n")


def _fmt_dt(value: object) -> str:
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    return "-"


def _fmt_age(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    total = int(seconds)
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _letter_dict(letter: CrawlDeadLetter) -> dict[str, object]:
    return {
        "dlq_id": letter.dlq_id,
        "status": letter.status,
        "crawler": letter.crawler,
        "target_type": letter.target_type,
        "target_id": letter.target_id,
        "failure_stage": letter.failure_stage,
        "error_code": letter.error_code,
        "error_message": letter.error_message,
        "retry_count": letter.retry_count,
        "max_retries": letter.max_retries,
        "next_retry_at": _fmt_dt(letter.next_retry_at),
        "original_run_id": letter.original_run_id,
        "replay_run_id": letter.replay_run_id,
        "created_at": _fmt_dt(letter.created_at),
        "updated_at": _fmt_dt(letter.updated_at),
        "resolved_at": _fmt_dt(letter.resolved_at),
    }


def _run_dict(run: CrawlExecutionRun | None) -> dict[str, object] | None:
    if run is None:
        return None
    return {
        "run_id": run.run_id,
        "crawler": run.crawler,
        "status": run.status,
        "started_at": _fmt_dt(run.started_at),
        "finished_at": _fmt_dt(run.finished_at),
        "error_code": run.error_code,
        "error_message": run.error_message,
        "replay_of_run_id": run.replay_of_run_id,
    }


def _cmd_status(args: argparse.Namespace) -> int:
    stats = collect_dlq_stats()
    if args.json:
        _write(json.dumps(stats.to_dict(), ensure_ascii=False, indent=2))
        return 0
    _write("DLQ status")
    _write(f"  pending         {stats.pending}  (due: {stats.due})")
    _write(f"  retrying        {stats.retrying}  (stale: {stats.stale_retrying})")
    _write(f"  exhausted       {stats.exhausted}")
    _write(f"  resolved        {stats.resolved}")
    _write(f"  ignored         {stats.ignored}")
    _write(f"  oldest pending  {_fmt_age(stats.oldest_pending_age_seconds)}")
    return 0


def _cmd_stats(args: argparse.Namespace) -> int:
    stats = collect_dlq_stats()
    if args.json:
        _write(json.dumps(stats.to_dict(), ensure_ascii=False, indent=2))
        return 0
    _write("DLQ stats")
    for key in ("pending", "due", "retrying", "stale_retrying", "resolved", "exhausted", "ignored"):
        _write(f"  {key:<16} {getattr(stats, key)}")
    _write(f"  oldest_pending   {_fmt_dt(stats.oldest_pending_at)}")
    _write(f"  oldest_age       {_fmt_age(stats.oldest_pending_age_seconds)}")
    if stats.by_status_crawler:
        _write("  by status/crawler")
        for (status, crawler), count in sorted(stats.by_status_crawler.items()):
            _write(f"    {status:<10} {crawler:<14} {count}")
    return 0


def _cmd_list(args: argparse.Namespace) -> int:
    with get_db_session() as session:
        letters = CrawlDeadLetterRepository(session).list_recent(
            crawler=args.crawler,
            status=args.status,
            error_code=args.error_code,
            limit=args.limit,
        )
        payload = [_letter_dict(letter) for letter in letters]
    if args.json:
        _write(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    if not payload:
        _write("(no dead letters)")
        return 0
    for item in payload:
        _write(
            f"{str(item['dlq_id'])[:12]:<12}  {item['status']!s:<10} {item['crawler']!s:<14} "
            f"{item['error_code']!s:<22} retry={item['retry_count']}/{item['max_retries']} "
            f"next={item['next_retry_at']}",
        )
    return 0


def _cmd_show(args: argparse.Namespace) -> int:
    with get_db_session() as session:
        letter = CrawlDeadLetterRepository(session).get_by_dlq_id(args.dlq_id)
        if letter is None:
            _write(f"dead letter not found: {args.dlq_id}")
            return 1
        run_ids = [run_id for run_id in (letter.original_run_id, letter.replay_run_id) if run_id]
        runs = CrawlExecutionRepository(session).get_by_run_ids(run_ids)
        data = _letter_dict(letter)
        data["original_run"] = _run_dict(runs.get(letter.original_run_id))
        data["replay_run"] = _run_dict(runs.get(letter.replay_run_id)) if letter.replay_run_id else None

    if args.json:
        _write(json.dumps(data, ensure_ascii=False, indent=2))
        return 0

    _write(f"dlq_id         {data['dlq_id']}")
    _write(f"status         {data['status']}")
    _write(f"crawler        {data['crawler']}")
    _write(f"target         {data['target_id']} ({data['target_type']})")
    _write(f"error          {data['error_code']}: {data['error_message'] or '-'}")
    _write(f"retry          {data['retry_count']} / {data['max_retries']}")
    _write(f"next retry     {data['next_retry_at']}")
    _write("")
    _write("original run")
    for key in ("run_id", "crawler", "status", "started_at", "finished_at", "error_code"):
        value = (data["original_run"] or {}).get(key, "-")
        _write(f"  {key:<12} {value}")
    _write("")
    _write("latest replay")
    replay = data["replay_run"]
    if replay is None:
        _write("  (none)")
    else:
        for key in ("run_id", "status", "started_at", "finished_at", "error_code"):
            _write(f"  {key:<12} {replay.get(key, '-')}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for the read-only DLQ commands."""
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "status": _cmd_status,
        "stats": _cmd_stats,
        "list": _cmd_list,
        "show": _cmd_show,
    }
    return handlers[args.subcommand](args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
