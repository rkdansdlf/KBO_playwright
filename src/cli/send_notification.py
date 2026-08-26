"""CLI command for dispatching system notifications and alerts."""

from __future__ import annotations

import argparse
import json
import sys

from src.notifications.dispatcher import NotificationDispatcher
from src.notifications.dto import (
    NotificationChannel,
    NotificationMessage,
    NotificationPriority,
)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build command-line argument parser."""
    parser = argparse.ArgumentParser(description="Send system notifications to Telegram, Slack, or Console.")
    parser.add_argument(
        "--channel",
        type=str,
        default="console",
        choices=["all", "telegram", "slack", "console"],
        help="Notification delivery channel (default: console).",
    )
    parser.add_argument(
        "--title",
        type=str,
        required=True,
        help="Notification title.",
    )
    parser.add_argument(
        "--body",
        type=str,
        required=True,
        help="Notification message body.",
    )
    parser.add_argument(
        "--priority",
        type=str,
        default="normal",
        choices=["low", "normal", "high", "critical"],
        help="Alert priority level (default: normal).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate dispatch without sending live API requests.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output dispatch report as JSON.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI execution entrypoint."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    dispatcher = NotificationDispatcher()
    priority = NotificationPriority(args.priority)

    if args.channel == "all":
        messages = [
            NotificationMessage(
                title=args.title,
                body=args.body,
                priority=priority,
                channel=NotificationChannel.TELEGRAM,
            ),
            NotificationMessage(
                title=args.title,
                body=args.body,
                priority=priority,
                channel=NotificationChannel.SLACK,
            ),
        ]
        report = dispatcher.dispatch_batch(messages, dry_run=args.dry_run)
    else:
        channel = NotificationChannel(args.channel)
        msg = NotificationMessage(
            title=args.title,
            body=args.body,
            priority=priority,
            channel=channel,
        )
        report = dispatcher.dispatch_batch([msg], dry_run=args.dry_run)

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))  # noqa: T201
    else:
        mode_tag = "[DRY-RUN]" if args.dry_run else "[DISPATCH]"
        print(f"=== Notification Dispatch Summary {mode_tag} ===")  # noqa: T201
        print(  # noqa: T201
            f"Total: {report.total_messages} | Sent: {report.sent_count} | "
            f"Failed: {report.failed_count} | Suppressed: {report.suppressed_count}"
        )
        for r in report.results:
            tag = f"[{r.status}]"
            err = f" (Error: {r.error_message})" if r.error_message else ""
            print(f"{tag:<12} {r.channel:<12}: {r.duration_seconds}s{err}")  # noqa: T201

    return 1 if report.failed_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
