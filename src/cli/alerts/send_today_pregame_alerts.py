"""CLI entrypoint to dynamically resolve today's KBO games and dispatch preview cards."""

from __future__ import annotations

import argparse
import json
import logging
import sys

from src.db.engine import get_db_session
from src.services.notification_service import NotificationService

logger = logging.getLogger(__name__)


def main() -> None:
    """Run CLI."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Send Today's KBO All Pregame Alerts")
    parser.add_argument("--date", type=str, default=None, help="Target date YYYYMMDD (defaults to today)")
    parser.add_argument("--season", type=int, default=2026, help="Season year")
    parser.add_argument(
        "--channels",
        type=str,
        default="telegram",
        help="Comma-separated channels (telegram, slack)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print summary without sending HTTP calls")

    args = parser.parse_args()
    channel_list = [c.strip() for c in args.channels.split(",") if c.strip()]

    with get_db_session() as session:
        service = NotificationService(session)

        if args.dry_run:
            result = service.send_today_all_pregame_alerts(
                target_date=args.date,
                season=args.season,
                channels=[],
            )
            sys.stdout.write(
                "\n".join(
                    [
                        "=" * 60,
                        f"[DRY-RUN TODAY PREGAME ALERTS]: {result.get('game_count', 0)} games found",
                        "=" * 60,
                        json.dumps(result, ensure_ascii=False, indent=2),
                    ],
                )
                + "\n",
            )
        else:
            result = service.send_today_all_pregame_alerts(
                target_date=args.date,
                season=args.season,
                channels=channel_list,
            )
            sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
