"""CLI entrypoint to dispatch pregame preview & milestone alert cards."""

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

    parser = argparse.ArgumentParser(description="Send KBO Pregame & Milestone Alerts")
    parser.add_argument("--away", type=str, required=True, help="Away team code (e.g. LG)")
    parser.add_argument("--home", type=str, required=True, help="Home team code (e.g. KIA)")
    parser.add_argument("--season", type=int, default=2026, help="Season year")
    parser.add_argument(
        "--channels",
        type=str,
        default="telegram,slack",
        help="Comma-separated channels (telegram, slack)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print summary without sending HTTP calls")

    args = parser.parse_args()
    channel_list = [c.strip() for c in args.channels.split(",") if c.strip()]

    with get_db_session() as session:
        service = NotificationService(session)

        if args.dry_run:
            preview = service.preview_generator.generate_preview(
                away_team=args.away,
                home_team=args.home,
                season=args.season,
            )
            sys.stdout.write(
                "\n".join(
                    [
                        "=" * 60,
                        f"[DRY-RUN PREVIEW REPORT]: {args.away} vs {args.home}",
                        "=" * 60,
                        preview["markdown_report"],
                        "=" * 60,
                    ],
                )
                + "\n",
            )
        else:
            result = service.send_game_preview_report(
                away_team=args.away,
                home_team=args.home,
                season=args.season,
                channels=channel_list,
            )
            sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
