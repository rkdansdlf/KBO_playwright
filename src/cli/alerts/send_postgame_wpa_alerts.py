"""CLI entrypoint to dispatch WPA Hero & postgame summary report for a completed game."""

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

    parser = argparse.ArgumentParser(description="Send Postgame WPA Hero Alert")
    parser.add_argument("--game-id", type=str, required=True, help="Completed game ID")
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
            result = service.send_postgame_wpa_hero_alert(
                game_id=args.game_id,
                season=args.season,
                channels=[],
            )
            sys.stdout.write(
                "\n".join(
                    [
                        "=" * 60,
                        f"[DRY-RUN POSTGAME WPA HERO]: Game ID {args.game_id}",
                        "=" * 60,
                        json.dumps(result, ensure_ascii=False, indent=2),
                    ],
                )
                + "\n",
            )
        else:
            result = service.send_postgame_wpa_hero_alert(
                game_id=args.game_id,
                season=args.season,
                channels=channel_list,
            )
            sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
