"""CLI entrypoint to dispatch daily summary of all upcoming KBO milestones."""

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

    parser = argparse.ArgumentParser(description="Send Daily Summary of Upcoming KBO Milestones")
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
            from src.repositories.milestone_repository import MilestoneRepository

            repo = MilestoneRepository(session)
            milestones = repo.get_upcoming_milestones(season=args.season)
            lines = [
                "=" * 60,
                f"[DRY-RUN MILESTONE SUMMARY]: {len(milestones)} upcoming items",
                "=" * 60,
            ]
            lines.extend(
                f"• {m.player_name} ({m.team_code}) - {m.milestone_category}: "
                f"{m.remaining_val}개 남음 ({m.current_val}/{m.target_val})"
                for m in milestones[:10]
            )
            lines.append("=" * 60)
            sys.stdout.write("\n".join(lines) + "\n")
        else:
            result = service.send_milestone_daily_summary(
                season=args.season,
                channels=channel_list,
            )
            sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
