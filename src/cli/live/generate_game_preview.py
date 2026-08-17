"""CLI entrypoint to generate rich game preview cards."""

from __future__ import annotations

import argparse
import json
import logging
import sys

from src.db.engine import get_db_session
from src.services.game_preview_generator import GamePreviewGenerator

logger = logging.getLogger(__name__)


def main() -> None:
    """Run CLI."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Generate KBO Game Preview Card")
    parser.add_argument("--away", type=str, required=True, help="Away team code (e.g. LG)")
    parser.add_argument("--home", type=str, required=True, help="Home team code (e.g. KIA)")
    parser.add_argument("--season", type=int, default=2026, help="Season year")
    parser.add_argument("--json", action="store_true", help="Output raw JSON format")

    args = parser.parse_args()

    with get_db_session() as session:
        generator = GamePreviewGenerator(session)
        preview = generator.generate_preview(
            away_team=args.away,
            home_team=args.home,
            season=args.season,
        )

        if args.json:
            print(json.dumps(preview, ensure_ascii=False, indent=2))  # noqa: T201
        else:
            print(preview["markdown_report"])  # noqa: T201


if __name__ == "__main__":
    main()
