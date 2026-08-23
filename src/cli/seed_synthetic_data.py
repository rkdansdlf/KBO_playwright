"""CLI command to generate and seed mathematically consistent synthetic KBO data."""

from __future__ import annotations

import argparse
import json
import sys

from src.db.engine import get_db_session
from src.testing.dto import SyntheticSeasonConfig
from src.testing.synthetic_generator import SyntheticKBOGenerator


def build_arg_parser() -> argparse.ArgumentParser:
    """Build command-line argument parser."""
    parser = argparse.ArgumentParser(description="Generate and seed synthetic KBO dataset.")
    parser.add_argument(
        "--season",
        type=int,
        default=2026,
        help="Season year for synthetic matches (default: 2026).",
    )
    parser.add_argument(
        "--games-per-team",
        type=int,
        default=5,
        help="Number of matches to generate per team (default: 5).",
    )
    parser.add_argument(
        "--players-per-team",
        type=int,
        default=15,
        help="Number of players to generate per team (default: 15).",
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default=None,
        help="Target database URL to seed (default: application Engine).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output seeding results as JSON.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI execution entrypoint."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    config = SyntheticSeasonConfig(
        season_year=args.season,
        games_per_team=args.games_per_team,
        players_per_team=args.players_per_team,
    )

    generator = SyntheticKBOGenerator()
    dataset = generator.generate_season(config)

    if args.db_url:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session

        custom_engine = create_engine(args.db_url)
        with Session(custom_engine) as session:
            result = generator.seed_to_database(session, dataset)
            session.commit()
    else:
        with get_db_session() as session:
            result = generator.seed_to_database(session, dataset)

    if args.json:
        print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))  # noqa: T201
    else:
        print(f"=== Synthetic KBO Dataset Seeded (Season {args.season}) ===")  # noqa: T201
        print(  # noqa: T201
            f"Games: {result.total_games} | Players: {result.total_players} | "
            f"Lineups: {result.total_lineups} | PBP Events: {result.total_pbp_events} | "
            f"Elapsed: {result.elapsed_seconds}s"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
