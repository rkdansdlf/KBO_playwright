"""CLI command for KBO player similarity search and 1:1 sabermetric comparison."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from src.analytics.similarity import PlayerSimilarityEngine
from src.analytics.similarity_dto import PlayerRole
from src.db.engine import get_db_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build argument parser for compare_players CLI."""
    parser = argparse.ArgumentParser(
        description="Search for similar KBO players or perform 1:1 head-to-head sabermetric comparisons.",
    )
    parser.add_argument(
        "--player1",
        "-p1",
        type=str,
        default=None,
        help="Player 1 name or ID for 1:1 comparison.",
    )
    parser.add_argument(
        "--player2",
        "-p2",
        type=str,
        default=None,
        help="Player 2 name or ID for 1:1 comparison.",
    )
    parser.add_argument(
        "--year1",
        "--season1",
        type=int,
        default=None,
        help="Season year for Player 1.",
    )
    parser.add_argument(
        "--year2",
        "--season2",
        type=int,
        default=None,
        help="Season year for Player 2.",
    )
    parser.add_argument(
        "--find-similar",
        "-s",
        type=str,
        default=None,
        help="Target player name or ID to search for similar historical/active players.",
    )
    parser.add_argument(
        "--top-k",
        "-k",
        type=int,
        default=5,
        help="Number of similar players to retrieve (default: 5).",
    )
    parser.add_argument(
        "--role",
        type=str,
        choices=["BATTER", "PITCHER"],
        default=None,
        help="Filter candidate pool by role.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "ascii", "markdown", "json"],
        default="ascii",
        help="Output format (default: ascii).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output result in JSON format.",
    )
    return parser


def run_comparison(args: argparse.Namespace, session: Session | None) -> int:
    """Execute player similarity search or 1:1 comparison with the given session."""
    engine = PlayerSimilarityEngine(session)
    output_fmt = "json" if args.json else args.format

    # Mode 1: Search for similar players
    if args.find_similar:
        role_enum = PlayerRole(args.role) if args.role else None
        sim_result = engine.find_similar_players(
            name_or_id=args.find_similar,
            season=args.year1,
            role=role_enum,
            top_k=args.top_k,
        )
        if output_fmt == "json":
            print(json.dumps(sim_result.to_dict(), ensure_ascii=False, indent=2))  # noqa: T201
        elif output_fmt == "markdown":
            print(sim_result.to_markdown())  # noqa: T201
        else:
            print(sim_result.to_ascii_card())  # noqa: T201
        return 0

    # Mode 2: 1:1 Head-to-Head Comparison
    p1 = args.player1 or "김도영"
    p2 = args.player2 or "이종범"
    cmp_result = engine.compare_players(
        player1=p1,
        player2=p2,
        season1=args.year1,
        season2=args.year2,
    )

    if output_fmt == "json":
        print(json.dumps(cmp_result.to_dict(), ensure_ascii=False, indent=2))  # noqa: T201
    elif output_fmt == "markdown":
        print(cmp_result.to_markdown())  # noqa: T201
    else:
        print(cmp_result.to_ascii_radar())  # noqa: T201

    return 0


def main(argv: list[str] | None = None) -> int:
    """Execute main player comparison CLI workflow."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        with get_db_session() as session:
            return run_comparison(args, session)
    except (SQLAlchemyError, RuntimeError, OSError, ValueError) as exc:
        logger.debug("Database session unavailable (%s). Running offline comparison.", exc)
        return run_comparison(args, None)


if __name__ == "__main__":
    sys.exit(main())
