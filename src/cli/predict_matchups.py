"""CLI command for KBO Game Matchup Win Prediction & Sabermetric Analysis."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from src.analytics.predictor import MatchupPredictor, SabermetricFeatureStore
from src.db.engine import get_db_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build argument parser for matchup prediction CLI."""
    parser = argparse.ArgumentParser(
        description="Predict KBO game win probabilities and expected scores using Sabermetrics."
    )
    parser.add_argument(
        "--game-id",
        "-g",
        type=str,
        default=None,
        help="Target Game ID (e.g., 20240829LGKIA0).",
    )
    parser.add_argument(
        "--home",
        type=str,
        default=None,
        help="Home team code (e.g., KIA, LG, SSG).",
    )
    parser.add_argument(
        "--away",
        type=str,
        default=None,
        help="Away team code (e.g., LG, DOOSAN, HANWHA).",
    )
    parser.add_argument(
        "--starter-home",
        type=str,
        default=None,
        help="Home starter pitcher name.",
    )
    parser.add_argument(
        "--starter-away",
        type=str,
        default=None,
        help="Away starter pitcher name.",
    )
    parser.add_argument(
        "--year",
        "--season",
        type=int,
        default=2024,
        help="Season year (default: 2024).",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Game date (YYYY-MM-DD or YYYYMMDD).",
    )
    parser.add_argument(
        "--format",
        choices=["text", "ascii", "markdown", "json"],
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON format (shorthand for --format json).",
    )
    return parser


def run_prediction(args: argparse.Namespace, session: Session | None = None) -> int:
    """Execute prediction based on CLI arguments."""
    feature_store = SabermetricFeatureStore(session)
    predictor = MatchupPredictor(session=session, feature_store=feature_store)

    if args.game_id:
        result = predictor.predict_game(args.game_id)
    elif args.home and args.away:
        features = feature_store.extract_features_for_teams(
            home_team=args.home,
            away_team=args.away,
            season=args.year,
            game_date=args.date,
            home_starter=args.starter_home,
            away_starter=args.starter_away,
        )
        result = predictor.predict_matchup(features)
    else:
        # Default sample prediction
        features = feature_store._generate_fallback_vector()  # noqa: SLF001
        result = predictor.predict_matchup(features)

    output_fmt = "json" if args.json else args.format

    if output_fmt == "json":
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))  # noqa: T201
    elif output_fmt == "markdown":
        print(result.to_markdown())  # noqa: T201
    else:
        print(result.to_ascii_card())  # noqa: T201

    return 0


def main(argv: list[str] | None = None) -> int:
    """Execute main matchup prediction CLI workflow."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        with get_db_session() as session:
            return run_prediction(args, session)
    except (SQLAlchemyError, RuntimeError, OSError, ValueError) as exc:
        logger.debug("Database session unavailable (%s). Running offline prediction.", exc)
        return run_prediction(args, None)


if __name__ == "__main__":
    sys.exit(main())
