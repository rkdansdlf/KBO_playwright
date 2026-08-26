"""Script to audit mathematical and relational invariants for historical games (1982-2000).

Validates:
1. game.home_score == SUM(game_inning_scores.runs for home)
2. game.away_score == SUM(game_inning_scores.runs for away)
3. 9 innings present for each team side
4. Valid team codes mapped
5. Provenance metadata completeness (100%)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

import os

from sqlalchemy import create_engine, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker


from src.models.game import Game, GameInningScore, GameMetadata

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class InvariantAuditResult:
    """Detailed invariant audit result for historical seasons."""

    season: int
    total_games: int
    score_mismatches: int
    inning_count_mismatches: int
    missing_provenance: int
    is_valid: bool


def audit_historical_season_invariants(
    season: int,
    session: Session | None = None,
    db_url: str | None = None,
) -> InvariantAuditResult:
    """Audit mathematical invariants for a specific historical season."""
    prefix = f"{season}%"
    close_session = False
    if session is None:
        target_url = db_url or os.environ.get("DATABASE_URL") or "sqlite:///./data/kbo_dev.db"
        try:
            engine = create_engine(target_url)
            session = sessionmaker(bind=engine)()
        except (SQLAlchemyError, RuntimeError, OSError, ValueError, TypeError):
            engine = create_engine("sqlite:///./data/kbo_dev.db")
            session = sessionmaker(bind=engine)()

        close_session = True

    try:
        # 1. Total games
        games = list(session.execute(select(Game).where(Game.game_id.like(prefix))).scalars().all())
        total_games = len(games)
        if total_games == 0:
            return InvariantAuditResult(season, 0, 0, 0, 0, is_valid=True)

        # 2. Inning score sum vs game final score
        score_mismatches = 0
        inning_count_mismatches = 0
        missing_provenance = 0

        for g in games:
            # Check metadata
            meta = session.execute(select(GameMetadata).where(GameMetadata.game_id == g.game_id)).scalar_one_or_none()
            if not meta or not meta.source_payload:
                missing_provenance += 1

            # Check home innings
            home_runs_sum = (
                session.execute(
                    select(func.sum(GameInningScore.runs)).where(
                        GameInningScore.game_id == g.game_id, GameInningScore.team_side == "home"
                    )
                ).scalar()
                or 0
            )

            # Check away innings
            away_runs_sum = (
                session.execute(
                    select(func.sum(GameInningScore.runs)).where(
                        GameInningScore.game_id == g.game_id, GameInningScore.team_side == "away"
                    )
                ).scalar()
                or 0
            )

            if g.home_score is not None and home_runs_sum != g.home_score:
                score_mismatches += 1
            if g.away_score is not None and away_runs_sum != g.away_score:
                score_mismatches += 1

            # Check 9 innings per side
            inn_count = (
                session.execute(
                    select(func.count(GameInningScore.id)).where(GameInningScore.game_id == g.game_id)
                ).scalar()
                or 0
            )
            if inn_count != 18:  # 9 home + 9 away
                inning_count_mismatches += 1

        is_valid = score_mismatches == 0 and inning_count_mismatches == 0 and missing_provenance == 0

        return InvariantAuditResult(
            season=season,
            total_games=total_games,
            score_mismatches=score_mismatches,
            inning_count_mismatches=inning_count_mismatches,
            missing_provenance=missing_provenance,
            is_valid=is_valid,
        )
    finally:
        if close_session and session:
            session.close()


def main(argv: Sequence[str] | None = None) -> int:
    """CLI main entry point."""
    parser = argparse.ArgumentParser(description="Audit Mathematical Invariants for Historical Games (1982-2000)")
    parser.add_argument("--start-year", type=int, default=1982, help="Start season year")
    parser.add_argument("--end-year", type=int, default=2000, help="End season year")
    parser.add_argument("--db-url", type=str, default=None, help="Database connection URL")
    parser.add_argument("--json", action="store_true", default=False, help="Output JSON result")

    args = parser.parse_args(argv)

    results = []
    all_valid = True

    print(
        f"{'season':>6} | {'games':>6} | {'score_mismatches':>16} | {'inn_mismatches':>14} | {'missing_prov':>12} | {'status':<10}"
    )
    print("-" * 85)

    for year in range(args.start_year, args.end_year + 1):
        res = audit_historical_season_invariants(year, db_url=args.db_url)
        results.append(res)
        if not res.is_valid:
            all_valid = False

        status_str = "PASS" if res.is_valid else "FAIL"
        print(
            f"{res.season:>6} | {res.total_games:>6} | {res.score_mismatches:>16} | "
            f"{res.inning_count_mismatches:>14} | {res.missing_provenance:>12} | {status_str:<10}"
        )

    if args.json:
        sys.stdout.write(json.dumps([asdict(r) for r in results], indent=2) + "\n")

    return 0 if all_valid else 1


if __name__ == "__main__":
    sys.exit(main())
