"""Link award winners to the player registry using PlayerIdResolver.

Read-only by default; use --apply to persist resolved player_id/team_code.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.engine import SessionLocal
from src.models.award import Award
from src.services.player_id_resolver import PlayerIdResolver
from src.utils.team_mapping import get_team_code

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AwardLinkReport:
    """Summary of the award-to-player linking run."""

    total: int = 0
    resolved: int = 0
    unresolved_team: int = 0
    unresolved_player: int = 0
    applied: bool = False


def link_award_player_ids(
    session: Session,
    *,
    year: int | None = None,
    award_type: str | None = None,
    apply: bool = False,
) -> AwardLinkReport:
    """Resolve player_id for awards, optionally persisting the result."""
    stmt = select(Award)
    if year is not None:
        stmt = stmt.where(Award.year == year)
    if award_type:
        stmt = stmt.where(Award.award_type == award_type)
    awards = list(session.scalars(stmt).all())

    resolver = PlayerIdResolver(session)
    resolved = 0
    unresolved_team = 0
    unresolved_player = 0
    for award in awards:
        team_code = get_team_code(award.team_name, award.year)
        if team_code is None:
            unresolved_team += 1
            logger.warning(" [team] %s %s %r has no team mapping", award.year, award.award_type, award.team_name)
            continue
        player_id = resolver.resolve_id(award.player_name, team_code, award.year)
        if player_id is None:
            unresolved_player += 1
            continue
        resolved += 1
        if apply:
            award.player_id = player_id
            award.team_code = team_code

    if apply:
        session.commit()
    return AwardLinkReport(
        total=len(awards),
        resolved=resolved,
        unresolved_team=unresolved_team,
        unresolved_player=unresolved_player,
        applied=apply,
    )


def _summary_line(report: AwardLinkReport) -> str:
    """Render the human-readable summary line."""
    mode = "applied" if report.applied else "dry-run"
    return (
        f"[award-link:{mode}] total={report.total} resolved={report.resolved} "
        f"unresolved_team={report.unresolved_team} unresolved_player={report.unresolved_player}"
    )


def main(argv: list[str] | None = None) -> int:
    """Run the award-to-player linking CLI."""
    parser = argparse.ArgumentParser(description="Link award winners to player IDs")
    parser.add_argument("--year", type=int, help="Only link awards of this year")
    parser.add_argument("--type", dest="award_type", help="Only link awards of this type")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist player_id/team_code (default is dry-run)",
    )
    args = parser.parse_args(argv)

    with SessionLocal() as session:
        report = link_award_player_ids(
            session,
            year=args.year,
            award_type=args.award_type,
            apply=args.apply,
        )
    print(_summary_line(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
