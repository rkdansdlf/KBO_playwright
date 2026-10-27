"""Report what a season contains that the index does not hold.

`audit_rag_index` compares the index against itself -- do the sparse and vector
stores agree about the same identity -- and `rag_identity_census` compares it
against the retired store. Neither asks whether what the calendar says belongs
in the index is actually there, which is how every postseason game from 2018 to
2026 went missing while both gates stayed green.

The unit is one ``game`` chunk per completed game. That is deliberately narrow:
one chunk per game is a contract the generator states, while highlight and
play-by-play counts vary with what happened on the field, so counting them would
invent gaps rather than find them. The season set matches the build's filter --
every game the season year holds, postseason rounds included.

A season that was never indexed and a season that lost rows produce the same
count and mean different things, so this reports the size of the gap against the
season rather than a verdict. The historical backlog is a known state; a season
that just lost rows is a fault, and ``--require-recent`` is what turns the second
into a gate without failing on the first.

Usage::

    python3 -m src.cli.rag.audit_rag_coverage
    python3 -m src.cli.rag.audit_rag_coverage --json
    python3 -m src.cli.rag.audit_rag_coverage --require-recent 2
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import select

from src.cli.rag.build_rag_index import _INDEXED_SEASON_TYPE_CODES
from src.constants import KST
from src.db.engine import get_rag_index_session
from src.models.game import Game
from src.models.rag_chunk import RagChunk
from src.models.season import KboSeason

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

#: The source table whose chunks stand one-for-one for a game. See the module
#: docstring for why the check is limited to it.
GAME_CHUNK_SOURCE = "game"

#: Statuses the game generator writes chunks for. Anything else is either not
#: played yet or impossible to collect, and demanding a chunk for it would be a
#: gap the build never promised to fill.
COMPLETED_LIKE_STATUSES = ("COMPLETED", "DRAW")


@dataclass(frozen=True, slots=True)
class CoverageReport:
    """Count the completed games per season and name the ones without a chunk."""

    seasons: dict[int, int] = field(default_factory=dict)
    missing: dict[int, tuple[str, ...]] = field(default_factory=dict)

    @property
    def missing_count(self) -> int:
        """Return how many completed games have no chunk."""
        return sum(len(ids) for ids in self.missing.values())

    def summary_line(self) -> str:
        """Return a one-line summary for logs."""
        return (
            f"seasons={len(self.seasons)} games={sum(self.seasons.values())} "
            f"missing={self.missing_count} seasons_with_gaps={len(self.missing)}"
        )


@dataclass(frozen=True, slots=True)
class CoverageGap:
    """One season's shortfall."""

    year: int
    games: int
    missing: tuple[str, ...]

    @property
    def covered(self) -> int:
        """Return how many of the season's completed games have a chunk."""
        return self.games - len(self.missing)

    def to_dict(self, *, sample: int) -> dict[str, object]:
        """Serialize the gap, with at most ``sample`` game ids."""
        return {
            "year": self.year,
            "games": self.games,
            "missing": len(self.missing),
            "covered": self.covered,
            "sample": list(self.missing[: max(sample, 0)]),
        }


def audit_coverage(session: Session) -> CoverageReport:
    """Count completed games per season against the ``game`` chunks on hand.

    Args:
        session: Session the games and chunks are read through.

    Returns:
        Per-season totals and the games without a chunk.

    """
    rows = session.execute(
        select(KboSeason.season_year, Game.game_id)
        .join(Game, Game.season_id == KboSeason.season_id)
        .where(
            Game.game_status.in_(COMPLETED_LIKE_STATUSES),
            # The same set the build indexes. Counting a season type the build
            # excludes would report spring training as a permanent gap.
            KboSeason.league_type_code.in_(_INDEXED_SEASON_TYPE_CODES),
        ),
    ).all()
    indexed = set(
        session.scalars(select(RagChunk.source_row_id).where(RagChunk.source_table == GAME_CHUNK_SOURCE)),
    )

    totals: dict[int, int] = {}
    missing: dict[int, list[str]] = {}
    for season_year, game_id in rows:
        year = int(season_year)
        totals[year] = totals.get(year, 0) + 1
        if str(game_id) not in indexed:
            missing.setdefault(year, []).append(str(game_id))

    return CoverageReport(
        seasons=totals,
        missing={year: tuple(sorted(ids)) for year, ids in missing.items()},
    )


def coverage_gaps(report: CoverageReport, *, since: int | None = None) -> list[CoverageGap]:
    """Return the seasons with a gap, newest first.

    Args:
        report: The audit result.
        since: Optional lower bound on the season year.

    Returns:
        One entry per season that is missing at least one chunk.

    """
    return [
        CoverageGap(year=year, games=games, missing=report.missing[year])
        for year, games in sorted(report.seasons.items(), reverse=True)
        if year in report.missing and (since is None or year >= since)
    ]


def main(argv: Sequence[str] | None = None) -> int:
    """Report season coverage from the command line.

    Args:
        argv: Optional argument list.

    Returns:
        Process exit code: 0 when acceptable, 1 when a required season has a gap.

    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    parser.add_argument("--sample", type=int, default=5, help="Game ids to show per season in JSON")
    parser.add_argument(
        "--require-recent",
        type=int,
        default=None,
        metavar="N",
        help="Return exit code 1 when any of the newest N season years has a gap",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    with get_rag_index_session() as session:
        report = audit_coverage(session)

    since: int | None = None
    if args.require_recent is not None:
        newest = max(report.seasons, default=datetime.now(KST).year)
        since = newest - max(args.require_recent, 1) + 1

    gaps = coverage_gaps(report, since=since)
    payload = {
        "seasons": {str(year): count for year, count in sorted(report.seasons.items())},
        "missing_count": report.missing_count,
        "gaps": [gap.to_dict(sample=args.sample) for gap in gaps],
    }

    if args.as_json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    else:
        logger.info("%s", report.summary_line())
        for gap in gaps:
            logger.info(
                "  %d: %d/%d covered, %d missing%s",
                gap.year,
                gap.covered,
                gap.games,
                len(gap.missing),
                f" (e.g. {', '.join(gap.missing[:3])})" if gap.missing else "",
            )

    if since is not None and gaps:
        if not args.as_json:
            logger.warning("coverage gap in %d season(s) since %d", len(gaps), since)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
