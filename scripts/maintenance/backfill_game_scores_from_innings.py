"""Fill in team totals for finished games whose inning lines already determine them.

A detail crawl writes the inning lines, the box score and the two team totals from
different parts of the same page. A partial render can produce the first two
without the third, and nothing then fills the totals in: the status derivation
falls back to "there is progress evidence", which is correct, so the row is stored
``COMPLETED`` with no score and nothing looks wrong.

The batch that wrote the innings for 2026-08-20 on 2026-08-26 did exactly this --
five games, all ``COMPLETED``, all with complete inning lines, no totals -- and
reported success. 365 stored games are in that state; 364 of them are from 2021,
2022 and 2023 and will never be re-crawled, because the daily pipeline does not
revisit past seasons.

Nothing here is inferred. Each total is the sum of that side's stored innings,
and every one of the 12,188 stored games that already carry both a total and
innings agrees with its own sum -- ``game_score_disagrees_with_innings`` reports
zero. That agreement is what makes this arithmetic rather than a guess, so the
script refuses any game where the two cannot be checked against each other.

Only a blank side is written. A total the source reported is the source's own
statement, and replacing it with a sum would erase the only trace that the two
parts of the page disagreed.

Both sides must have innings, and the line must look like a whole game. See
``game_save._innings_are_complete``: the away side always bats nine innings and
the home side bats nine or one fewer, so a shorter line is a partial crawl whose
sum is a partial score. Writing that is worse than leaving the total blank --
the value then agrees with the innings it came from and no check can see it.

Two limits worth knowing before trusting the length rule:

* Years 2024 and 2025 store every game as twelve innings, with innings ten to
  twelve written as ``is_extra=True, runs=0``. Length therefore says nothing
  there, and a truncated line in those years cannot be told from a complete one.
  No target is in those years, and the padding is harmless to the arithmetic
  because the extra slots are zero.
* A called game and a truncated crawl are structurally identical -- both are a
  prefix of innings starting at one with no gaps -- so a short line is skipped
  rather than judged. Exactly one target is short: ``20230404LTSK0``, seven
  innings on each side, which may be a game called after seven or a crawl that
  lost the eighth and ninth. It is reported rather than filled.

Usage:
    python3 -m scripts.maintenance.backfill_game_scores_from_innings
    python3 -m scripts.maintenance.backfill_game_scores_from_innings --apply
    python3 -m scripts.maintenance.backfill_game_scores_from_innings --year 2023 --apply

Writing is opt-in: without ``--apply`` the run reports what it would do and
changes nothing.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal
from src.models.game import Game, GameInningScore

# Imported rather than reimplemented. Two copies of this arithmetic would drift,
# and a backfill that disagrees with the save path would write different totals
# for the same game depending on which one ran last.
from src.repositories.game_save import _innings_are_complete, _score_from_innings

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

#: Statuses whose games have been played to a result.
FINISHED_STATUSES = ("COMPLETED", "FINAL", "DRAW", "종료")


@dataclass
class BackfillReport:
    """What one run of the backfill found and did.

    The three skip reasons are kept apart because they mean different things to
    whoever reads the summary. Most missing scores in this database belong to
    games that were never crawled in this detail -- there are thousands of those
    and nothing is wrong with them. A line that is present but unusable is a
    defect and there is one. Merging them would report thousands of defects.
    """

    scanned: int = 0
    filled: int = 0
    #: Games with no score *and* no innings for at least one side: not crawled.
    skipped_not_crawled: list[str] = field(default_factory=list)
    #: Games whose line is too short to be a whole game, so its sum is partial.
    skipped_incomplete_line: list[str] = field(default_factory=list)
    #: Games whose innings are the right length but hold no number to sum.
    skipped_no_runs: list[str] = field(default_factory=list)

    def render(self) -> str:
        """Return a human-readable summary."""
        return "\n".join(
            [
                f"scanned                 : {self.scanned}",
                f"filled                  : {self.filled}",
                f"skipped (not crawled)   : {len(self.skipped_not_crawled)}",
                f"skipped (short line)    : {len(self.skipped_incomplete_line)}",
                f"skipped (no run numbers): {len(self.skipped_no_runs)}",
                *[f"    short line: {game_id}" for game_id in self.skipped_incomplete_line],
                *[f"    no numbers: {game_id}" for game_id in self.skipped_no_runs],
            ],
        )


def _candidate_games(session: Session, year: int | None) -> list[Game]:
    """Return finished games that are missing a team total."""
    stmt = (
        select(Game)
        .where(
            Game.is_primary.is_(True),
            Game.game_status.in_(FINISHED_STATUSES),
            (Game.home_score.is_(None)) | (Game.away_score.is_(None)),
        )
        .order_by(Game.game_date, Game.game_id)
    )
    if year is not None:
        stmt = stmt.where(Game.game_date >= f"{year}-01-01", Game.game_date <= f"{year}-12-31")
    return list(session.execute(stmt).scalars())


def _innings_by_game(session: Session, game_ids: Sequence[str]) -> dict[str, list[dict[str, Any]]]:
    """Return the stored inning rows for each game, grouped by game."""
    rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if not game_ids:
        return rows
    stmt = select(
        GameInningScore.game_id,
        GameInningScore.team_side,
        GameInningScore.inning,
        GameInningScore.runs,
    ).where(GameInningScore.game_id.in_(list(game_ids)))
    for game_id, team_side, inning, runs in session.execute(stmt):
        rows[game_id].append({"team_side": team_side, "inning": inning, "runs": runs})
    return rows


def _skip_reason(rows: list[dict[str, Any]]) -> str | None:
    """Return why a game's innings cannot determine its total, or None if they can.

    The three reasons are separate because they describe different things: a game
    nobody crawled, a line too short to be a whole game, and a line of the right
    length with no numbers in it. Only the middle one is a defect, and there is
    one stored game in that state.
    """
    sides = {row["team_side"] for row in rows}
    if sides != {"home", "away"}:
        return "not_crawled"
    if not _innings_are_complete(rows):
        return "short_line"
    if any(_score_from_innings(rows, side) is None for side in ("home", "away")):
        return "no_runs"
    return None


def _write_missing_totals(game: Game, rows: list[dict[str, Any]], *, apply: bool) -> None:
    """Set the blank side of each total from the innings, when writing is on.

    A side the source stated is left alone; a sum is not a correction for it.
    """
    for side in ("home", "away"):
        field = f"{side}_score"
        if getattr(game, field) is not None:
            continue
        total = _score_from_innings(rows, side)
        logger.info("%s %s <- %s (from innings)", game.game_id, field, total)
        if apply:
            setattr(game, field, total)


def backfill_game_scores(
    *,
    apply: bool = False,
    year: int | None = None,
    session: Session | None = None,
) -> BackfillReport:
    """Derive missing team totals from stored innings for finished games.

    Args:
        apply: Write the derived totals. Without it nothing is changed.
        year: Only consider games from this season.
        session: Use an existing session instead of opening one.

    Returns:
        A summary of what was scanned, written and skipped.

    """
    report = BackfillReport()
    own_session = session is None
    db = session or SessionLocal()
    try:
        candidates = _candidate_games(db, year)
        report.scanned = len(candidates)
        innings = _innings_by_game(db, [game.game_id for game in candidates])

        for game in candidates:
            rows = innings.get(game.game_id, [])
            reason = _skip_reason(rows)
            if reason == "not_crawled":
                report.skipped_not_crawled.append(game.game_id)
            elif reason == "short_line":
                report.skipped_incomplete_line.append(game.game_id)
            elif reason == "no_runs":
                report.skipped_no_runs.append(game.game_id)
            else:
                _write_missing_totals(game, rows, apply=apply)
                report.filled += 1

        if apply:
            db.commit()
    finally:
        if own_session:
            db.close()
    return report


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI entrypoint."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Derive missing team totals from stored inning lines",
    )
    parser.add_argument("--year", type=int, default=None, help="Target season (default: all)")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the derived totals to the database (default: dry-run)",
    )
    args = parser.parse_args(argv)

    report = backfill_game_scores(apply=args.apply, year=args.year)

    mode = "APPLIED" if args.apply else "DRY RUN (nothing written)"
    print(f"[{mode}]")
    print(report.render())
    return 0


if __name__ == "__main__":
    sys.exit(main())
