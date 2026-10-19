"""Void the result of a game whose own play-by-play declares it a no-game.

A game called before five innings has no result to record -- five is the
league's minimum for a winner to exist, which is why such a game is declared a
no-game and replayed. This database cannot say that. ``game_status`` has no
no-game value, the schedule parser has no label for it, and
``derive_stable_game_status`` reads only the scores, so a partial line score
written during the stoppage is promoted to a final one and both teams count a
game that produced nothing.

``20260830LGLT0`` is that row. It was called after three innings of rain on
2026-08-30 and is stored as COMPLETED, 0-2. The declaration is not inferred:
it is in the game's own play-by-play, crawled text, as the last event -- "3회말
1번타순 3구 후 우천으로 인한 노게임 선언".

That is also the guard. This script does not void a game because a threshold
says it is short; it voids a game **whose stored play-by-play declares it
void** and which is also structurally too short to be a result. The two
conditions together are what makes this a correction rather than a guess, and
both are read from rows that already exist.

What is written, and what is left alone
---------------------------------------
The four columns that carry the result are cleared and the status becomes
CANCELLED: the closest value this schema has for "no result", chosen because no
durable one exists. Scores are cleared rather than left alone because a
cancelled row carrying scores is exactly what ``game_cancelled_with_scores``
reports, and because a no-game has no score to carry.

Everything the teams actually did stays: the six inning rows, the lineups, the
box score, the pitching lines and the play-by-play. Those describe three
innings that were genuinely played, and the play-by-play holds the declaration
this repair depends on. Removing them would delete the evidence for the change
that removed them.

The limitation, stated because it is not fixable from here
----------------------------------------------------------
**This correction does not survive a status refresh of that date.**
``_evaluate_game`` refills a NULL score from the stored innings whenever it
evaluates a past game that has them, and ``_derive_game_status`` then reads the
restored score plus the box-score body and returns COMPLETED -- it never consults
the status that was already there. Any later
``refresh_game_status_for_date("2026-08-30")`` therefore puts the 0-2 and the
COMPLETED back.

Daily operation does not reach it: the pipeline refreshes its own target date,
and the past-game recovery sweep looks back fourteen days. The exposure is a
historical refresh of that specific date.

Making the verdict durable needs a value for it in the status vocabulary and a
derivation rule that preserves it -- the root cause, deliberately left out of
this change. Until then ``game_final_with_too_few_innings`` is the tripwire: a
reverted row is a finished game under five innings again, so the reversal
becomes a reported failure instead of a silent one. Every audit row this script
writes carries the same warning, so the next reader of the row finds it there
and not only here.

Usage:
    python3 -m scripts.maintenance.repair_no_game_declared
    python3 -m scripts.maintenance.repair_no_game_declared --apply
    python3 -m scripts.maintenance.repair_no_game_declared --game-id 20260830LGLT0 --apply

Writing is opt-in: without ``--apply`` the run reports what it would do and
changes nothing.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import distinct, func, select

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal
from src.models.audit_trail import CorrectionAuditTrail
from src.models.game import Game, GameInningScore, GamePlayByPlay
from src.utils.game_status import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DRAW,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

#: Statuses that assert a result was reached.
FINISHED_STATUSES = (GAME_STATUS_COMPLETED, "FINAL", GAME_STATUS_DRAW, "종료")

#: Lowest inning count that can still produce a result. Below it the league
#: declares a no-game, so a finished row in this range is the shape this
#: repair exists for.
MIN_RESULT_INNINGS = 5

#: The four columns that together state a result. Clearing the status alone
#: would leave scores that no longer belong to any game, and clearing the scores
#: alone would leave a winner for a result that never happened.
RESULT_FIELDS: tuple[str, ...] = (
    "game_status",
    "home_score",
    "away_score",
    "winning_team",
    "winning_score",
)

#: Written onto the status audit row so the evidence travels with the record.
AUDIT_ENTITY_TYPE = "game"
#: Names this script as the actor, matching how audit rows name theirs
#: (``corrected_source`` is who changed it, ``raw_source`` is where the old
#: value was read from).
AUDIT_ACTOR = "repair_no_game_declared"
AUDIT_RAW_STATUS_SOURCE = "stored_game_status"
AUDIT_RAW_SCORE_SOURCE = "stored_game_score"

#: Warns the next reader that the row can be reverted by a refresh.
REVERSAL_WARNING = (
    "Reversal path: refresh_game_status_for_date for this date refills the NULL "
    "score from the stored innings and derive_stable_game_status then returns "
    "COMPLETED from the restored score plus the box-score body, without reading "
    "the stored status. game_final_with_too_few_innings reports that state when "
    "it happens. A durable fix needs a no-game value in the status vocabulary "
    "and a derivation rule that preserves it."
)


@dataclass
class RepairReport:
    """What one run found, and what it would do or did do.

    ``applied`` exists because a dry run must not claim work it did not do.
    Without it the same list renders as ``repaired`` in both modes, and the
    header alone would have to carry the difference -- which is the kind of
    small dishonesty that gets read as a real write later.
    """

    applied: bool
    scanned: int = 0
    repaired: list[str] = field(default_factory=list)
    #: Structurally impossible but carrying no declaration to justify voiding it.
    skipped_no_declaration: list[str] = field(default_factory=list)
    #: The declaration text, per repaired game, so the run records its evidence.
    evidence: dict[str, str] = field(default_factory=dict)

    def render(self) -> str:
        """Return a human-readable summary."""
        action = "repaired" if self.applied else "would repair"
        return "\n".join(
            [
                f"scanned (finished, both sides, < {MIN_RESULT_INNINGS} away innings): {self.scanned}",
                f"{action:<26}: {len(self.repaired)}",
                f"skipped (no declaration)   : {len(self.skipped_no_declaration)}",
                *[f"    {action}: {game_id}" for game_id in self.repaired],
                *[f"    no declaration: {game_id}" for game_id in self.skipped_no_declaration],
                *[f"    evidence {game_id}: {text}" for game_id, text in self.evidence.items()],
            ],
        )


def _short_games(session: Session, game_id: str | None) -> list[Game]:
    """Return finished games whose inning line cannot produce a result.

    The filter is the same shape as the ``game_final_with_too_few_innings``
    check: a stored result, both sides' innings present, and the away side
    short of ``MIN_RESULT_INNINGS``. Keeping the two identical is deliberate --
    a game this returns and the check would report are the same set, so a row
    that slips past one slips past the other, which is exactly what should not
    be possible. Matching them here rather than in Python also keeps the scan
    to one statement instead of one per finished game.
    """
    away_innings = (
        select(func.count(distinct(GameInningScore.inning)))
        .where(
            GameInningScore.game_id == Game.game_id,
            GameInningScore.team_side == "away",
        )
        .scalar_subquery()
    )
    both_sides = (
        select(GameInningScore.game_id)
        .where(GameInningScore.game_id == Game.game_id)
        .group_by(GameInningScore.game_id)
        .having(func.count(distinct(GameInningScore.team_side)) == 2)
        .exists()
    )
    stmt = (
        select(Game)
        .where(
            Game.is_primary.is_(True),
            Game.game_status.in_(FINISHED_STATUSES),
            both_sides,
            away_innings > 0,
            away_innings < MIN_RESULT_INNINGS,
        )
        .order_by(Game.game_date, Game.game_id)
    )
    if game_id is not None:
        stmt = stmt.where(Game.game_id == game_id)
    return list(session.execute(stmt).scalars())


def _declaration_text(session: Session, game_id: str, pattern: str) -> str | None:
    """Return the stored play-by-play line that declares this game void.

    The evidence is read from rows the crawler already stored. A game is only
    repairable when its own record says so, which is why the pattern is matched
    against the play-by-play rather than assumed from the inning count.
    """
    rows = session.execute(
        select(GamePlayByPlay.play_description)
        .where(
            GamePlayByPlay.game_id == game_id,
            GamePlayByPlay.play_description.contains(pattern),
        )
        .order_by(GamePlayByPlay.inning, GamePlayByPlay.inning_half)
    ).all()
    if not rows:
        return None
    return "; ".join(str(text) for (text,) in rows)


def _write_void(
    session: Session,
    game: Game,
    evidence: str,
    *,
    apply: bool,
) -> None:
    """Clear the result columns and record why, when writing is on."""
    pre_image = {name: getattr(game, name) for name in RESULT_FIELDS}
    reason = (
        "no-game declared before five innings; the stored play-by-play records "
        f"the declaration ({evidence}) and the inning line is shorter than any "
        f"game that can produce a result (< {MIN_RESULT_INNINGS} away innings). "
        "The played innings, lineups, box score and play-by-play are kept: they "
        "describe what was genuinely played and hold the declaration itself. " + REVERSAL_WARNING
    )
    logger.info(
        "%s %s: %s -> CANCELLED, scores/winner cleared%s",
        game.game_id,
        RESULT_FIELDS,
        pre_image,
        "" if apply else " (dry run, not written)",
    )
    if not apply:
        return
    game.game_status = GAME_STATUS_CANCELLED
    game.home_score = None
    game.away_score = None
    game.winning_team = None
    game.winning_score = None
    session.add(
        CorrectionAuditTrail(
            game_id=game.game_id,
            entity_type=AUDIT_ENTITY_TYPE,
            entity_id=game.game_id,
            field_name="game_status",
            raw_value=str(pre_image["game_status"]),
            raw_source=AUDIT_RAW_STATUS_SOURCE,
            corrected_value=GAME_STATUS_CANCELLED,
            corrected_source=AUDIT_ACTOR,
            correction_reason=reason,
            confidence=1.0,
            extra_metadata={
                "pre_image": {name: str(value) for name, value in pre_image.items()},
                "evidence": evidence,
            },
        )
    )
    for name in ("home_score", "away_score", "winning_team", "winning_score"):
        session.add(
            CorrectionAuditTrail(
                game_id=game.game_id,
                entity_type=AUDIT_ENTITY_TYPE,
                entity_id=game.game_id,
                field_name=name,
                raw_value=None if pre_image[name] is None else str(pre_image[name]),
                raw_source=AUDIT_RAW_SCORE_SOURCE,
                corrected_value=None,
                corrected_source=AUDIT_ACTOR,
                correction_reason=reason,
                confidence=1.0,
            )
        )


def repair_no_games(
    *,
    apply: bool = False,
    game_id: str | None = None,
    declaration_pattern: str = "노게임",
    session: Session | None = None,
) -> RepairReport:
    """Void the result of every game whose own play-by-play declares it void.

    Args:
        apply: Write the change. Without it nothing is modified.
        game_id: Only consider this game.
        declaration_pattern: Text that must appear in the stored play-by-play.
        session: Use an existing session instead of opening one.

    Returns:
        A summary of what was found, voided and skipped, with the evidence.

    Re-running is a no-op, and not because there is a "already done" check --
    there is none. Voiding a row changes its status out of ``FINISHED_STATUSES``,
    so it leaves the candidate set and the next run scans zero games. The same
    is true in reverse: if a status refresh puts the result back, the row is a
    finished game under five innings again and this finds it again, which is the
    repair half of the tripwire
    ``game_final_with_too_few_innings`` reports the other half of.

    """
    report = RepairReport(applied=apply)
    own_session = session is None
    db = session or SessionLocal()
    try:
        candidates = _short_games(db, game_id)
        report.scanned = len(candidates)
        for game in candidates:
            evidence = _declaration_text(db, game.game_id, declaration_pattern)
            if evidence is None:
                report.skipped_no_declaration.append(game.game_id)
            else:
                _write_void(db, game, evidence, apply=apply)
                report.repaired.append(game.game_id)
                report.evidence[game.game_id] = evidence
        if apply:
            db.commit()
    finally:
        if own_session:
            db.close()
    return report


def main(argv: list[str] | None = None) -> int:
    """Run the CLI entrypoint."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Void the result of a game whose play-by-play declares it a no-game",
    )
    parser.add_argument("--game-id", default=None, help="Only consider this game")
    parser.add_argument(
        "--declaration-pattern",
        default="노게임",
        help="Text the stored play-by-play must contain (default: 노게임)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the change to the database (default: dry-run)",
    )
    args = parser.parse_args(argv)

    report = repair_no_games(
        apply=args.apply,
        game_id=args.game_id,
        declaration_pattern=args.declaration_pattern,
    )

    mode = "APPLIED" if args.apply else "DRY RUN (nothing written)"
    print(f"[{mode}]")
    print(report.render())
    if report.skipped_no_declaration:
        print(
            "\nA short line with no declaration was left alone on purpose. A "
            "truncated crawl and a game called early look identical from the "
            "innings, so only the stored declaration can settle it."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
