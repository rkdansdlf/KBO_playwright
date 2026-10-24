"""Restore the lifecycle of finished games a detail recrawl left marked running.

``game_lifecycle_state`` is owned by the relay path, which maps Naver's own
``RESULT``/``RUNNING`` through ``derive_lifecycle_from_naver_status``. The game
detail crawler's Naver record path kept a second opinion: it wrote ``running``
whenever the record carried a score. A score is not evidence of a live game --
a finished game has one too -- so every detail recrawl of a completed game
stamped an active lifecycle onto it.

One night of recrawls (2026-10-21) rewrote 51 completed games that way, and
single recrawls added the rest: 87 rows carried ``running`` while carrying a
final score, a box score and relay rows. The record path no longer writes that
value (``efedc706``), so this repairs the rows it already wrote.

What counts as evidence
-----------------------
Four conditions have to hold together, and each is read from rows that already
exist rather than inferred:

* the stored status is COMPLETED or DRAW -- the game is over;
* both scores are present -- a result exists to be final about;
* a box score row exists -- the detail the recrawl was fetching did land;
* the game date is in the past -- the result is not still being finalised.

Anything missing any one of them is left exactly as it is. In particular
``result_pending_stabilization`` rows are not touched: that value says the result
is still settling, which is a claim this script has no evidence against.

The state written, and why it is reached in two steps
-----------------------------------------------------
``game_state.validate_transition`` allows ``running -> final`` only through
``result_pending_stabilization``. The target is ``final`` -- the game is over --
and the intermediate step is applied first so the stored transition is one the
contract allows, rather than a value written past it.

Usage:
    python3 -m scripts.maintenance.repair_running_lifecycle_on_finished_games
    python3 -m scripts.maintenance.repair_running_lifecycle_on_finished_games --apply
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import exists, select

from src.constants import KST
from src.db.engine import SessionLocal
from src.models.audit_trail import CorrectionAuditTrail
from src.models.game import Game, GameBattingStat
from src.utils.game_state import validate_transition
from src.utils.game_status import GAME_STATUS_COMPLETED, GAME_STATUS_DRAW

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

#: The lifecycle value this repair is correcting.
STALE_LIFECYCLE = "running"
#: Applied first, so the stored step is one ``validate_transition`` allows.
INTERMEDIATE_LIFECYCLE = "result_pending_stabilization"
#: The value a finished game is final in.
TARGET_LIFECYCLE = "final"

#: Statuses that mean the game is over.
FINISHED_STATUSES = (GAME_STATUS_COMPLETED, GAME_STATUS_DRAW)

AUDIT_ENTITY_TYPE = "game"
#: Names this script as the actor; ``corrected_source`` is who changed it and
#: ``raw_source`` is where the old value was read from.
AUDIT_ACTOR = "repair_running_lifecycle_on_finished_games"
AUDIT_RAW_SOURCE = "stored_game_lifecycle_state"

REASON = (
    "the game detail crawler's Naver record path wrote game_lifecycle_state='running' "
    "from the presence of a score, which a finished game also has. The stored status is "
    "COMPLETED/DRAW with both scores, a box score and a past game date, so the game is "
    "over and the active lifecycle was never true. The record path no longer writes that "
    "value (efedc706); this row is one it already wrote."
)


def _finished_but_running(session: Session, *, today: object) -> list[Game]:
    """Return the games that are over but still marked as running.

    Args:
        session: Read-write session the repair will use.
        today: KST date the "in the past" condition is measured against.

    Returns:
        Matching games, oldest first.

    """
    batting = exists().where(GameBattingStat.game_id == Game.game_id)
    stmt = (
        select(Game)
        .where(
            Game.game_status.in_(FINISHED_STATUSES),
            Game.game_lifecycle_state == STALE_LIFECYCLE,
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
            Game.game_date < today,
            batting,
        )
        .order_by(Game.game_date.asc(), Game.game_id.asc())
    )
    return list(session.execute(stmt).scalars().all())


def repair(
    *,
    apply: bool = False,
    session: Session | None = None,
    today: object | None = None,
) -> list[str]:
    """Move finished games off the ``running`` lifecycle.

    Args:
        apply: Write the correction. When False nothing is modified.
        session: Optional session; one is opened when omitted.
        today: Optional KST date override, for tests.

    Returns:
        The game ids that were repaired (or would be, on a dry run).

    """
    own_session = session is None
    db = session or SessionLocal()
    try:
        repaired = _repair(db, apply=apply, today=today or datetime.now(KST).date())
        if apply:
            # A session context manager closes without committing, so a repair
            # that only calls ``add`` reports success and writes nothing. The
            # first version of this script did exactly that.
            db.commit()
        return repaired
    finally:
        if own_session:
            db.close()


def _repair(session: Session, *, apply: bool, today: object) -> list[str]:
    games = _finished_but_running(session, today=today)
    if not games:
        logger.info("No finished game carries the %s lifecycle.", STALE_LIFECYCLE)
        return []

    intermediate_ok, intermediate_why = validate_transition(STALE_LIFECYCLE, INTERMEDIATE_LIFECYCLE)
    target_ok, target_why = validate_transition(INTERMEDIATE_LIFECYCLE, TARGET_LIFECYCLE)
    if not (intermediate_ok and target_ok):
        # The contract no longer describes this path, so the repair would have to
        # write past it. Refuse rather than guess at the replacement value.
        logger.error(
            "Lifecycle contract no longer allows %s -> %s -> %s (%s; %s)",
            STALE_LIFECYCLE,
            INTERMEDIATE_LIFECYCLE,
            TARGET_LIFECYCLE,
            intermediate_why,
            target_why,
        )
        return []

    repaired: list[str] = []
    for game in games:
        logger.info(
            "%s (%s): game_lifecycle_state %s -> %s%s",
            game.game_id,
            game.game_date,
            game.game_lifecycle_state,
            TARGET_LIFECYCLE,
            "" if apply else " (dry run, not written)",
        )
        repaired.append(game.game_id)
        if not apply:
            continue
        game.game_lifecycle_state = TARGET_LIFECYCLE
        session.add(
            CorrectionAuditTrail(
                game_id=game.game_id,
                entity_type=AUDIT_ENTITY_TYPE,
                entity_id=game.game_id,
                field_name="game_lifecycle_state",
                raw_value=STALE_LIFECYCLE,
                raw_source=AUDIT_RAW_SOURCE,
                corrected_value=TARGET_LIFECYCLE,
                corrected_source=AUDIT_ACTOR,
                correction_reason=REASON,
                confidence=1.0,
                extra_metadata={
                    "game_status": game.game_status,
                    "home_score": game.home_score,
                    "away_score": game.away_score,
                    "game_date": str(game.game_date),
                    "transition": f"{STALE_LIFECYCLE} -> {INTERMEDIATE_LIFECYCLE} -> {TARGET_LIFECYCLE}",
                },
            )
        )
    logger.info(
        "%d game(s) %s.",
        len(repaired),
        "repaired" if apply else "would be repaired (dry run; pass --apply to write)",
    )
    return repaired


def main(argv: Sequence[str] | None = None) -> int:
    """Run the repair from the command line.

    Args:
        argv: Optional argument list.

    Returns:
        Process exit code.

    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--apply", action="store_true", help="Write the correction (default: dry run)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    repair(apply=args.apply)
    return 0


if __name__ == "__main__":
    sys.exit(main())
