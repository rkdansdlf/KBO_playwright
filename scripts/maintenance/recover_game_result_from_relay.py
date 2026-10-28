"""Recover a finished game's result from the relay when no source could deliver it.

The relay feed is the one source that kept working for ``20261026HTSS0``. It
carried the game to its closing marker -- 9 innings, 582 play-by-play rows, 77
at-bats -- and the at-bats carry the running score, which ends at KIA 5 : 삼성 1.
Nothing else arrived. ``game_detail`` was attempted eight times between 18:00 and
20:00 that evening and failed every time (``naver_record_unavailable``), and no
attempt was made after the game ended, so no boxscore, no score and no innings
row was ever written.

The status column then told the rest of the system that the game never happened:
``refresh_game_status_for_date`` had written ``CANCELLED`` from a pre-game signal
at 18:01, and because the row had metadata and none of the detail it counts --
play-by-play was not among them -- every later re-derivation read the same rows
and agreed. The cancellation removed the game from the detail targets, so the one
collection that could have corrected it was the one the cancellation prevented.

What this script reads, and why each condition is one
-----------------------------------------------------
* the relay feed's closing marker -- the feed says the game ended;
* the last at-bat that carries a score -- the result, taken from the row the feed
  wrote rather than inferred from innings or from the status column;
* the game date is in the past -- the result is not still being finalised;
* ``is_primary`` -- retired duplicate rows are not what a reader sees;
* the date is inside the writable window -- a season the historical policy
  refuses is reported and left alone, exactly as the status refresh leaves it.

The score is only recovered for the score columns. Play-by-play and events are
counted and the boxscore is *not* written: those rows did not arrive, and saying
otherwise would be inventing a box score. Fetching them still needs the source;
this script recovers only what the relay actually delivered.

Usage:
    python3 -m scripts.maintenance.recover_game_result_from_relay
    python3 -m scripts.maintenance.recover_game_result_from_relay --apply
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import exists, or_, select

from src.config.historical_backfill_policy import load_historical_backfill_policy
from src.constants import FINAL_PBP_MARKER, KST
from src.db.engine import SessionLocal
from src.models.audit_trail import CorrectionAuditTrail
from src.models.game import Game, GameEvent, GamePlayByPlay
from src.repositories.game_helpers import _resolve_terminal_status
from src.utils.game_state import RELAY_ACTIVE_STATES, validate_transition
from src.utils.game_status import GAME_STATUS_CANCELLED

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

#: ``corrected_source`` written onto every audit row this script produces.
AUDIT_ACTOR = "recover_game_result_from_relay"
AUDIT_ENTITY_TYPE = "game"
#: The value a finished game's lifecycle settles on.
TARGET_LIFECYCLE = "final"
#: Applied before ``final`` when the stored lifecycle is ``running``, because
#: ``validate_transition`` allows ``running -> final`` only through it.
INTERMEDIATE_LIFECYCLE = "result_pending_stabilization"
#: Where the pre-image values came from: the stored columns, not a document.
RAW_SOURCE = "stored_game_columns"
SCORE_RAW_SOURCE = "relay_at_bat_running_score"

REASON_STATUS = (
    "the relay feed wrote its closing marker for this game, and the last stored at-bat "
    "carries a final score, so the game was played to the end. The row said the game "
    "produced no result, which removed it from the detail targets that could have "
    "corrected it; the result is restored from the relay row that recorded it."
)
REASON_SCORE = (
    "the score was never stored because the boxscore collection failed before the game "
    "ended and was never retried afterwards. The relay's last at-bat carries the running "
    "score at the end of the game, which matches the stored final score on every finished "
    "game that has both, so it is read from there rather than invented."
)
REASON_LIFECYCLE = (
    "the lifecycle still marked the game as one the live path should keep polling, so a "
    "finished game was re-fetched every cycle. The result is final, and the contract "
    "allows this transition for a game that has one."
)


@dataclass(frozen=True)
class Recovery:
    """One game this script can (or cannot) restore a result for."""

    game_id: str
    game_date: date
    stored_status: str | None
    stored_lifecycle: str | None
    #: What the row holds now. The backup writes these, so a reader can restore it.
    stored_away_score: int | None
    stored_home_score: int | None
    #: What the relay's last at-bat says, which is what a write would store.
    recovered_away_score: int | None
    recovered_home_score: int | None
    #: False when the row cannot be written -- the policy refuses the season, or no
    #: at-bat carries a score and there is nothing to restore from.
    writable: bool
    skipped_because: str | None = None


def _finished_relay_exists() -> object:
    """Return the correlated ``EXISTS`` that identifies a finished relay.

    Returns:
        A SQLAlchemy ``EXISTS`` clause correlated with the enclosing ``game`` row.

    """
    return exists().where(
        GamePlayByPlay.game_id == Game.game_id,
        GamePlayByPlay.play_description.like(f"%{FINAL_PBP_MARKER}%"),
    )


def _has_missing_result() -> object:
    """Return the clause that selects rows whose result is absent.

    Returns:
        A SQLAlchemy boolean clause: cancelled, or missing either score.

    """
    return or_(
        Game.game_status == GAME_STATUS_CANCELLED,
        Game.home_score.is_(None),
        Game.away_score.is_(None),
    )


def _candidates(session: Session, *, today: date) -> list[Game]:
    """Return the games whose relay finished but whose result is missing.

    Args:
        session: Read-only session.
        today: KST date the "in the past" condition is measured against.

    Returns:
        Matching games, oldest first.

    """
    stmt = (
        select(Game)
        .where(
            Game.game_date < today,
            Game.is_primary.is_(True),
            _has_missing_result(),
            _finished_relay_exists(),
        )
        .order_by(Game.game_date.asc(), Game.game_id.asc())
    )
    return list(session.execute(stmt).scalars().all())


def _relay_final_score(session: Session, game_id: str) -> tuple[int, int] | None:
    """Read the game's final score off the relay's own at-bats.

    The last at-bat that carries a score is the end of the game, because the score
    columns are the running score and the game's last at-bat is the last one
    written. ``20261026HTSS0`` ends on a fly out in the bottom of the 9th.

    Args:
        session: Read-only session.
        game_id: Game whose at-bats are read.

    Returns:
        ``(away_score, home_score)``, or None when no at-bat states a score.

    """
    row = session.execute(
        select(GameEvent.away_score, GameEvent.home_score)
        .where(
            GameEvent.game_id == game_id,
            GameEvent.away_score.isnot(None),
            GameEvent.home_score.isnot(None),
        )
        .order_by(GameEvent.event_seq.desc())
        .limit(1),
    ).first()
    if row is None:
        return None
    return int(row[0]), int(row[1])


def _lifecycle_path(current: str | None) -> tuple[str, ...]:
    """Return the lifecycle values to write, in order, for a finished game.

    Args:
        current: The stored ``game_lifecycle_state``, which may be NULL.

    Returns:
        ``("final",)`` when one step is allowed, ``(intermediate, "final")`` when
        the stored value is ``running``, and ``()`` when the contract allows
        neither -- an unknown value is left alone rather than written past.

    """
    if current is None or current == TARGET_LIFECYCLE:
        # NULL states nothing, so there is no false claim to correct. Writing
        # "final" there would be adding a value, not repairing one.
        return ()
    if current not in RELAY_ACTIVE_STATES:
        return ()
    direct_ok, _ = validate_transition(current, TARGET_LIFECYCLE)
    if direct_ok:
        return (TARGET_LIFECYCLE,)
    step_ok, _ = validate_transition(current, INTERMEDIATE_LIFECYCLE)
    final_ok, _ = validate_transition(INTERMEDIATE_LIFECYCLE, TARGET_LIFECYCLE)
    if step_ok and final_ok:
        return (INTERMEDIATE_LIFECYCLE, TARGET_LIFECYCLE)
    return ()


def survey(session: Session, *, today: date, policy_approved: bool = False) -> list[Recovery]:
    """Report what this script would do, without writing anything.

    Args:
        session: Read-only session.
        today: KST date the "in the past" condition is measured against.
        policy_approved: Treat every season as writable. Only a reviewer sets this.

    Returns:
        One entry per candidate, in game-date order.

    """
    policy = load_historical_backfill_policy()
    recoveries: list[Recovery] = []
    for game in _candidates(session, today=today):
        allowed, skip_reason = policy.status_write_allowed(game.game_date, today)
        score = _relay_final_score(session, game.game_id)
        half_scored = (game.away_score is None) != (game.home_score is None)
        writable = (allowed or policy_approved) and score is not None and not half_scored
        if score is None:
            because = "no at-bat row carries a score to restore from"
        elif half_scored:
            because = "only one side of the stored score is missing; the halves are left for review"
        elif not allowed and not policy_approved:
            because = f"outside the writable window ({skip_reason})"
        else:
            because = None
        recoveries.append(
            Recovery(
                game_id=game.game_id,
                game_date=game.game_date,
                stored_status=game.game_status,
                stored_lifecycle=game.game_lifecycle_state,
                stored_away_score=game.away_score,
                stored_home_score=game.home_score,
                recovered_away_score=None if score is None else score[0],
                recovered_home_score=None if score is None else score[1],
                writable=writable,
                skipped_because=because,
            ),
        )
    return recoveries


def _backup_path(today: date) -> str:
    """Return the pre-image backup path for one run.

    Args:
        today: KST date used in the file name.

    Returns:
        A path under ``data/repair_backups``.

    """
    stamp = datetime.now(KST).strftime("%Y%m%dT%H%M%S")
    return f"data/repair_backups/recover_from_relay_{today:%Y%m%d}_{stamp}.csv"


def _write_backup(recoveries: Sequence[Recovery], path: str) -> None:
    """Write the pre-image of every row about to change.

    The stored columns, not the recovered ones. A backup that holds the value the
    run is about to write cannot restore anything, which defeats the point of
    writing it before the write.

    Args:
        recoveries: The rows this run will write.
        path: Destination CSV path.

    """
    from pathlib import Path

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "game_id",
                "game_date",
                "game_status",
                "game_lifecycle_state",
                "away_score",
                "home_score",
                "recovered_away_score",
                "recovered_home_score",
            ],
        )
        for item in recoveries:
            writer.writerow(
                [
                    item.game_id,
                    item.game_date,
                    item.stored_status,
                    item.stored_lifecycle,
                    item.stored_away_score,
                    item.stored_home_score,
                    item.recovered_away_score,
                    item.recovered_home_score,
                ],
            )
    logger.info("Pre-image backup written to %s", path)


def _audit(
    session: Session,
    *,
    game: Game,
    field_name: str,
    raw_value: object,
    raw_source: str,
    corrected_value: object,
    reason: str,
) -> None:
    """Record one field change on the audit trail.

    Args:
        session: Session the row is added to.
        game: Game being corrected.
        field_name: Column that changed.
        raw_value: Value before the change.
        raw_source: Where the pre-image was read from.
        corrected_value: Value written.
        reason: Why the change is justified.

    """
    session.add(
        CorrectionAuditTrail(
            game_id=game.game_id,
            entity_type=AUDIT_ENTITY_TYPE,
            entity_id=game.game_id,
            field_name=field_name,
            raw_value=None if raw_value is None else str(raw_value),
            raw_source=raw_source,
            corrected_value=None if corrected_value is None else str(corrected_value),
            corrected_source=AUDIT_ACTOR,
            correction_reason=reason,
            confidence=1.0,
            extra_metadata={
                "game_date": str(game.game_date),
                "stored_status": game.game_status,
                "stored_lifecycle": game.game_lifecycle_state,
            },
        ),
    )


def recover(*, apply: bool = False, session: Session | None = None, today: date | None = None) -> list[str]:
    """Restore the results of finished games the relay delivered.

    Args:
        apply: Write the corrections. When False nothing is modified.
        session: Optional session; one is opened when omitted.
        today: Optional KST date override, for tests.

    Returns:
        The game ids that were recovered (or would be, on a dry run).

    """
    own_session = session is None
    db = session or SessionLocal()
    target_date = today or datetime.now(KST).date()
    try:
        recovered = _recover(db, apply=apply, today=target_date)
        if apply:
            # The session context manager closes without committing, so a run
            # that only calls ``add`` reports success and writes nothing.
            db.commit()
        return recovered
    finally:
        if own_session:
            db.close()


def _apply_one(session: Session, *, game: Game, score: tuple[int, int]) -> None:
    """Write one game's recovered result, auditing every field that changes.

    Only a fully absent score is written. A row that already carries one side is
    left to the review path: filling the other half from a second source would
    paper over a disagreement between them, and a disagreement is the thing a
    reader needs to see.

    Args:
        session: Session the changes are added to.
        game: Game being corrected.
        score: ``(away_score, home_score)`` read from the relay.

    """
    if game.away_score is None and game.home_score is None:
        for field_name, corrected in (("away_score", score[0]), ("home_score", score[1])):
            _audit(
                session,
                game=game,
                field_name=field_name,
                raw_value=getattr(game, field_name),
                raw_source=SCORE_RAW_SOURCE,
                corrected_value=corrected,
                reason=REASON_SCORE,
            )
            setattr(game, field_name, corrected)

    next_status = _resolve_terminal_status(score[1], score[0])
    if game.game_status != next_status:
        _audit(
            session,
            game=game,
            field_name="game_status",
            raw_value=game.game_status,
            raw_source=RAW_SOURCE,
            corrected_value=next_status,
            reason=REASON_STATUS,
        )
        game.game_status = next_status

    for step in _lifecycle_path(game.game_lifecycle_state):
        _audit(
            session,
            game=game,
            field_name="game_lifecycle_state",
            raw_value=game.game_lifecycle_state,
            raw_source=RAW_SOURCE,
            corrected_value=step,
            reason=REASON_LIFECYCLE,
        )
        game.game_lifecycle_state = step

    session.add(game)


def _recover(session: Session, *, apply: bool, today: date) -> list[str]:
    entries = survey(session, today=today)
    if not entries:
        logger.info("No finished relay is missing its result.")
        return []

    skipped = [item for item in entries if not item.writable]
    for item in skipped:
        logger.warning("%s (%s): left alone -- %s", item.game_id, item.game_date, item.skipped_because)

    writable = [item for item in entries if item.writable]
    if not writable:
        logger.info("%d candidate(s) reported, none writable.", len(entries))
        return []

    if apply:
        _write_backup(writable, _backup_path(today))

    recovered: list[str] = []
    for item in writable:
        # ``Game`` is keyed by its integer ``id``; the game id is a business key,
        # so it is looked up by that column rather than handed to ``get``.
        game = session.execute(select(Game).where(Game.game_id == item.game_id)).scalars().first()
        if game is None:  # pragma: no cover - the row was selected from this session
            continue
        score = (item.recovered_away_score, item.recovered_home_score)
        logger.info(
            "%s (%s): %s %s-%s -> %s %s-%s%s",
            game.game_id,
            game.game_date,
            game.game_status,
            game.away_score,
            game.home_score,
            _resolve_terminal_status(score[1], score[0]),
            score[0],
            score[1],
            "" if apply else " (dry run, not written)",
        )
        recovered.append(game.game_id)
        if apply:
            _apply_one(session, game=game, score=score)

    logger.info(
        "%d game(s) %s.",
        len(recovered),
        "recovered" if apply else "would be recovered (dry run; pass --apply to write)",
    )
    return recovered


def main(argv: Sequence[str] | None = None) -> int:
    """Run the recovery from the command line.

    Args:
        argv: Optional argument list.

    Returns:
        Process exit code.

    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--apply", action="store_true", help="Write the corrections (default: dry run)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    recover(apply=args.apply)
    return 0


if __name__ == "__main__":
    sys.exit(main())
