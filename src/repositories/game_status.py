"""Game status management: update, refresh, and derive game status."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlalchemy.orm import Session
from zoneinfo import ZoneInfo

from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError

from src.config.historical_backfill_policy import (
    HistoricalBackfillPolicy,
    load_historical_backfill_policy,
)
from src.db.engine import get_db_session
from src.models.game import (
    Game,
    GameBattingStat,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
)
from src.repositories.game_helpers import (
    _canonicalize_game_id,
    _derive_game_status,
    _has_game_child_rows,
)
from src.utils.date_helpers import parse_date_str

logger = logging.getLogger(__name__)

#: ``corrected_source`` written onto every audit row this module produces.
AUDIT_SOURCE = "refresh_game_status"

#: Where the pre-image value came from. The refresh path reads the stored status
#: rather than a source document, so it says so instead of borrowing a source
#: name it did not actually consult.
AUDIT_RAW_SOURCE = "stored_game_status"

_EVIDENCE_KEYS = (
    "has_metadata",
    "has_inning_scores",
    "has_lineups",
    "has_batting",
    "has_pitching",
)


@dataclass(frozen=True)
class StatusTransition:
    """One derived status change and the evidence that produced it.

    The evidence is what a reader needs in order to disagree with the change.
    Without it the audit row says a status moved and nothing about why, which is
    the same silence the 2026-09-23 sweep left behind.

    """

    game_id: str
    from_status: str | None
    to_status: str
    age_days: int
    evidence: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Serialize the transition for the manifest and CLI output."""
        return {
            "game_id": self.game_id,
            "from_status": self.from_status,
            "to_status": self.to_status,
            "age_days": self.age_days,
            "evidence": dict(self.evidence),
        }


def current_season_year(today: date | None = None) -> int:
    """Return the season year used by automatic runs (KST calendar year)."""
    if today is None:
        today = datetime.now(ZoneInfo("Asia/Seoul")).date()
    return today.year


def game_year_of(game: Game) -> int | None:
    """Return the calendar year of a game's date, or None if unknown."""
    game_date = game.game_date
    if game_date is None:
        return None
    return getattr(game_date, "year", None)


def automatic_write_allowed(game_year: int | None, season_year: int) -> bool:
    """Automatic runs may write only games from the current season.

    Anything else (past seasons, unknown dates) needs an explicit operator run
    with ``allow_historical=True`` (issue #32).
    """
    return game_year is not None and game_year == season_year


def update_game_status(
    game_id: str, status: str, session: Session | None = None, *, allow_historical: bool = False
) -> bool:
    """Update one game's status.

    Guarded: rows outside the current season are refused unless ``allow_historical``
    is True. Explicit operator runs pass game IDs and opt in; automatic scans do not.

    Args:
        game_id: Game ID.
        status: Status.
        session: DB Session.
        allow_historical: Opt in to writes outside the current season.

    """
    if session is None:
        try:
            with get_db_session() as s:
                return update_game_status(game_id, status, session=s, allow_historical=allow_historical)
        except Exception:
            logger.exception("[ERROR] DB Error (Status)")
            return False

    game_id, _ = _canonicalize_game_id(game_id)

    if not game_id or not status:
        return False

    game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    if not game:
        return False
    if not allow_historical and not automatic_write_allowed(game_year_of(game), current_season_year()):
        logger.warning(
            "[GUARD] Refused automatic status write outside the current season: %s (%s -> %s). "
            "Use allow_historical=True from an explicit operator run.",
            game_id,
            game.game_status,
            status,
        )
        return False
    game.game_status = status
    session.flush()
    return True


def _record_transition_audits(session: Session, transitions: Iterable[StatusTransition], target_date: str) -> int:
    """Persist one audit row per transition inside the caller's transaction.

    The rows go in through the caller's session rather than a second one so the
    status change and the record of it commit or roll back together. An audit
    row that survived a rolled-back status write would be worse than no row.

    Args:
        session: Caller-owned session.
        transitions: Transitions to record.
        target_date: Target date, used in the correction reason.

    Returns:
        Number of rows added.

    """
    from src.services.multi_source_reconciler import CorrectionAuditRequest, MultiSourceReconciler

    reconciler = MultiSourceReconciler(session)
    recorded = 0
    for transition in transitions:
        reconciler.record_audit(
            CorrectionAuditRequest(
                game_id=transition.game_id,
                entity_type="game",
                entity_id=transition.game_id,
                field_name="game_status",
                raw_value=transition.from_status,
                raw_source=AUDIT_RAW_SOURCE,
                corrected_value=transition.to_status,
                corrected_source=AUDIT_SOURCE,
                reason=f"refresh:{target_date}",
                confidence=1.0,
                extra_metadata=transition.evidence,
            ),
            session=session,
        )
        recorded += 1
    if recorded:
        session.flush()
    return recorded


@dataclass(frozen=True)
class StatusRefreshOptions:
    """Caller-side choices for one status refresh.

    Grouped so the public signature stays short and so the reporting knobs move
    together: enabling ``dry_run`` without also disabling the audit would report
    a change nobody recorded.

    """

    dry_run: bool = False
    caller: str = "unknown"
    policy: HistoricalBackfillPolicy | None = None


@dataclass(frozen=True)
class _GameEvaluation:
    """Derived status for one game plus the evidence behind it."""

    next_status: str
    evidence: dict[str, Any]


def _fill_scores_from_innings(session: Session, game: Game) -> None:
    """Fill a missing final score from the stored inning lines."""
    inning_totals = dict(
        session.query(GameInningScore.team_side, func.sum(GameInningScore.runs))
        .filter(GameInningScore.game_id == game.game_id)
        .group_by(GameInningScore.team_side)
        .all(),
    )
    if inning_totals.get("away") is None or inning_totals.get("home") is None:
        return
    game.away_score = int(inning_totals["away"] or 0)
    game.home_score = int(inning_totals["home"] or 0)


@dataclass(frozen=True)
class _EvaluationContext:
    """Per-run facts every game evaluation needs."""

    today: date
    write: bool
    caller: str
    dry_run: bool


def _game_year(game: Game) -> int | None:
    """Return the stored season year of a game row, or None when unreadable."""
    year = getattr(game.game_date, "year", None)
    if year is not None:
        return int(year)
    try:
        return int(str(game.game_date)[:4])
    except ValueError:
        return None


def _evaluate_game(session: Session, game: Game, context: _EvaluationContext) -> _GameEvaluation:
    """Derive one game's status and the evidence that produced it.

    Args:
        session: DB Session.
        game: Game row being evaluated.
        context: Per-run facts: today, whether the caller persists, and attribution.

    Returns:
        The derived status and its evidence.

    """
    has_metadata = _has_game_child_rows(session, GameMetadata, game.game_id)
    has_inning = _has_game_child_rows(session, GameInningScore, game.game_id)
    has_lineup = _has_game_child_rows(session, GameLineup, game.game_id)
    has_batting = _has_game_child_rows(session, GameBattingStat, game.game_id)
    has_pitching = _has_game_child_rows(session, GamePitchingStat, game.game_id)
    if (
        context.write
        and game.game_date < context.today
        and has_inning
        and (game.home_score is None or game.away_score is None)
    ):
        _fill_scores_from_innings(session, game)
    next_status = _derive_game_status(
        game_date=game.game_date,
        home_score=game.home_score,
        away_score=game.away_score,
        current_status=game.game_status,
        has_metadata=has_metadata,
        has_inning_scores=has_inning,
        has_lineups=has_lineup,
        has_batting=has_batting,
        has_pitching=has_pitching,
        today=context.today,
    )
    evidence = dict(
        zip(
            _EVIDENCE_KEYS,
            (has_metadata, has_inning, has_lineup, has_batting, has_pitching),
            strict=True,
        ),
        home_score=game.home_score,
        away_score=game.away_score,
        caller=context.caller,
        dry_run=context.dry_run,
    )
    return _GameEvaluation(next_status=next_status, evidence=evidence)


def _collect_transitions(
    session: Session,
    games: list[Game],
    context: _EvaluationContext,
) -> tuple[list[Game], list[StatusTransition], dict[str, int], dict[str, list[str]]]:
    """Evaluate every game on the date and gather the ones whose status moves.

    Args:
        session: DB Session.
        games: Games on the target date.
        context: Per-run facts.

    Returns:
        The games whose status moved, the transitions found, the status tally and
        the per-status game ids.

    """
    changed_games: list[Game] = []
    transitions: list[StatusTransition] = []
    status_counts: dict[str, int] = {}
    game_ids_by_status: dict[str, list[str]] = {}
    for game in games:
        evaluation = _evaluate_game(session, game, context)
        status_counts[evaluation.next_status] = status_counts.get(evaluation.next_status, 0) + 1
        game_ids_by_status.setdefault(evaluation.next_status, []).append(game.game_id)
        if game.game_status == evaluation.next_status:
            continue
        changed_games.append(game)
        transitions.append(
            StatusTransition(
                game_id=game.game_id,
                from_status=game.game_status,
                to_status=evaluation.next_status,
                age_days=(context.today - game.game_date).days,
                evidence=evaluation.evidence,
            ),
        )
    return changed_games, transitions, status_counts, game_ids_by_status


def refresh_game_status_for_date(
    target_date: str,
    today: date | None = None,
    session: Session | None = None,
    *,
    options: StatusRefreshOptions | None = None,
) -> dict[str, Any]:
    """Recompute game_status only for one target date (YYYYMMDD).

    Every change is reported, and every change that is written also leaves an
    audit row. A date whose season falls outside the policy's grace window is
    reported and left alone: the rewrite is refused, not the run.

    Args:
        target_date: Target date for the operation.
        today: Today.
        session: DB Session.
        options: Dry-run, caller attribution and historical-write policy.

    """
    options = options or StatusRefreshOptions()
    if session is None:
        try:
            with get_db_session() as s:
                return refresh_game_status_for_date(target_date, today, session=s, options=options)
        except SQLAlchemyError:
            logger.exception("[ERROR] DB Error (Status Refresh)")
            return _empty_status_result(target_date)

    try:
        dt = parse_date_str(target_date)
    except ValueError:
        return _empty_status_result(target_date)

    # There is deliberately no pre-2001 floor in front of the policy call. One
    # used to sit here, and because it ran first the policy's ``approved_dates``
    # could never be reached: an approval is the single way a historical date
    # becomes writable, so a guard above it turned every approval into a no-op
    # and made the refusal report no transitions. The policy refuses old seasons
    # by default, so removing the duplicate guard changes no default behaviour
    # -- it only lets a reviewed approval through.
    if today is None:
        today = datetime.now(ZoneInfo("Asia/Seoul")).date()

    policy = options.policy or load_historical_backfill_policy()
    allowed, skip_reason = policy.status_write_allowed(dt, today)
    write = allowed and not options.dry_run
    if not allowed:
        logger.warning(
            "[StatusRefresh] %s is outside the writable window (%s); reporting without writing",
            target_date,
            skip_reason,
        )

    games = session.query(Game).filter(Game.game_date == dt).all()
    # Rows outside the current season are reported rather than rewritten; the
    # policy above is what decides whether the date may be written at all.
    skipped_past_season = sum(1 for game in games if (year := _game_year(game)) is not None and year != today.year)
    context = _EvaluationContext(
        today=today,
        write=write,
        caller=options.caller,
        dry_run=options.dry_run,
    )
    changed_games, transitions, status_counts, game_ids_by_status = _collect_transitions(
        session,
        list(games),
        context,
    )
    audited = 0
    if write and transitions:
        # Assign and record in the caller's transaction so an audit row cannot
        # outlive the status write it describes.
        for game, transition in zip(changed_games, transitions, strict=True):
            game.game_status = transition.to_status
        audited = _record_transition_audits(session, transitions, target_date)
    session.flush()

    transition_counts: dict[str, int] = {}
    for transition in transitions:
        key = f"{transition.from_status}->{transition.to_status}"
        transition_counts[key] = transition_counts.get(key, 0) + 1

    return {
        "target_date": target_date,
        "total": len(games),
        "skipped_past_season": skipped_past_season,
        "updated": len(transitions) if write else 0,
        "status_counts": status_counts,
        "game_ids": sorted(game.game_id for game in games),
        "updated_game_ids": sorted(game.game_id for game in changed_games) if write else [],
        "game_ids_by_status": {status: sorted(ids) for status, ids in game_ids_by_status.items()},
        "dry_run": options.dry_run,
        "write_allowed": allowed,
        "skipped": not allowed,
        "skip_reason": skip_reason,
        "transitions": [transition.to_dict() for transition in transitions],
        "transition_counts": transition_counts,
        "audited": audited,
    }


def _empty_status_result(target_date: str, skip_reason: str = "invalid_target_date") -> dict[str, Any]:
    """Return the result shape used when a refresh refuses or cannot read.

    Every refusal has to answer the same keys. A caller that refreshes a date
    outside its window reads ``skipped`` and ``skip_reason`` to explain itself,
    and a dict carrying only ``total`` and ``updated`` turns the rest of the
    contract into a KeyError for every caller that reads it.

    Args:
        target_date: Target date the caller asked for.
        skip_reason: Why the refresh produced no transitions.

    Returns:
        The result mapping, with no transitions.

    """
    return {
        "target_date": target_date,
        "total": 0,
        "skipped_past_season": 0,
        "updated": 0,
        "status_counts": {},
        "game_ids": [],
        "updated_game_ids": [],
        "game_ids_by_status": {},
        "dry_run": False,
        "write_allowed": False,
        "skipped": True,
        "skip_reason": skip_reason,
        "transitions": [],
        "transition_counts": {},
        "audited": 0,
    }
