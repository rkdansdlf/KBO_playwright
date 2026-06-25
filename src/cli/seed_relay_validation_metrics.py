"""Seed canonical relay validation metrics for completed games.

This command is intentionally conservative: it records a validation state for
every completed/DRAW game so relay gaps are explainable, but it does not invent
PBP rows or events.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import func

from src.constants import KST
from src.db.engine import SessionLocal
from src.models.game import Game, GameEvent, GamePlayByPlay, GameValidationMetrics
from src.services.wpa_transitions import event_has_wpa_state
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES
from src.utils.relay_validation import (
    VALIDATION_SOURCE_INCOMPLETE,
    VALIDATION_SOURCE_UNAVAILABLE,
    VALIDATION_UNVERIFIED,
    VALIDATION_VERIFIED,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def seed_relay_validation_metrics(
    *,
    season: int | None = None,
    mark_legacy_unavailable: bool = True,
) -> dict[str, int]:
    """Seeds relay validation metrics.

    Returns:
        Dictionary result.

    """
    counts: dict[str, int] = {}
    with SessionLocal() as session:
        query = session.query(Game).filter(Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)))
        if season is not None:
            query = query.filter(Game.game_id.like(f"{int(season)}%"))
        games = query.order_by(Game.game_id.asc()).all()
        game_ids = [game.game_id for game in games]
        if not game_ids:
            return counts

        pbp_ids = {
            row[0]
            for row in session.query(GamePlayByPlay.game_id)
            .filter(GamePlayByPlay.game_id.in_(game_ids))
            .distinct()
            .all()
        }
        event_rows = session.query(GameEvent).filter(GameEvent.game_id.in_(game_ids)).all()
        event_ids = {row.game_id for row in event_rows}
        event_state_ids = {row.game_id for row in event_rows if event_has_wpa_state(row)}
        row_counts = {
            game_id: int(count)
            for game_id, count in session.query(GameEvent.game_id, func.count(GameEvent.id))
            .filter(GameEvent.game_id.in_(game_ids))
            .group_by(GameEvent.game_id)
            .all()
        }

        now = datetime.now(KST)
        for game in games:
            game_id = game.game_id
            year = int(game_id[:4])
            has_pbp = game_id in pbp_ids
            has_events = game_id in event_ids
            has_event_state = game_id in event_state_ids

            if has_pbp and has_event_state:
                status = VALIDATION_VERIFIED
                reason = "seeded_from_existing_relay_state"
            elif has_pbp or has_events:
                status = VALIDATION_SOURCE_INCOMPLETE
                reason = "existing_relay_without_full_state"
            elif mark_legacy_unavailable and year <= 2009:
                status = VALIDATION_SOURCE_UNAVAILABLE
                reason = "legacy_public_relay_source_unavailable"
            else:
                status = VALIDATION_UNVERIFIED
                reason = "missing_relay_rows"

            metrics = (
                session.query(GameValidationMetrics).filter(GameValidationMetrics.game_id == game_id).one_or_none()
            )
            if metrics is None:
                metrics = GameValidationMetrics(game_id=game_id)
                session.add(metrics)
            elif metrics.validation_status != status:
                metrics.previous_status = metrics.validation_status
            metrics.validation_status = status
            metrics.source_used = metrics.source_used or "seed"
            metrics.last_successful_event_at = (
                now if status == VALIDATION_VERIFIED else metrics.last_successful_event_at
            )
            metrics.evidence_json = {
                "reason": reason,
                "has_pbp": has_pbp,
                "has_events": has_events,
                "has_event_state": has_event_state,
                "event_rows": row_counts.get(game_id, 0),
            }
            counts[status] = counts.get(status, 0) + 1

        session.commit()
    return counts


def main(argv: Sequence[str] | None = None) -> int:
    """Main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="Seed relay validation metrics for completed games")
    parser.add_argument("--season", type=int, help="Optional season year")
    parser.add_argument(
        "--no-mark-legacy-unavailable",
        action="store_true",
        help="Leave legacy no-source games as unverified instead of source_unavailable",
    )
    args = parser.parse_args(argv)
    counts = seed_relay_validation_metrics(
        season=args.season,
        mark_legacy_unavailable=not args.no_mark_legacy_unavailable,
    )
    logger.info("[INFO] Seeded relay validation metrics: %s", counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
