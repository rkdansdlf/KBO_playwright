"""Generate post-game LLM-ready story timelines from game_events."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.db.engine import SessionLocal
from src.models.game import Game, GameEvent, GameSummary, GameValidationMetrics
from src.repositories.game_repository import refresh_game_status_for_date
from src.services.game_story_builder import STORY_SUMMARY_TYPE, GameStoryBuilder
from src.sync.oci_sync import OCISync
from src.utils.date_helpers import parse_date_str
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES
from src.utils.refresh_manifest import write_refresh_manifest

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)
STORY_DB_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)
TRUSTED_RELAY_STATUSES = {"verified", "recovered"}


def dump_story_json(story_data: dict) -> str:
    """Dumps story json.

    Args:
        story_data: Story Data.

    Returns:
        String result.

    """
    return json.dumps(story_data, ensure_ascii=False)


def _upsert_story_summary(session: Session, game_id: str, story_json: str) -> None:
    existing_summaries = (
        session.query(GameSummary)
        .filter(
            GameSummary.game_id == game_id,
            GameSummary.summary_type == STORY_SUMMARY_TYPE,
        )
        .all()
    )
    if existing_summaries:
        for summary in existing_summaries:
            summary.detail_text = story_json
        return

    session.add(
        GameSummary(
            game_id=game_id,
            summary_type=STORY_SUMMARY_TYPE,
            detail_text=story_json,
        ),
    )


def _build_story_data(builder: GameStoryBuilder, session: Session, game: Game) -> dict[str, Any]:
    events = (
        session.query(GameEvent)
        .filter(GameEvent.game_id == game.game_id)
        .order_by(GameEvent.event_seq.asc(), GameEvent.id.asc())
        .all()
    )
    return builder.build(game, events)


def _trusted_relay_game_ids(session: Session, game_ids: Sequence[str]) -> set[str]:
    if not game_ids:
        return set()
    target_ids = {str(game_id) for game_id in game_ids}
    metric_rows = (
        session.query(GameValidationMetrics.game_id, GameValidationMetrics.validation_status)
        .filter(GameValidationMetrics.game_id.in_(list(target_ids)))
        .all()
    )
    statuses = {str(row.game_id): row.validation_status for row in metric_rows}
    trusted_ids = {game_id for game_id, status in statuses.items() if status in TRUSTED_RELAY_STATUSES}

    missing_metric_ids = target_ids - set(statuses)
    if missing_metric_ids:
        wpa_rows = (
            session.query(GameEvent.game_id)
            .filter(GameEvent.game_id.in_(list(missing_metric_ids)), GameEvent.wpa.isnot(None))
            .distinct()
            .all()
        )
        trusted_ids.update(str(row[0]) for row in wpa_rows)

    return trusted_ids


def _sync_story_summaries(game_ids: Sequence[str]) -> None:
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url or not game_ids:
        return
    with SessionLocal() as sync_session:
        syncer = OCISync(oci_url, sync_session)
        try:
            syncer.sync_review_summaries_for_games(
                sorted(set(game_ids)),
                summary_type=STORY_SUMMARY_TYPE,
            )
        finally:
            syncer.close()


async def run_story_batch(target_date: str, *, sync_to_oci: bool | None = None) -> list[str]:
    """Runs story batch.

    Args:
        target_date: Target Date.

    Returns:
        List of results.

    """
    logger.info("🚀 Starting Post-game Story Data Batch for %s...", target_date)

    target_dt_obj = parse_date_str(target_date)
    status_result = refresh_game_status_for_date(target_date)
    if status_result.get("updated", 0):
        logger.info(
            "🔄 Refreshed game statuses before story generation: updated=%s counts=%s",
            status_result.get("updated", 0),
            status_result.get("status_counts", {}),
        )

    saved_ids: list[str] = []
    with SessionLocal() as session:
        builder = GameStoryBuilder()
        games = (
            session.query(Game)
            .filter(
                Game.game_date == target_dt_obj,
                Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
            )
            .order_by(Game.game_id.asc())
            .all()
        )

        if not games:
            manifest_path = write_refresh_manifest(
                phase="postgame_story",
                target_date=target_date,
                game_ids=[],
                datasets=["game", "game_events", "game_summary"],
            )
            logger.info("ℹ️ No completed games found for %s. manifest=%s", target_date, manifest_path)
            return []

        trusted_game_ids = _trusted_relay_game_ids(session, [game.game_id for game in games])

        for game in games:
            if game.game_id not in trusted_game_ids:
                logger.warning("  ⚠️ Skipping story for %s: relay validation is not trusted", game.game_id)
                continue
            logger.info("📚 Generating story timeline for %s...", game.game_id)
            story_data = _build_story_data(builder, session, game)
            if not story_data["timeline"]:
                logger.info(
                    "  ⚠️ No story timeline events selected for %s. warnings=%s",
                    game.game_id,
                    story_data["source"].get("warnings", []),
                )
            _upsert_story_summary(session, game.game_id, dump_story_json(story_data))
            saved_ids.append(game.game_id)

        try:
            session.commit()
        except STORY_DB_EXCEPTIONS:
            session.rollback()
            logger.exception("❌ Failed to save game stories to DB")
            raise

    should_sync = sync_to_oci if sync_to_oci is not None else bool(os.getenv("OCI_DB_URL"))
    if should_sync:
        _sync_story_summaries(saved_ids)

    manifest_path = write_refresh_manifest(
        phase="postgame_story",
        target_date=target_date,
        game_ids=saved_ids,
        datasets=["game", "game_events", "game_summary"],
    )
    logger.info("✅ Story batch finished. saved=%s manifest=%s", len(saved_ids), manifest_path)
    return saved_ids


def main(argv: Sequence[str] | None = None) -> int:
    """Main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="KBO Daily Game Story Generator")
    parser.add_argument("--date", type=str, help="Target date (YYYYMMDD). Defaults to today.", default=None)
    parser.add_argument("--no-sync", action="store_true", help="Skip explicit OCI sync after local writes")
    args = parser.parse_args(argv)

    target = args.date or datetime.now(KST).strftime("%Y%m%d")
    asyncio.run(run_story_batch(target, sync_to_oci=not args.no_sync))
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
