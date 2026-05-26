"""Generate post-game LLM-ready story timelines from game_events."""
from __future__ import annotations

import logging
import argparse
import asyncio
import json
import os
from datetime import datetime
from typing import List, Sequence

from src.db.engine import SessionLocal
from src.models.game import Game, GameEvent, GameSummary
from src.repositories.game_repository import refresh_game_status_for_date
from src.services.game_story_builder import GameStoryBuilder, STORY_SUMMARY_TYPE
from src.sync.oci_sync import OCISync
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES
from src.utils.refresh_manifest import write_refresh_manifest
from src.utils.safe_print import safe_print as print

logger = logging.getLogger(__name__)


def dump_story_json(story_data: dict) -> str:
    return json.dumps(story_data, ensure_ascii=False)


def _upsert_story_summary(session, game_id: str, story_json: str) -> None:
    existing_summaries = session.query(GameSummary).filter(
        GameSummary.game_id == game_id,
        GameSummary.summary_type == STORY_SUMMARY_TYPE,
    ).all()
    if existing_summaries:
        for summary in existing_summaries:
            summary.detail_text = story_json
        return

    session.add(
        GameSummary(
            game_id=game_id,
            summary_type=STORY_SUMMARY_TYPE,
            detail_text=story_json,
        )
    )


def _build_story_data(builder: GameStoryBuilder, session, game: Game) -> dict:
    events = (
        session.query(GameEvent)
        .filter(GameEvent.game_id == game.game_id)
        .order_by(GameEvent.event_seq.asc(), GameEvent.id.asc())
        .all()
    )
    return builder.build(game, events)


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


async def run_story_batch(target_date: str, *, sync_to_oci: bool | None = None) -> List[str]:
    print(f"🚀 Starting Post-game Story Data Batch for {target_date}...")

    target_dt_obj = datetime.strptime(target_date, "%Y%m%d").date()
    status_result = refresh_game_status_for_date(target_date)
    if status_result.get("updated", 0):
        print(
            "🔄 Refreshed game statuses before story generation: "
            f"updated={status_result.get('updated', 0)} "
            f"counts={status_result.get('status_counts', {})}"
        )

    saved_ids: List[str] = []
    with SessionLocal() as session:
        builder = GameStoryBuilder()
        games = session.query(Game).filter(
            Game.game_date == target_dt_obj,
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
        ).order_by(Game.game_id.asc()).all()

        if not games:
            manifest_path = write_refresh_manifest(
                phase="postgame_story",
                target_date=target_date,
                game_ids=[],
                datasets=["game", "game_events", "game_summary"],
            )
            print(f"ℹ️ No completed games found for {target_date}. manifest={manifest_path}")
            return []

        for game in games:
            print(f"📚 Generating story timeline for {game.game_id}...")
            story_data = _build_story_data(builder, session, game)
            if not story_data["timeline"]:
                print(
                    f"  ⚠️ No story timeline events selected for {game.game_id}. "
                    f"warnings={story_data['source'].get('warnings', [])}"
                )
            _upsert_story_summary(session, game.game_id, dump_story_json(story_data))
            saved_ids.append(game.game_id)

        try:
            session.commit()
        except Exception:
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
    print(f"✅ Story batch finished. saved={len(saved_ids)} manifest={manifest_path}")
    return saved_ids


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KBO Daily Game Story Generator")
    parser.add_argument("--date", type=str, help="Target date (YYYYMMDD). Defaults to today.", default=None)
    parser.add_argument("--no-sync", action="store_true", help="Skip explicit OCI sync after local writes")
    args = parser.parse_args(argv)

    target = args.date if args.date else datetime.now().strftime("%Y%m%d")
    asyncio.run(run_story_batch(target, sync_to_oci=not args.no_sync))
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
