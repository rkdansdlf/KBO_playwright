"""
Daily Highlight Batch Script
Generate game highlights from PBP/WPA data, persists them, syncs to OCI, and sends Telegram reports.

"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from src.aggregators.highlight_aggregator import HighlightAggregator
from src.constants import KST
from src.db.engine import SessionLocal
from src.models.game import Game, GameHighlight
from src.sync.oci_sync import OCISync
from src.utils.alerting import SlackWebhookClient
from src.utils.date_helpers import parse_date_str
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

HIGHLIGHT_SYNC_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)


def _load_completed_games(session: Session, target_date: date) -> list[Game]:
    return (
        session.query(Game)
        .filter(
            Game.game_date == target_date,
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
        )
        .all()
    )


def _process_highlight_games(
    session: Session,
    games: list[Game],
    *,
    force: bool,
    dry_run: bool,
) -> tuple[list[str], dict[str, list[GameHighlight]], dict[str, Game]]:
    processed_game_ids: list[str] = []
    game_highlights_map: dict[str, list[GameHighlight]] = {}
    game_map: dict[str, Game] = {}
    aggregator = HighlightAggregator(session)

    for game in games:
        game_id = game.game_id
        game_map[game_id] = game
        if not force and not dry_run:
            existing = session.query(GameHighlight).filter(GameHighlight.game_id == game_id).all()
            if existing:
                logger.info("   ⏩ Skipping %s: Highlights already exist (use --force to overwrite)", game_id)
                game_highlights_map[game_id] = existing
                processed_game_ids.append(game_id)
                continue

        logger.info("📊 Aggregating highlights for %s (%s vs %s)...", game_id, game.away_team, game.home_team)
        highlights = aggregator.aggregate_game_highlights(game_id)
        if not highlights:
            logger.warning("   ⚠️ No significant highlight plays found for %s.", game_id)
            continue

        logger.info("   ✨ Generated %s highlights.", len(highlights))
        game_highlights_map[game_id] = highlights
        if not dry_run:
            logger.info("   💾 Saved %s highlights to local DB.", aggregator.save_highlights(game_id, highlights))
        else:
            logger.info("   🧪 [DRY-RUN] Highlights not saved.")
        processed_game_ids.append(game_id)

    return processed_game_ids, game_highlights_map, game_map


def _sync_highlights_to_oci(processed_game_ids: list[str], *, dry_run: bool, sync_to_oci: bool | None) -> None:
    should_sync = sync_to_oci if sync_to_oci is not None else bool(os.getenv("OCI_DB_URL"))
    if not should_sync or not processed_game_ids or dry_run:
        return
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        return
    logger.info("🔄 Syncing highlights for %s games to OCI PostgreSQL...", len(processed_game_ids))
    with SessionLocal() as sync_session:
        syncer = OCISync(oci_url, sync_session)
        try:
            for game_id in sorted(set(processed_game_ids)):
                syncer.sync_specific_game(game_id)
        except HIGHLIGHT_SYNC_EXCEPTIONS:
            logger.exception("OCI Sync failed")
        finally:
            syncer.close()


def _highlight_notification_message(
    target_date_str: str,
    processed_game_ids: list[str],
    game_highlights_map: dict[str, list[GameHighlight]],
    game_map: dict[str, Game],
) -> str:
    all_highlights = [highlight for highlights in game_highlights_map.values() for highlight in highlights]
    lead_change_games, walkoff_games = _highlight_special_matchups(processed_game_ids, game_highlights_map, game_map)
    top_3_plays = sorted(
        [h for h in all_highlights if h.wpa is not None],
        key=lambda h: abs(h.wpa or 0.0),
        reverse=True,
    )[:3]

    target_date_formatted = f"{target_date_str[:4]}-{target_date_str[4:6]}-{target_date_str[6:]}"
    message = f"🎬 <b>KBO 일일 하이라이트 요약 ({target_date_formatted})</b>\n\n"
    message += "📊 <b>경기 요약</b>\n"
    message += f"- 대상 경기: {len(processed_game_ids)}경기\n"
    lc_text = f"{len(lead_change_games)}경기 ({', '.join(lead_change_games)})" if lead_change_games else "0경기"
    wo_text = f"{len(walkoff_games)}경기 ({', '.join(walkoff_games)})" if walkoff_games else "0경기"
    message += f"- 역전 경기: {lc_text}\n"
    message += f"- 끝내기 경기: {wo_text}\n\n"
    message += "🔥 <b>오늘의 주요 순간 TOP 3 (WPA 기준)</b>\n"
    return message + _format_top_highlight_plays(top_3_plays, game_map)


def _highlight_special_matchups(
    processed_game_ids: list[str],
    game_highlights_map: dict[str, list[GameHighlight]],
    game_map: dict[str, Game],
) -> tuple[list[str], list[str]]:
    lead_change_games = []
    walkoff_games = []
    for game_id in processed_game_ids:
        highlights = game_highlights_map.get(game_id, [])
        game = game_map[game_id]
        matchup = f"{game.away_team} vs {game.home_team}"
        if any(highlight.highlight_type == "LEAD_CHANGE" or "역전" in highlight.tags for highlight in highlights):
            lead_change_games.append(matchup)
        if any(highlight.highlight_type == "WALK_OFF" or "끝내기" in highlight.tags for highlight in highlights):
            walkoff_games.append(matchup)
    return lead_change_games, walkoff_games


def _format_top_highlight_plays(top_3_plays: list[GameHighlight], game_map: dict[str, Game]) -> str:
    if not top_3_plays:
        return "- 없음 (이벤트 WPA 데이터 불충분)\n"
    message = ""
    for rank, highlight in enumerate(top_3_plays, start=1):
        game = game_map[highlight.game_id]
        half_str = "초" if highlight.inning_half == "top" else "말"
        message += f"{rank}. <b>{game.away_team} {game.away_score} : {game.home_score} {game.home_team}</b> ({highlight.inning}회{half_str})\n"
        message += f"   👉 {highlight.description} (WPA {highlight.wpa:+.3f})\n"
    return message


def _send_highlight_notification(message: str, *, dry_run: bool) -> None:
    if dry_run:
        logger.info("\n🧪 [DRY-RUN] Telegram message content:")
        logger.info(message)
        return
    logger.info("📣 Sending Telegram notification summary...")
    if SlackWebhookClient.send_alert(message):
        logger.info("   ✅ Telegram alert sent successfully.")
    else:
        logger.warning("   ⚠️ Failed to send Telegram alert.")


async def run_highlight_batch(
    target_date_str: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    sync_to_oci: bool | None = None,
    notify: bool = True,
) -> list[str]:
    """
    Run highlight batch.

    Args:
        target_date_str: Target Date Str.
        force: If True, forces the operation even if data already exists.
        dry_run: If True, performs a dry run without persisting changes.
        sync_to_oci: Sync To Oci.
        notify: Notify.
        target_date_str: Target Date Str.

    Returns:
        List of results.

    """
    logger.info("🎬 Starting Daily Highlight Batch for %s...", target_date_str)

    try:
        target_date = parse_date_str(target_date_str)
    except ValueError:
        logger.exception("❌ Invalid date format: %s. Expected YYYYMMDD.", target_date_str)
        return []

    with SessionLocal() as session:
        games = _load_completed_games(session, target_date)
        if not games:
            logger.info("ℹ️ No completed games found for %s.", target_date_str)
            return []

        processed_game_ids, game_highlights_map, game_map = _process_highlight_games(
            session,
            games,
            force=force,
            dry_run=dry_run,
        )

    _sync_highlights_to_oci(processed_game_ids, dry_run=dry_run, sync_to_oci=sync_to_oci)
    if notify and processed_game_ids:
        _send_highlight_notification(
            _highlight_notification_message(target_date_str, processed_game_ids, game_highlights_map, game_map),
            dry_run=dry_run,
        )

    logger.info("✅ Highlight batch finished. Processed=%s games.", len(processed_game_ids))
    return processed_game_ids


def main(argv: Sequence[str] | None = None) -> int:
    """
    Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = argparse.ArgumentParser(description="KBO Daily Highlights Generator")

    parser.add_argument("--date", type=str, help="Target date (YYYYMMDD). Defaults to today.", default=None)
    parser.add_argument("--force", action="store_true", help="Re-generate and overwrite highlights if they exist")
    parser.add_argument("--dry-run", action="store_true", help="Run without persisting database changes or alerting")
    parser.add_argument("--no-sync", action="store_true", help="Skip explicit OCI sync after local writes")
    parser.add_argument("--no-notify", action="store_true", help="Skip sending Telegram alert summary")
    args = parser.parse_args(argv)

    target = args.date or datetime.now(KST).strftime("%Y%m%d")
    asyncio.run(
        run_highlight_batch(
            target,
            force=args.force,
            dry_run=args.dry_run,
            sync_to_oci=False if args.no_sync else None,
            notify=not args.no_notify,
        ),
    )
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
