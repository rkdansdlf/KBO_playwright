"""
Daily Highlight Batch Script
Generates game highlights from PBP/WPA data, persists them, syncs to OCI, and sends Telegram reports.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from collections.abc import Sequence
from datetime import datetime

from src.aggregators.highlight_aggregator import HighlightAggregator
from src.db.engine import SessionLocal
from src.models.game import Game, GameHighlight
from src.sync.oci_sync import OCISync
from src.utils.alerting import SlackWebhookClient
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

logger = logging.getLogger(__name__)


async def run_highlight_batch(
    target_date_str: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    sync_to_oci: bool | None = None,
    notify: bool = True,
) -> list[str]:
    logger.info("🎬 Starting Daily Highlight Batch for %s...", target_date_str)

    try:
        target_date = datetime.strptime(target_date_str, "%Y%m%d").date()
    except ValueError:
        logger.exception("❌ Invalid date format: %s. Expected YYYYMMDD.", target_date_str)
        return []

    processed_game_ids: list[str] = []
    game_highlights_map: dict[str, list[GameHighlight]] = {}
    game_map: dict[str, Game] = {}

    with SessionLocal() as session:
        # Fetch completed/draw games for the target date
        games = (
            session.query(Game)
            .filter(
                Game.game_date == target_date,
                Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
            )
            .all()
        )

        if not games:
            logger.info("ℹ️ No completed games found for %s.", target_date_str)
            return []

        aggregator = HighlightAggregator(session)

        for game in games:
            game_id = game.game_id
            game_map[game_id] = game

            # If force is False, skip if highlights already exist in local DB
            if not force and not dry_run:
                exists = session.query(GameHighlight).filter(GameHighlight.game_id == game_id).first() is not None
                if exists:
                    logger.info("   ⏩ Skipping %s: Highlights already exist (use --force to overwrite)", game_id)
                    # Load existing highlights for notification purposes
                    existing = session.query(GameHighlight).filter(GameHighlight.game_id == game_id).all()
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
                saved_count = aggregator.save_highlights(game_id, highlights)
                logger.info("   💾 Saved %s highlights to local DB.", saved_count)
            else:
                logger.info("   🧪 [DRY-RUN] Highlights not saved.")

            processed_game_ids.append(game_id)

    # Sync to OCI if requested
    should_sync = sync_to_oci if sync_to_oci is not None else bool(os.getenv("OCI_DB_URL"))
    if should_sync and processed_game_ids and not dry_run:
        oci_url = os.getenv("OCI_DB_URL")
        if oci_url:
            logger.info("🔄 Syncing highlights for %s games to OCI PostgreSQL...", len(processed_game_ids))
            with SessionLocal() as sync_session:
                syncer = OCISync(oci_url, sync_session)
                try:
                    for game_id in sorted(set(processed_game_ids)):
                        syncer.sync_specific_game(game_id)
                except Exception as e:
                    logger.exception("OCI Sync failed: %s", e)
                finally:
                    syncer.close()

    # Send Telegram/Slack notification summary
    if notify and processed_game_ids:
        # Collate all highlights across games
        all_highlights: list[GameHighlight] = []
        for _g_id, h_list in game_highlights_map.items():
            all_highlights.extend(h_list)

        # Count special games
        lead_change_games = []
        walkoff_games = []

        for g_id in processed_game_ids:
            h_list = game_highlights_map.get(g_id, [])
            game = game_map[g_id]
            matchup = f"{game.away_team} vs {game.home_team}"

            is_lc = any(h.highlight_type == "LEAD_CHANGE" or "역전" in h.tags for h in h_list)
            is_wo = any(h.highlight_type == "WALK_OFF" or "끝내기" in h.tags for h in h_list)

            if is_lc:
                lead_change_games.append(matchup)
            if is_wo:
                walkoff_games.append(matchup)

        # Find top 3 plays by absolute WPA (WPA must not be None)
        valid_wpa_highlights = [h for h in all_highlights if h.wpa is not None]
        top_3_plays = sorted(valid_wpa_highlights, key=lambda h: abs(h.wpa or 0.0), reverse=True)[:3]

        # Format Telegram alert message
        target_date_formatted = f"{target_date_str[:4]}-{target_date_str[4:6]}-{target_date_str[6:]}"
        message = f"🎬 <b>KBO 일일 하이라이트 요약 ({target_date_formatted})</b>\n\n"

        message += "📊 <b>경기 요약</b>\n"
        message += f"- 대상 경기: {len(processed_game_ids)}경기\n"

        lc_text = f"{len(lead_change_games)}경기 ({', '.join(lead_change_games)})" if lead_change_games else "0경기"
        wo_text = f"{len(walkoff_games)}경기 ({', '.join(walkoff_games)})" if walkoff_games else "0경기"
        message += f"- 역전 경기: {lc_text}\n"
        message += f"- 끝내기 경기: {wo_text}\n\n"

        message += "🔥 <b>오늘의 주요 순간 TOP 3 (WPA 기준)</b>\n"
        if top_3_plays:
            for rank, h in enumerate(top_3_plays, start=1):
                game = game_map[h.game_id]
                half_str = "초" if h.inning_half == "top" else "말"
                message += f"{rank}. <b>{game.away_team} {game.away_score} : {game.home_score} {game.home_team}</b> ({h.inning}회{half_str})\n"
                message += f"   👉 {h.description} (WPA {h.wpa:+.3f})\n"
        else:
            message += "- 없음 (이벤트 WPA 데이터 불충분)\n"

        if dry_run:
            logger.info("\n🧪 [DRY-RUN] Telegram message content:")
            logger.info(message)
        else:
            logger.info("📣 Sending Telegram notification summary...")
            sent = SlackWebhookClient.send_alert(message)
            if sent:
                logger.info("   ✅ Telegram alert sent successfully.")
            else:
                logger.warning("   ⚠️ Failed to send Telegram alert.")

    logger.info("✅ Highlight batch finished. Processed=%s games.", len(processed_game_ids))
    return processed_game_ids


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KBO Daily Highlights Generator")
    parser.add_argument("--date", type=str, help="Target date (YYYYMMDD). Defaults to today.", default=None)
    parser.add_argument("--force", action="store_true", help="Re-generate and overwrite highlights if they exist")
    parser.add_argument("--dry-run", action="store_true", help="Run without persisting database changes or alerting")
    parser.add_argument("--no-sync", action="store_true", help="Skip explicit OCI sync after local writes")
    parser.add_argument("--no-notify", action="store_true", help="Skip sending Telegram alert summary")
    args = parser.parse_args(argv)

    target = args.date if args.date else datetime.now().strftime("%Y%m%d")
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
