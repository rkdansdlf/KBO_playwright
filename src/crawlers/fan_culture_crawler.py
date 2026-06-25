"""YouTube Data API-based KBO cheer song crawler.

Replaces the Namu Wiki crawler (which was blocked) by fetching
cheer song metadata from official KBO team YouTube channels.

Data collected:
  - Song title (from video title)
  - Player name (parsed from title)
  - Song type (TEAM / PLAYER / STARTER / CLOSER etc.)
  - YouTube URL as source_url
  - Published date

Usage:
    python -m src.crawlers.fan_culture_crawler --save
    python -m src.crawlers.fan_culture_crawler --team LG --dry-run
    python -m src.crawlers.fan_culture_crawler --save --season 2026
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.db.engine import SessionLocal
from src.repositories.fan_culture_repository import FanCultureRepository
from src.utils.youtube_api_client import (
    TEAM_YOUTUBE_CHANNELS,
    YouTubeAPIClient,
    YouTubeVideoItem,
    _classify_song_type,
    _extract_player_name,
)

logger = logging.getLogger(__name__)

YOUTUBE_VIDEO_BASE = "https://www.youtube.com/watch?v="
FAN_CULTURE_DB_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)

# 응원가로 판단하기 위한 제목 필터 (이 단어가 포함되어야 함)
CHEERSONG_TITLE_KEYWORDS = re.compile(
    r"응원가|응원\s*송|cheersong|cheer\s*song|응원\s*모음",
    re.IGNORECASE,
)

# 제외 패턴 (예: 쇼츠, 하이라이트, 뉴스 등 응원가 아닌 영상)
EXCLUDE_PATTERNS = re.compile(
    r"하이라이트|highlight|뉴스|news|경기\s*영상|직캠|fancam|영입|인터뷰|interview|예고|preview",
    re.IGNORECASE,
)

# 시즌 연도 추출
SEASON_PATTERN = re.compile(r"(20\d{2})\s*(?:시즌|season)?")


def _extract_season(title: str, fallback: int | None = None) -> int | None:
    m = SEASON_PATTERN.search(title)
    return int(m.group(1)) if m else fallback


def _video_to_song(item: YouTubeVideoItem, team_id: str, current_season: int) -> dict[str, Any] | None:
    """Convert a YouTube video item to a cheer_song dict."""
    title = item.title.strip()

    # 필터링: 응원가 키워드가 없으면 제외
    if not CHEERSONG_TITLE_KEYWORDS.search(title):
        return None

    # 제외 패턴 매칭
    if EXCLUDE_PATTERNS.search(title):
        return None

    song_type = _classify_song_type(title)
    player_name = _extract_player_name(title)
    season = _extract_season(title, fallback=current_season)

    return {
        "team_id": team_id,
        "song_name": title[:200],
        "song_type": song_type,
        "lyrics": None,  # YouTube API does not provide lyrics
        "description": (
            f"{'선수: ' + player_name + ' | ' if player_name else ''}시즌: {season} | 출처: YouTube {item.channel_id}"
        ),
        "video_url": f"{YOUTUBE_VIDEO_BASE}{item.video_id}",
        "introduction_year": season,
    }


class FanCultureCrawler:
    """Crawls KBO cheer song metadata from official team YouTube channels.

    Replaces the previous Namu Wiki-based implementation.
    Requires YOUTUBE_API_KEY environment variable.
    """

    def __init__(self, season: int | None = None, max_results_per_team: int = 50) -> None:
        self.season = season or datetime.now(KST).year
        self.max_results = max_results_per_team
        self.client = YouTubeAPIClient()

    async def run(
        self,
        *,
        save: bool = False,
        team_filter: str | None = None,
        dry_run: bool = False,
    ) -> list[dict]:
        """Crawl cheer songs from YouTube for all (or one) KBO team.

        Args:
            save: Persist results to database.
            team_filter: Only crawl this team code (e.g. 'LG').
            dry_run: Print results without saving.

        """
        if not self.client.is_configured():
            logger.warning("[FanCulture] ⚠️  YOUTUBE_API_KEY not set.")
            logger.info("[FanCulture]    Set it in .env to enable YouTube-based cheer song crawling.")
            logger.info("[FanCulture]    Get a free key at: https://console.cloud.google.com/")
            return []

        teams = (
            {team_filter.upper(): TEAM_YOUTUBE_CHANNELS[team_filter.upper()]}
            if team_filter and team_filter.upper() in TEAM_YOUTUBE_CHANNELS
            else TEAM_YOUTUBE_CHANNELS
        )

        all_songs: list[dict] = []

        for team_id, ch_info in teams.items():
            channel_id = ch_info["channel_id"]
            logger.info("[FanCulture] %s (%s) — searching YouTube...", team_id, ch_info["name"])

            team_songs = await self._crawl_team(team_id, channel_id, ch_info["search_queries"])
            all_songs.extend(team_songs)
            logger.info("[FanCulture]   → %s cheer songs found", len(team_songs))

            # API 호출 간격 (rate limit 준수)
            await asyncio.sleep(2.0)

        logger.info("[FanCulture] Total: %s songs across %s teams", len(all_songs), len(teams))

        if dry_run or not save:
            for s in all_songs[:5]:
                logger.info(
                    "  [%s] %s | type=%s | player=%s",
                    s["team_id"],
                    s["song_name"],
                    s["song_type"],
                    s.get("player_name"),
                )
            if len(all_songs) > 5:
                logger.info("  ... and %s more", len(all_songs) - 5)
        elif save:
            self._save_to_db(all_songs)

        return all_songs

    async def _crawl_team(
        self,
        team_id: str,
        channel_id: str,
        search_queries: list[str],
    ) -> list[dict]:
        """Crawl cheer songs for a single team via YouTube search."""
        seen_video_ids: set[str] = set()
        songs: list[dict] = []

        for query in search_queries:
            items = await self.client.search_videos(
                channel_id,
                query,
                max_results=self.max_results,
            )
            for item in items:
                if item.video_id in seen_video_ids:
                    continue
                seen_video_ids.add(item.video_id)

                song = _video_to_song(item, team_id, self.season)
                if song:
                    songs.append(song)

            await asyncio.sleep(1.5)

        return songs

    def _save_to_db(self, songs: list[dict]) -> None:
        with SessionLocal() as session:
            try:
                repo = FanCultureRepository(session)
                saved = 0
                for item in songs:
                    try:
                        repo.save_cheer_song(item)
                        saved += 1
                    except FAN_CULTURE_DB_EXCEPTIONS:
                        logger.exception("Failed to save song: %s", item.get("song_name", ""))
                session.commit()
                logger.info("[FanCulture] Saved %s cheer songs to DB.", saved)
            except FAN_CULTURE_DB_EXCEPTIONS:
                session.rollback()
                logger.exception("[FanCulture] DB save failed")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="YouTube-based KBO cheer song crawler")
    parser.add_argument("--save", action="store_true", help="Save to database")
    parser.add_argument("--dry-run", action="store_true", help="Print without saving")
    parser.add_argument("--team", type=str, default=None, help="Team code (e.g. LG)")
    parser.add_argument("--season", type=int, default=None, help="Season year (default: current year)")
    args = parser.parse_args()

    asyncio.run(
        FanCultureCrawler(season=args.season).run(
            save=args.save,
            team_filter=args.team,
            dry_run=args.dry_run,
        ),
    )
