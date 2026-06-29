"""
YouTube Data API v3 client for KBO cheer song collection.

Fetches cheer song playlists from official KBO team YouTube channels.
This replaces the Namu Wiki crawler which is blocked.

Free quota: 10,000 units/day (sufficient for all 10 teams daily).

Environment variable:
    YOUTUBE_API_KEY — Google Cloud API key with YouTube Data API v3 enabled

Reference:
    https://developers.google.com/youtube/v3/docs/playlistItems/list
    https://developers.google.com/youtube/v3/docs/search/list
"""

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any

import httpx

logger = logging.getLogger(__name__)

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"

# 10개 KBO 구단 공식 YouTube 채널 ID
# Handle ID (@xxx 형식)는 channels API로 실제 채널 ID 조회 필요.
# 여기서는 채널 핸들과 채널 ID를 모두 관리.
TEAM_YOUTUBE_CHANNELS: dict[str, dict[str, Any]] = {
    "LG": {
        "handle": "@LGTwinsTV",
        "channel_id": "UCL6QZZxb-HR4hCh_eFAnQWA",
        "name": "LGTWINSTV",
        "search_queries": ["응원가", "cheersong"],
    },
    "OB": {
        "handle": "@bearstv1982",
        "channel_id": "UCsebzRfMhwYfjeBIxNX1brg",
        "name": "BEARS TV",
        "search_queries": ["응원가", "cheersong"],
    },
    "KIA": {
        "handle": "@kiatigerstv",
        "channel_id": "UCKp8knO8a6tSI1oaLjfd9XA",
        "name": "기아타이거즈 KIA TIGERS",
        "search_queries": ["응원가", "cheersong", "타이거즈 응원"],
    },
    "SS": {
        "handle": "@SamsungLionsTV",
        "channel_id": "UCMWAku3a3h65QpLm63Jf2pw",
        "name": "SamsungLionsTV",
        "search_queries": ["응원가", "cheersong", "라이온즈 응원"],
    },
    "LT": {
        "handle": "@LotteGiantsTV",
        "channel_id": "UCAZQZdSY5_YrziMPqXi-Zfw",
        "name": "롯데자이언츠 TV",
        "search_queries": ["응원가", "cheersong", "자이언츠 응원"],
    },
    "KT": {
        "handle": "@ktwizofficial",
        "channel_id": "UCvScyjGkBUx2CJDMNAi9Twg",
        "name": "kt wiz",
        "search_queries": ["응원가", "cheersong", "위즈 응원"],
    },
    "SSG": {
        "handle": "@ssglanders",
        "channel_id": "UCt8iRtgjVqm5rJHNl1TUojg",
        "name": "SSG 랜더스",
        "search_queries": ["응원가", "cheersong", "랜더스 응원"],
    },
    "NC": {
        "handle": "@NCdinosofficial",
        "channel_id": "UC8_FRgynMX8wlGsU6Jh3zKg",
        "name": "NC 다이노스",
        "search_queries": ["응원가", "cheersong", "다이노스 응원"],
    },
    "HH": {
        "handle": "@HanwhaEagles",
        "channel_id": "UCdq4Ji3772xudYRUatdzRrg",
        "name": "한화이글스",
        "search_queries": ["응원가", "cheersong", "이글스 응원"],
    },
    "KH": {
        "handle": "@KiwoomHeroesTV",
        "channel_id": "UC_MA8-XEaVmvyayPzG66IKg",
        "name": "키움히어로즈",
        "search_queries": ["응원가", "cheersong", "히어로즈 응원"],
    },
}

# 응원가 유형 분류 키워드
SONG_TYPE_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"팀\s*응원가|team\s*song|응원가\s*모음|전체\s*응원", re.IGNORECASE), "TEAM"),
    (re.compile(r"타순|lead.off|1번타자", re.IGNORECASE), "LEADOFF"),
    (re.compile(r"끝내기|walk.off|끝내|승리의", re.IGNORECASE), "WALKOFF"),
    (re.compile(r"마무리|클로저|closer", re.IGNORECASE), "CLOSER"),
    (re.compile(r"선발|starter|starting", re.IGNORECASE), "STARTER"),
]

# 선수 응원가 제목 패턴: "홍길동 응원가", "[홍길동] 응원가"
PLAYER_NAME_PATTERN = re.compile(r"(?:\[)?([가-힣]{2,4})(?:\])?\s*응원가")


@dataclass
class YouTubeVideoItem:
    """YouTubeVideoItem class."""

    video_id: str
    title: str
    description: str
    published_at: str
    thumbnail_url: str
    playlist_id: str | None = None
    channel_id: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


def _classify_song_type(title: str) -> str:
    """
    Classifies song type.

    Args:
        title: Title.

    Returns:
        String result.

    """
    for pattern, song_type in SONG_TYPE_RULES:
        if pattern.search(title):
            return song_type
    return "PLAYER"


def _extract_player_name(title: str) -> str | None:
    """
    Extracts player name.

    Args:
        title: Title.

    Returns:
        The result of the operation.

    """
    m = PLAYER_NAME_PATTERN.search(title)
    return m.group(1) if m else None


class YouTubeAPIClient:
    """
    Async client for YouTube Data API v3.

    Usage:
        client = YouTubeAPIClient()
        items = await client.search_videos("UCiXGdRARMxrZ5kJLe7t4Xpg", "응원가")
    """

    def __init__(self) -> None:
        """Initializes a new instance."""
        self.api_key = os.getenv("YOUTUBE_API_KEY", "")

    def is_configured(self) -> bool:
        """
        Returns whether the configured.

        Returns:
            True if the condition is met, False otherwise.

        """
        return bool(self.api_key)

    async def _get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        """
        Gets  get.

        Args:
            endpoint: Endpoint.
            params: Parameters dictionary.

        Returns:
            Dictionary mapping.

        """
        params["key"] = self.api_key
        url = f"{YOUTUBE_API_BASE}/{endpoint}"
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()  # type: ignore[no-any-return]

    async def search_videos(
        self,
        channel_id: str,
        query: str,
        max_results: int = 50,
    ) -> list[YouTubeVideoItem]:
        """Search videos in a channel matching a query."""
        try:
            data = await self._get(
                "search",
                {
                    "part": "snippet",
                    "channelId": channel_id,
                    "q": query,
                    "type": "video",
                    "maxResults": min(max_results, 50),
                    "order": "relevance",
                },
            )
        except httpx.HTTPError as e:
            logger.warning("[YouTube] Search failed for ch=%s q=%r: %s", channel_id, query, e)
            return []

        items = []
        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            video_id = item.get("id", {}).get("videoId", "")
            if not video_id:
                continue
            items.append(
                YouTubeVideoItem(
                    video_id=video_id,
                    title=snippet.get("title", ""),
                    description=snippet.get("description", ""),
                    published_at=snippet.get("publishedAt", ""),
                    thumbnail_url=snippet.get("thumbnails", {}).get("default", {}).get("url", ""),
                    channel_id=channel_id,
                    raw=item,
                ),
            )
        return items

    async def get_channel_playlists(self, channel_id: str, max_results: int = 20) -> list[dict[str, Any]]:
        """List playlists for a channel."""
        try:
            data = await self._get(
                "playlists",
                {
                    "part": "snippet",
                    "channelId": channel_id,
                    "maxResults": min(max_results, 50),
                },
            )
        except httpx.HTTPError as e:
            logger.warning("[YouTube] Playlists failed for ch=%s: %s", channel_id, e)
            return []
        return data.get("items", [])  # type: ignore[no-any-return]

    async def get_playlist_items(self, playlist_id: str, max_results: int = 50) -> list[YouTubeVideoItem]:
        """List videos in a specific playlist."""
        items: list[YouTubeVideoItem] = []
        page_token: str | None = None

        while True:
            params: dict[str, Any] = {
                "part": "snippet",
                "playlistId": playlist_id,
                "maxResults": min(max_results - len(items), 50),
            }
            if page_token:
                params["pageToken"] = page_token

            try:
                data = await self._get("playlistItems", params)
            except httpx.HTTPError as e:
                logger.warning("[YouTube] PlaylistItems failed for pl=%s: %s", playlist_id, e)
                break

            for item in data.get("items", []):
                snippet = item.get("snippet", {})
                resource = snippet.get("resourceId", {})
                video_id = resource.get("videoId", "")
                if not video_id:
                    continue
                items.append(
                    YouTubeVideoItem(
                        video_id=video_id,
                        title=snippet.get("title", ""),
                        description=snippet.get("description", ""),
                        published_at=snippet.get("publishedAt", ""),
                        thumbnail_url=snippet.get("thumbnails", {}).get("default", {}).get("url", ""),
                        playlist_id=playlist_id,
                        channel_id=snippet.get("channelId"),
                        raw=item,
                    ),
                )
                if len(items) >= max_results:
                    break

            page_token = data.get("nextPageToken")
            if not page_token or len(items) >= max_results:
                break

        return items
