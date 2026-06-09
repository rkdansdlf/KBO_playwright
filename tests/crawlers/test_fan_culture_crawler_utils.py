from __future__ import annotations

from datetime import datetime

import pytest

from src.crawlers.fan_culture_crawler import _extract_season, _video_to_song
from src.utils.youtube_api_client import YouTubeVideoItem


class TestExtractSeason:
    def test_with_season_keyword(self):
        assert _extract_season("2025 시즌 응원가 모음") == 2025

    def test_year_only(self):
        assert _extract_season("2026 응원가") == 2026

    def test_year_with_season_english(self):
        assert _extract_season("2025 season cheering song") == 2025

    def test_no_year(self):
        assert _extract_season("응원가 모음", fallback=2026) == 2026

    def test_no_year_no_fallback(self):
        assert _extract_season("LG 트윈스 응원가") is None

    def test_invalid_year(self):
        assert _extract_season("abc year 응원가") is None


class TestVideoToSong:
    def test_basic_conversion(self):
        item = YouTubeVideoItem(
            video_id="abc123",
            title="홍길동 응원가 (2025 시즌)",
            description="홍길동 선수 응원가입니다",
            published_at="2025-03-01T00:00:00Z",
            thumbnail_url="https://example.com/thumb.jpg",
            channel_id="UCtest",
        )
        result = _video_to_song(item, "LG", 2026)
        assert result is not None
        assert result["team_id"] == "LG"
        assert result["song_name"] == "홍길동 응원가 (2025 시즌)"
        assert result["song_type"] == "PLAYER"
        assert result["video_url"] == "https://www.youtube.com/watch?v=abc123"
        assert result["introduction_year"] == 2025

    def test_excluded_pattern(self):
        item = YouTubeVideoItem(
            video_id="excl1",
            title="2025 시즌 하이라이트",
            description="",
            published_at="2025-03-01T00:00:00Z",
            thumbnail_url="",
            channel_id="UCtest",
        )
        result = _video_to_song(item, "LG", 2026)
        assert result is None

    def test_no_cheersong_keyword(self):
        item = YouTubeVideoItem(
            video_id="no1",
            title="LG 트윈스 경기 영상",
            description="",
            published_at="2025-03-01T00:00:00Z",
            thumbnail_url="",
            channel_id="UCtest",
        )
        result = _video_to_song(item, "LG", 2026)
        assert result is None

    def test_team_song_type(self):
        item = YouTubeVideoItem(
            video_id="team1",
            title="LG 트윈스 팀 응원가",
            description="",
            published_at="2025-03-01T00:00:00Z",
            thumbnail_url="",
            channel_id="UCtest",
        )
        result = _video_to_song(item, "LG", 2026)
        assert result is not None
        assert result["song_type"] == "TEAM"

    def test_fallback_season(self):
        item = YouTubeVideoItem(
            video_id="fback1",
            title="새로운 응원가",
            description="",
            published_at="2026-06-01T00:00:00Z",
            thumbnail_url="",
            channel_id="UCtest",
        )
        result = _video_to_song(item, "OB", 2026)
        assert result is not None
        assert result["team_id"] == "OB"
        assert result["introduction_year"] == 2026  # fallback to current_season

    def test_player_name_extracted(self):
        item = YouTubeVideoItem(
            video_id="play1",
            title="김철수 응원가",
            description="",
            published_at="2025-04-01T00:00:00Z",
            thumbnail_url="",
            channel_id="UCtest",
        )
        result = _video_to_song(item, "SS", 2026)
        assert result is not None
        assert "김철수" in result["description"]
        assert result["song_type"] == "PLAYER"
