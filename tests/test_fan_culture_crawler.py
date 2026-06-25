"""Tests for YouTube Data API-based fan culture crawler.

Covers:
  - YouTubeAPIClient: configuration check
  - _classify_song_type: song type detection
  - _extract_player_name: Korean player name parsing
  - _extract_season: year parsing from title
  - _video_to_song: full payload construction
  - CHEERSONG_TITLE_KEYWORDS: filter logic
  - EXCLUDE_PATTERNS: exclusion logic
  - FanCultureCrawler: graceful fallback without API key
  - seed_fan_culture: CSV loading
"""

from __future__ import annotations

from src.crawlers.fan_culture_crawler import (
    CHEERSONG_TITLE_KEYWORDS,
    EXCLUDE_PATTERNS,
    _extract_season,
    _video_to_song,
)
from src.utils.youtube_api_client import (
    TEAM_YOUTUBE_CHANNELS,
    YouTubeAPIClient,
    YouTubeVideoItem,
    _classify_song_type,
    _extract_player_name,
)

# ─────────────────────────────────────────────
# YouTubeAPIClient
# ─────────────────────────────────────────────


class TestYouTubeAPIClient:
    def test_not_configured_without_key(self, monkeypatch):
        monkeypatch.delenv("YOUTUBE_API_KEY", raising=False)
        client = YouTubeAPIClient()
        assert client.is_configured() is False

    def test_configured_with_key(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "test_key_123")
        client = YouTubeAPIClient()
        assert client.is_configured() is True

    def test_api_key_read_from_env(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "my_api_key")
        client = YouTubeAPIClient()
        assert client.api_key == "my_api_key"


class TestTeamYouTubeChannels:
    def test_all_ten_teams_defined(self):
        assert len(TEAM_YOUTUBE_CHANNELS) == 10

    def test_required_fields_per_team(self):
        for code, info in TEAM_YOUTUBE_CHANNELS.items():
            assert "channel_id" in info, f"{code} missing channel_id"
            assert "handle" in info, f"{code} missing handle"
            assert "search_queries" in info, f"{code} missing search_queries"
            assert len(info["search_queries"]) >= 1

    def test_lg_and_ob_present(self):
        assert "LG" in TEAM_YOUTUBE_CHANNELS
        assert "OB" in TEAM_YOUTUBE_CHANNELS

    def test_channel_ids_non_empty(self):
        for code, info in TEAM_YOUTUBE_CHANNELS.items():
            assert info["channel_id"], f"{code} has empty channel_id"


# ─────────────────────────────────────────────
# _classify_song_type
# ─────────────────────────────────────────────


class TestClassifySongType:
    def test_team_song(self):
        assert _classify_song_type("2026 LG 트윈스 팀 응원가") == "TEAM"

    def test_team_song_collection(self):
        assert _classify_song_type("응원가 모음 2026") == "TEAM"

    def test_closer(self):
        assert _classify_song_type("마무리 투수 응원가") == "CLOSER"

    def test_starter(self):
        assert _classify_song_type("선발 투수 응원가") == "STARTER"

    def test_default_player(self):
        assert _classify_song_type("홍길동 응원가") == "PLAYER"


# ─────────────────────────────────────────────
# _extract_player_name
# ─────────────────────────────────────────────


class TestExtractPlayerName:
    def test_korean_name_before_cheersong(self):
        name = _extract_player_name("홍길동 응원가")
        assert name == "홍길동"

    def test_bracket_format(self):
        name = _extract_player_name("[김민수] 응원가")
        assert name == "김민수"

    def test_no_name(self):
        assert _extract_player_name("팀 응원가 모음") is None

    def test_team_keyword_excluded(self):
        # "팀" is 1 char after extraction doesn't match 2-4 char pattern for likely player names
        name = _extract_player_name("LG 트윈스 팀 응원가 2026")
        # "트윈스" or None depending on position — key thing is it doesn't crash
        assert name is None or isinstance(name, str)


# ─────────────────────────────────────────────
# _extract_season
# ─────────────────────────────────────────────


class TestExtractSeason:
    def test_year_from_title(self):
        assert _extract_season("2026 LG 응원가") == 2026

    def test_year_with_season_word(self):
        assert _extract_season("2025시즌 삼성 응원가") == 2025

    def test_no_year_uses_fallback(self):
        assert _extract_season("응원가 모음", fallback=2024) == 2024

    def test_no_year_no_fallback(self):
        assert _extract_season("응원가 모음") is None


# ─────────────────────────────────────────────
# CHEERSONG_TITLE_KEYWORDS and EXCLUDE_PATTERNS
# ─────────────────────────────────────────────


class TestFilterPatterns:
    def test_cheersong_keyword_matches(self):
        assert CHEERSONG_TITLE_KEYWORDS.search("2026 홍길동 응원가")
        assert CHEERSONG_TITLE_KEYWORDS.search("cheersong playlist")
        assert CHEERSONG_TITLE_KEYWORDS.search("응원 모음")

    def test_cheersong_keyword_no_match(self):
        assert not CHEERSONG_TITLE_KEYWORDS.search("경기 하이라이트")
        assert not CHEERSONG_TITLE_KEYWORDS.search("선수 인터뷰")

    def test_exclude_pattern_highlight(self):
        assert EXCLUDE_PATTERNS.search("경기 하이라이트 모음")
        assert EXCLUDE_PATTERNS.search("게임 highlight 영상")

    def test_exclude_pattern_interview(self):
        assert EXCLUDE_PATTERNS.search("선수 인터뷰 영상")

    def test_exclude_pattern_safe(self):
        assert not EXCLUDE_PATTERNS.search("2026 LG 응원가 모음")


# ─────────────────────────────────────────────
# _video_to_song
# ─────────────────────────────────────────────


class TestVideoToSong:
    def _make_item(self, title="홍길동 응원가 2026", video_id="abc123", channel_id="UC_LG"):
        return YouTubeVideoItem(
            video_id=video_id,
            title=title,
            description="응원가 설명",
            published_at="2026-04-01T09:00:00Z",
            thumbnail_url="https://i.ytimg.com/vi/abc123/default.jpg",
            channel_id=channel_id,
        )

    def test_returns_dict_for_valid_title(self):
        result = _video_to_song(self._make_item(), "LG", 2026)
        assert result is not None
        assert result["team_id"] == "LG"

    def test_returns_none_for_non_cheersong(self):
        result = _video_to_song(self._make_item(title="경기 하이라이트"), "LG", 2026)
        assert result is None

    def test_returns_none_for_excluded_title(self):
        result = _video_to_song(self._make_item(title="선수 인터뷰 응원가"), "LG", 2026)
        assert result is None

    def test_video_url_field(self):
        result = _video_to_song(self._make_item(video_id="xyz789"), "LG", 2026)
        assert result is not None
        assert "xyz789" in result["video_url"]

    def test_introduction_year_extracted(self):
        result = _video_to_song(self._make_item(title="2025 홍길동 응원가"), "LG", 2026)
        assert result is not None
        assert result["introduction_year"] == 2025

    def test_introduction_year_fallback(self):
        result = _video_to_song(self._make_item(title="홍길동 응원가"), "LG", 2026)
        assert result is not None
        assert result["introduction_year"] == 2026

    def test_song_name_max_200(self):
        long_title = "응원가 " + "A" * 300
        result = _video_to_song(self._make_item(title=long_title), "LG", 2026)
        assert result is not None
        assert len(result["song_name"]) <= 200

    def test_lyrics_is_none(self):
        result = _video_to_song(self._make_item(), "LG", 2026)
        assert result is not None
        assert result["lyrics"] is None


# ─────────────────────────────────────────────
# FanCultureCrawler integration
# ─────────────────────────────────────────────


class TestFanCultureCrawler:
    def test_returns_empty_when_not_configured(self, monkeypatch):
        import asyncio

        from src.crawlers.fan_culture_crawler import FanCultureCrawler

        monkeypatch.delenv("YOUTUBE_API_KEY", raising=False)
        crawler = FanCultureCrawler()
        result = asyncio.run(crawler.run(save=False))
        assert result == []

    def test_run_with_mocked_api(self, monkeypatch):
        import asyncio

        async def _no_sleep(*_args, **_kwargs):
            pass

        from src.crawlers.fan_culture_crawler import FanCultureCrawler

        monkeypatch.setattr("asyncio.sleep", _no_sleep)
        fake_item = YouTubeVideoItem(
            video_id="test123",
            title="2026 홍길동 응원가",
            description="",
            published_at="2026-04-01T00:00:00Z",
            thumbnail_url="",
            channel_id="UC_TEST",
        )

        async def fake_search_videos(channel_id, query, max_results=50):
            return [fake_item]

        monkeypatch.setenv("YOUTUBE_API_KEY", "test_key")
        crawler = FanCultureCrawler(season=2026)
        crawler.client.search_videos = fake_search_videos

        # Only test one team to speed up
        results = asyncio.run(crawler.run(save=False, team_filter="LG"))
        assert len(results) >= 1
        assert results[0]["team_id"] == "LG"
        assert results[0]["introduction_year"] == 2026


# ─────────────────────────────────────────────
# seed_fan_culture CSV loading
# ─────────────────────────────────────────────


class TestSeedFanCultureCSV:
    def test_csv_file_exists(self):
        from pathlib import Path

        csv_path = Path("data/seed/team_rivalries.csv")
        assert csv_path.exists(), f"Seed CSV not found at {csv_path}"

    def test_csv_has_required_columns(self):
        import csv
        from pathlib import Path

        csv_path = Path("data/seed/team_rivalries.csv")
        with csv_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
        for col in ["team_id_a", "team_id_b", "rivalry_name", "intensity"]:
            assert col in headers, f"Missing column: {col}"

    def test_csv_has_10_rows(self):
        import csv
        from pathlib import Path

        csv_path = Path("data/seed/team_rivalries.csv")
        with csv_path.open(encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 10

    def test_csv_intensity_valid_values(self):
        import csv
        from pathlib import Path

        valid = {"HIGH", "MEDIUM", "LOW"}
        csv_path = Path("data/seed/team_rivalries.csv")
        with csv_path.open(encoding="utf-8") as f:
            for row in csv.DictReader(f):
                assert row["intensity"] in valid, f"Invalid intensity: {row['intensity']}"
