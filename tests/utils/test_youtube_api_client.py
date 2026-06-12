import pytest

from src.utils.youtube_api_client import (
    TEAM_YOUTUBE_CHANNELS,
    YouTubeAPIClient,
    YouTubeVideoItem,
    _classify_song_type,
    _extract_player_name,
)


class TestClassifySongType:
    def test_team_song(self):
        assert _classify_song_type("팀 응원가 모음") == "TEAM"
        assert _classify_song_type("team song") == "TEAM"

    def test_leadoff(self):
        assert _classify_song_type("1번타자 응원가") == "LEADOFF"

    def test_walkoff(self):
        assert _classify_song_type("끝내기 응원가") == "WALKOFF"

    def test_closer(self):
        assert _classify_song_type("마무리 클로저") == "CLOSER"

    def test_starter(self):
        assert _classify_song_type("선발 투수") == "STARTER"

    def test_default_player(self):
        assert _classify_song_type("일반 노래") == "PLAYER"


class TestExtractPlayerName:
    def test_simple_name(self):
        assert _extract_player_name("홍길동 응원가") == "홍길동"

    def test_bracketed_name(self):
        assert _extract_player_name("[홍길동] 응원가") == "홍길동"

    def test_no_match(self):
        assert _extract_player_name("일반 영상") is None

    def test_two_char_name(self):
        assert _extract_player_name("길동 응원가") == "길동"

    def test_four_char_name(self):
        assert _extract_player_name("홍길동이 응원가") == "홍길동이"


class TestYouTubeVideoItem:
    def test_video_item_fields(self):
        item = YouTubeVideoItem(
            video_id="abc123",
            title="Test",
            description="Desc",
            published_at="2025-01-01",
            thumbnail_url="http://img.com/1.jpg",
        )
        assert item.video_id == "abc123"
        assert item.title == "Test"
        assert item.playlist_id is None


class TestTeamYoutubeChannels:
    def test_all_10_teams_present(self):
        assert len(TEAM_YOUTUBE_CHANNELS) == 10

    def test_each_team_has_required_keys(self):
        for _code, info in TEAM_YOUTUBE_CHANNELS.items():
            assert "handle" in info
            assert "channel_id" in info
            assert "name" in info
            assert "search_queries" in info

    def test_lg_channel(self):
        lg = TEAM_YOUTUBE_CHANNELS["LG"]
        assert lg["handle"] == "@LGTwinsTV"
        assert lg["channel_id"] == "UCiXGdRARMxrZ5kJLe7t4Xpg"

    def test_search_queries_contain_cheersong(self):
        for _code, info in TEAM_YOUTUBE_CHANNELS.items():
            assert any("응원가" in q for q in info["search_queries"])


@pytest.mark.asyncio
class TestYouTubeAPIClient:
    async def test_not_configured(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "")
        client = YouTubeAPIClient()
        assert client.is_configured() is False

    async def test_configured(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "test-key")
        client = YouTubeAPIClient()
        assert client.is_configured() is True

    async def test_search_videos_no_key_returns_empty(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "")
        client = YouTubeAPIClient()
        results = await client.search_videos("channel1", "응원가")
        assert results == []

    async def test_get_channel_playlists_no_key(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "")
        client = YouTubeAPIClient()
        results = await client.get_channel_playlists("channel1")
        assert results == []

    async def test_get_playlist_items_no_key(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "")
        client = YouTubeAPIClient()
        results = await client.get_playlist_items("playlist1")
        assert results == []
