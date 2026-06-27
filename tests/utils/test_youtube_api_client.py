from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

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
        assert lg["channel_id"] == "UCL6QZZxb-HR4hCh_eFAnQWA"

    def test_search_queries_contain_cheersong(self):
        for _code, info in TEAM_YOUTUBE_CHANNELS.items():
            assert any("응원가" in q for q in info["search_queries"])


def _make_search_response():
    return {
        "items": [
            {
                "id": {"videoId": "vid001"},
                "snippet": {
                    "title": "홍길동 응원가",
                    "description": "Player cheer song",
                    "publishedAt": "2025-06-01T00:00:00Z",
                    "thumbnails": {"default": {"url": "http://img.com/1.jpg"}},
                    "channelId": "UCtest123",
                },
            },
            {
                "id": {"videoId": "vid002"},
                "snippet": {
                    "title": "팀 응원가 모음",
                    "description": "Team song collection",
                    "publishedAt": "2025-06-02T00:00:00Z",
                    "thumbnails": {"default": {"url": "http://img.com/2.jpg"}},
                    "channelId": "UCtest123",
                },
            },
        ]
    }


def _make_playlists_response():
    return {
        "items": [
            {
                "id": "PL001",
                "snippet": {"title": "응원가 모음", "channelId": "UCtest123"},
            }
        ]
    }


def _make_playlist_items_response(next_page_token=None):
    data = {
        "items": [
            {
                "snippet": {
                    "title": "Player A 응원가",
                    "description": "Desc A",
                    "publishedAt": "2025-05-01T00:00:00Z",
                    "thumbnails": {"default": {"url": "http://img.com/a.jpg"}},
                    "resourceId": {"videoId": "pl_vid001"},
                    "channelId": "UCtest123",
                }
            },
            {
                "snippet": {
                    "title": "Player B 응원가",
                    "description": "Desc B",
                    "publishedAt": "2025-05-02T00:00:00Z",
                    "thumbnails": {"default": {"url": "http://img.com/b.jpg"}},
                    "resourceId": {"videoId": "pl_vid002"},
                    "channelId": "UCtest123",
                }
            },
        ]
    }
    if next_page_token:
        data["nextPageToken"] = next_page_token
    return data


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

    async def test_search_videos_success(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "test-key")
        client = YouTubeAPIClient()

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = _make_search_response()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            results = await client.search_videos("UCtest123", "응원가")

        assert len(results) == 2
        assert results[0].video_id == "vid001"
        assert results[0].title == "홍길동 응원가"
        assert results[0].channel_id == "UCtest123"
        assert results[1].video_id == "vid002"
        mock_client.get.assert_called_once()

    async def test_search_videos_skips_items_without_video_id(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "test-key")
        client = YouTubeAPIClient()

        response_data = {
            "items": [
                {"id": {"kind": "youtube#channel"}, "snippet": {"title": "no video id"}},
                {
                    "id": {"videoId": "valid001"},
                    "snippet": {
                        "title": "Valid Video",
                        "description": "",
                        "publishedAt": "",
                        "thumbnails": {},
                    },
                },
            ]
        }

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = response_data

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            results = await client.search_videos("UCtest123", "query")

        assert len(results) == 1
        assert results[0].video_id == "valid001"

    async def test_search_videos_http_error_returns_empty(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "test-key")
        client = YouTubeAPIClient()

        import httpx

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(side_effect=httpx.HTTPError("connection failed"))

        with patch("httpx.AsyncClient", return_value=mock_client):
            results = await client.search_videos("UCtest123", "query")

        assert results == []

    async def test_get_channel_playlists_success(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "test-key")
        client = YouTubeAPIClient()

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = _make_playlists_response()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            results = await client.get_channel_playlists("UCtest123")

        assert len(results) == 1
        assert results[0]["id"] == "PL001"

    async def test_get_channel_playlists_http_error(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "test-key")
        client = YouTubeAPIClient()

        import httpx

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(side_effect=httpx.HTTPError("timeout"))

        with patch("httpx.AsyncClient", return_value=mock_client):
            results = await client.get_channel_playlists("UCtest123")

        assert results == []

    async def test_get_playlist_items_success(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "test-key")
        client = YouTubeAPIClient()

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = _make_playlist_items_response()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            results = await client.get_playlist_items("PL001")

        assert len(results) == 2
        assert results[0].video_id == "pl_vid001"
        assert results[0].playlist_id == "PL001"
        assert results[1].video_id == "pl_vid002"

    async def test_get_playlist_items_pagination(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "test-key")
        client = YouTubeAPIClient()

        page1 = _make_playlist_items_response(next_page_token="token123")
        page2 = {
            "items": [
                {
                    "snippet": {
                        "title": "Player C 응원가",
                        "description": "Desc C",
                        "publishedAt": "2025-05-03T00:00:00Z",
                        "thumbnails": {"default": {"url": "http://img.com/c.jpg"}},
                        "resourceId": {"videoId": "pl_vid003"},
                        "channelId": "UCtest123",
                    }
                }
            ]
        }

        responses = [page1, page2]
        call_count = 0

        async def mock_get(*args, **kwargs):
            nonlocal call_count
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            resp.json.return_value = responses[call_count]
            call_count += 1
            return resp

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = mock_get

        with patch("httpx.AsyncClient", return_value=mock_client):
            results = await client.get_playlist_items("PL001")

        assert len(results) == 3
        assert results[2].video_id == "pl_vid003"

    async def test_get_playlist_items_skips_without_video_id(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "test-key")
        client = YouTubeAPIClient()

        response_data = {
            "items": [
                {
                    "snippet": {
                        "title": "Deleted Video",
                        "description": "",
                        "publishedAt": "",
                        "thumbnails": {},
                        "resourceId": {},
                    }
                },
                {
                    "snippet": {
                        "title": "Valid Video",
                        "description": "",
                        "publishedAt": "",
                        "thumbnails": {},
                        "resourceId": {"videoId": "valid001"},
                    }
                },
            ]
        }

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = response_data

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            results = await client.get_playlist_items("PL001")

        assert len(results) == 1
        assert results[0].video_id == "valid001"

    async def test_get_playlist_items_respects_max_results(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "test-key")
        client = YouTubeAPIClient()

        response_data = {
            "items": [
                {
                    "snippet": {
                        "title": f"Video {i}",
                        "description": "",
                        "publishedAt": "",
                        "thumbnails": {},
                        "resourceId": {"videoId": f"vid{i:03d}"},
                    }
                }
                for i in range(3)
            ]
        }

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = response_data

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            results = await client.get_playlist_items("PL001", max_results=2)

        assert len(results) == 2

    async def test_get_playlist_items_http_error_breaks_loop(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_API_KEY", "test-key")
        client = YouTubeAPIClient()

        import httpx

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(side_effect=httpx.HTTPError("server error"))

        with patch("httpx.AsyncClient", return_value=mock_client):
            results = await client.get_playlist_items("PL001")

        assert results == []
