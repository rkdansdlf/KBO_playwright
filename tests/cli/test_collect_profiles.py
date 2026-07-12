from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.collect_profiles import collect_profiles, main


class TestCollectProfilesCLI:
    def test_main_default(self):
        with (
            patch("src.cli.collect_profiles.SessionLocal") as mock_sesh,
            patch("src.cli.collect_profiles.PlayerRepository"),
            patch("src.cli.collect_profiles.AsyncPlaywrightPool"),
            patch("src.cli.collect_profiles.PlayerProfileCrawler"),
            patch("sys.argv", ["collect_profiles"]),
        ):
            mock_session = MagicMock()
            mock_session.execute.return_value.scalars.return_value.all.return_value = []
            mock_sesh.return_value = mock_session
            main()

    def test_main_with_limit(self):
        with (
            patch("src.cli.collect_profiles.SessionLocal") as mock_sesh,
            patch("src.cli.collect_profiles.PlayerRepository"),
            patch("src.cli.collect_profiles.AsyncPlaywrightPool"),
            patch("src.cli.collect_profiles.PlayerProfileCrawler"),
            patch("sys.argv", ["collect_profiles", "--limit", "50"]),
        ):
            mock_session = MagicMock()
            mock_session.execute.return_value.scalars.return_value.all.return_value = []
            mock_sesh.return_value = mock_session
            main()

    def test_main_with_ids(self):
        with (
            patch("src.cli.collect_profiles.SessionLocal") as mock_sesh,
            patch("src.cli.collect_profiles.PlayerRepository"),
            patch("src.cli.collect_profiles.AsyncPlaywrightPool"),
            patch("src.cli.collect_profiles.PlayerProfileCrawler"),
            patch("sys.argv", ["collect_profiles", "--ids", "12345,67890"]),
        ):
            mock_session = MagicMock()
            mock_session.execute.return_value.scalars.return_value.all.return_value = []
            mock_sesh.return_value = mock_session
            main()


class TestCollectProfiles:
    @pytest.mark.asyncio
    async def test_collect_profiles_upserts_fetched_profile_and_closes_session(self):
        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = [
            SimpleNamespace(kbo_person_id="123", name_kor="홍길동"),
        ]
        pool = MagicMock()
        pool.__aenter__ = AsyncMock(return_value=pool)
        pool.__aexit__ = AsyncMock(return_value=False)
        crawler = MagicMock()
        crawler.crawl_player_profile = AsyncMock(
            return_value={
                "name": "홍길동",
                "height_cm": 180,
                "weight_kg": 80,
                "education_path": ["KBO High"],
            },
        )
        repo = MagicMock()

        with (
            patch("src.cli.collect_profiles.SessionLocal", return_value=session),
            patch("src.cli.collect_profiles.PlayerRepository", return_value=repo),
            patch("src.cli.collect_profiles.AsyncPlaywrightPool", return_value=pool),
            patch("src.cli.collect_profiles.PlayerProfileCrawler", return_value=crawler),
        ):
            await collect_profiles(target_ids=["123"])

        crawler.crawl_player_profile.assert_awaited_once_with("123")
        player_id, parsed = repo.upsert_player_profile.call_args.args
        assert player_id == "123"
        assert parsed.player_name == "홍길동"
        assert parsed.height_cm == 180
        assert parsed.education_or_career_path == ["KBO High"]
        session.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_collect_profiles_skips_players_without_kbo_id(self):
        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = [
            SimpleNamespace(kbo_person_id=None, name_kor="미확인"),
        ]
        pool = MagicMock()
        pool.__aenter__ = AsyncMock(return_value=pool)
        pool.__aexit__ = AsyncMock(return_value=False)
        crawler = MagicMock()
        crawler.crawl_player_profile = AsyncMock()

        with (
            patch("src.cli.collect_profiles.SessionLocal", return_value=session),
            patch("src.cli.collect_profiles.PlayerRepository"),
            patch("src.cli.collect_profiles.AsyncPlaywrightPool", return_value=pool),
            patch("src.cli.collect_profiles.PlayerProfileCrawler", return_value=crawler),
        ):
            await collect_profiles(limit=1)

        crawler.crawl_player_profile.assert_not_awaited()
        session.close.assert_called_once()
