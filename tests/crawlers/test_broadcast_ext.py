from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.crawlers.broadcast_crawler import BroadcastCrawler


class TestNormalizeGameIds:
    def test_empty(self):
        assert BroadcastCrawler._normalize_game_ids([], 2023) == []

    def test_none_game_date_skipped(self):
        data = [
            {
                "game_date": None,
                "away_team_code": "LG",
                "home_team_code": "SS",
                "broadcaster": "SPOTV",
                "channel_name": "SPOTV",
                "source": "KBO",
            }
        ]
        assert BroadcastCrawler._normalize_game_ids(data, 2023) == []

    def test_invalid_date_format_skipped(self):
        data = [
            {
                "game_date": "invalid",
                "away_team_code": "LG",
                "home_team_code": "SS",
                "broadcaster": "SPOTV",
                "channel_name": "SPOTV",
                "source": "KBO",
            }
        ]
        assert BroadcastCrawler._normalize_game_ids(data, 2023) == []

    def test_missing_team_skipped(self):
        data = [
            {
                "game_date": "20230625",
                "away_team_code": None,
                "home_team_code": "SS",
                "broadcaster": "SPOTV",
                "channel_name": "SPOTV",
                "source": "KBO",
            }
        ]
        assert BroadcastCrawler._normalize_game_ids(data, 2023) == []

    def test_uses_season_year(self):
        data = [
            {
                "game_date": "20230625",
                "away_team_code": "LG",
                "home_team_code": "SS",
                "broadcaster": "SPOTV",
                "channel_name": "SPOTV",
                "source": "KBO",
            }
        ]
        result = BroadcastCrawler._normalize_game_ids(data, 2023)
        assert len(result) == 1
        assert "20230625" in result[0]["game_id"]

    def test_source_default_kbo(self):
        data = [
            {
                "game_date": "20230625",
                "away_team_code": "LG",
                "home_team_code": "SS",
                "broadcaster": "SPOTV",
                "channel_name": "SPOTV",
            }
        ]
        result = BroadcastCrawler._normalize_game_ids(data, 2023)
        assert result[0]["source"] == "KBO"

    def test_preserves_custom_source(self):
        data = [
            {
                "game_date": "20230625",
                "away_team_code": "LG",
                "home_team_code": "SS",
                "broadcaster": "SPOTV",
                "channel_name": "SPOTV",
                "source": "CUSTOM",
            }
        ]
        result = BroadcastCrawler._normalize_game_ids(data, 2023)
        assert result[0]["source"] == "CUSTOM"

    def test_multiple_teams(self):
        data = [
            {
                "game_date": "20230625",
                "away_team_code": "LG",
                "home_team_code": "SS",
                "broadcaster": "SPOTV",
                "channel_name": "SPOTV",
                "source": "KBO",
            },
            {
                "game_date": "20230625",
                "away_team_code": "KT",
                "home_team_code": "NC",
                "broadcaster": "SBS",
                "channel_name": "SBS",
                "source": "KBO",
            },
            {
                "game_date": "20230626",
                "away_team_code": "두산",
                "home_team_code": "KIA",
                "broadcaster": "KBS",
                "channel_name": "KBS",
                "source": "KBO",
            },
        ]
        result = BroadcastCrawler._normalize_game_ids(data, 2023)
        assert len(result) == 3

    def test_handles_all_teams(self):
        teams = ["LG", "KT", "NC", "두산", "�데", "삼성", "키움", "한화", "KIA", "SSG"]
        data = [
            {
                "game_date": "20230625",
                "away_team_code": t1,
                "home_team_code": t2,
                "broadcaster": "SPOTV",
                "channel_name": "SPOTV",
                "source": "KBO",
            }
            for t1, t2 in zip(teams, reversed(teams), strict=False)
        ]
        result = BroadcastCrawler._normalize_game_ids(data, 2023)
        assert len(result) == len(teams)


class TestSaveToDb:
    def test_save_empty_data(self):
        crawler = BroadcastCrawler()
        crawler._save_to_db([])

    def test_save_handles_sqlalchemy_error(self):
        mock_session = MagicMock()
        mock_repo = MagicMock()
        from sqlalchemy.exc import SQLAlchemyError

        mock_repo.save_broadcast.side_effect = SQLAlchemyError("DB error", None, None)
        with (
            patch("src.crawlers.broadcast_crawler.SessionLocal", return_value=mock_session),
            patch("src.crawlers.broadcast_crawler.BroadcastRepository", return_value=mock_repo),
        ):
            crawler = BroadcastCrawler()
            crawler._save_to_db(
                [{"game_id": "20230625LGSS0", "broadcaster": "SPOTV", "channel_name": "SPOTV", "source": "KBO"}]
            )

    def test_save_rollback_on_error(self):
        from sqlalchemy.exc import SQLAlchemyError

        mock_session = MagicMock()
        mock_repo = MagicMock()
        mock_repo.save_broadcast.return_value = None
        mock_session.commit.side_effect = SQLAlchemyError("commit fail", None, None)
        with (
            patch("src.crawlers.broadcast_crawler.SessionLocal", return_value=mock_session),
            patch("src.crawlers.broadcast_crawler.BroadcastRepository", return_value=mock_repo),
        ):
            crawler = BroadcastCrawler()
            crawler._save_to_db(
                [{"game_id": "20230625LGSS0", "broadcaster": "SPOTV", "channel_name": "SPOTV", "source": "KBO"}]
            )
            mock_session.rollback.assert_called()

    def test_save_calls_close(self):
        mock_session = MagicMock()
        mock_repo = MagicMock()
        mock_repo.save_broadcast.return_value = None
        with (
            patch("src.crawlers.broadcast_crawler.SessionLocal", return_value=mock_session),
            patch("src.crawlers.broadcast_crawler.BroadcastRepository", return_value=mock_repo),
        ):
            crawler = BroadcastCrawler()
            crawler._save_to_db(
                [{"game_id": "20230625LGSS0", "broadcaster": "SPOTV", "channel_name": "SPOTV", "source": "KBO"}]
            )
            mock_session.close.assert_called()
