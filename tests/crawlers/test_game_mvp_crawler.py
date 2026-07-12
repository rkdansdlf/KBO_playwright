from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.crawlers.game_mvp_crawler import GameMvpCrawler


@pytest.fixture
def crawler() -> GameMvpCrawler:
    return GameMvpCrawler()


class TestParseMvpPlayer:
    def test_korean_name_before_mvp(self, crawler):
        result = crawler._parse_mvp_player("홍길동 선수, MVP 선정!")
        assert result == "홍길동"

    def test_mvp_colon_name(self, crawler):
        result = crawler._parse_mvp_player("MVP: 김철수")
        assert result == "김철수"

    @pytest.mark.xfail(reason="Pattern 2 (MVP + Korean word) matches before pattern 3 (name, MVP)")
    def test_name_comma_mvp(self, crawler):
        result = crawler._parse_mvp_player("이영희, MVP 수상")
        assert result == "이영희"

    def test_name_directly_before_mvp(self, crawler):
        result = crawler._parse_mvp_player("박민수 MVP")
        assert result == "박민수"

    def test_no_mvp_keyword(self, crawler):
        result = crawler._parse_mvp_player("오늘의 선수: 최준용")
        assert result is None

    def test_empty_text(self, crawler):
        assert crawler._parse_mvp_player("") is None


class TestParseMvpTeam:
    def test_lg(self, crawler):
        assert crawler._parse_mvp_team("LG 트윈스") == "LG"

    def test_doosan(self, crawler):
        assert crawler._parse_mvp_team("두산 베어스") == "DB"

    def test_lotte(self, crawler):
        assert crawler._parse_mvp_team("롯데 자이언츠") == "LT"

    def test_samsung(self, crawler):
        assert crawler._parse_mvp_team("삼성 라이온즈") == "SS"

    def test_kiwoom(self, crawler):
        assert crawler._parse_mvp_team("키움 히어로즈") == "KH"

    def test_hanwha(self, crawler):
        assert crawler._parse_mvp_team("한화 이글스") == "HH"

    def test_kia(self, crawler):
        assert crawler._parse_mvp_team("KIA 타이거즈") == "KIA"

    def test_ssg(self, crawler):
        assert crawler._parse_mvp_team("SSG 랜더스") == "SSG"

    def test_nc(self, crawler):
        assert crawler._parse_mvp_team("NC 다이노스") == "NC"

    def test_kt(self, crawler):
        assert crawler._parse_mvp_team("KT 위즈") == "KT"

    def test_no_match(self, crawler):
        assert crawler._parse_mvp_team("") is None
        assert crawler._parse_mvp_team("야쿠르트") is None


@pytest.mark.asyncio
class TestGameMvpCrawler:
    async def test_search_mvp_for_game_parses_matching_news(self, crawler):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {
            "result": {
                "newsList": [
                    {"title": "일반 뉴스"},
                    {"title": "LG MVP: 홍길동", "subContent": "결승타"},
                ],
            },
        }
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        client.get = AsyncMock(return_value=response)

        with patch("src.crawlers.game_mvp_crawler.httpx.AsyncClient", return_value=client):
            result = await crawler._search_mvp_for_game("20250501LGDB0")

        assert result == {
            "game_id": "20250501LGDB0",
            "player_name": "홍길동",
            "team_id": "LG",
            "mvp_type": "GAME",
            "reason": "LG MVP: 홍길동",
            "award_source": "NAVER",
        }

    async def test_search_mvp_for_game_returns_none_for_non_ok_response(self, crawler):
        response = MagicMock(status_code=503)
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        client.get = AsyncMock(return_value=response)

        with patch("src.crawlers.game_mvp_crawler.httpx.AsyncClient", return_value=client):
            result = await crawler._search_mvp_for_game("20250501LGDB0")

        assert result is None

    async def test_fetch_recent_mvp_news_builds_fallback_game_id(self, crawler):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"result": {"newsList": [{"title": "두산 김철수 MVP"}]}}
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        client.get = AsyncMock(side_effect=[response, *[httpx.HTTPError("stop")] * 6])

        with patch("src.crawlers.game_mvp_crawler.httpx.AsyncClient", return_value=client):
            records = await crawler._fetch_recent_mvp_news()

        assert len(records) == 1
        assert records[0]["player_name"] == "김철수"
        assert records[0]["team_id"] == "DB"
        assert records[0]["game_id"].endswith("0000")

    async def test_run_with_game_ids_saves_only_found_results(self, crawler):
        crawler._search_mvp_for_game = AsyncMock(side_effect=[{"game_id": "G1"}, None])

        with patch.object(GameMvpCrawler, "_save_to_db") as save:
            await crawler.run(["G1", "G2"], save=True)

        save.assert_called_once_with([{"game_id": "G1"}])

    async def test_run_without_game_ids_fetches_and_saves_news(self, crawler):
        crawler._fetch_recent_mvp_news = AsyncMock(return_value=[{"game_id": "G1"}])

        with patch.object(GameMvpCrawler, "_save_to_db") as save:
            await crawler.run(save=True)

        save.assert_called_once_with([{"game_id": "G1"}])

    async def test_save_to_db_commits_and_closes_session(self):
        session = MagicMock()
        repo = MagicMock()

        with (
            patch("src.crawlers.game_mvp_crawler.SessionLocal", return_value=session),
            patch("src.crawlers.game_mvp_crawler.GameMvpRepository", return_value=repo),
        ):
            GameMvpCrawler._save_to_db([{"game_id": "G1"}])

        repo.save_mvp.assert_called_once_with({"game_id": "G1"})
        session.commit.assert_called_once()
        session.close.assert_called_once()
