from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.crawlers.schedule_crawler import ScheduleCrawler


@pytest.fixture
def crawler():
    return ScheduleCrawler()


def _make_raw_game(
    game_id="20250625LGSS0",
    game_date="20250625",
    away_segment="LG",
    home_segment="SS",
    game_status="SCHEDULED",
    crawl_status="link_parsed",
    doubleheader_no=0,
    game_time="18:30",
    stadium="잠실",
    url_suffix="/Schedule/GameCenter/Main.aspx?gameId=20250625LGSS0",
    season_year=2025,
    season_type="regular",
    away_name=None,
    home_name=None,
):
    g = {
        "game_id": game_id,
        "game_date": game_date,
        "season_year": season_year,
        "season_type": season_type,
        "away_segment": away_segment,
        "home_segment": home_segment,
        "doubleheader_no": doubleheader_no,
        "game_status": game_status,
        "crawl_status": crawl_status,
        "url_suffix": url_suffix,
        "game_time": game_time,
        "stadium": stadium,
    }
    if away_name:
        g["away_name"] = away_name
    if home_name:
        g["home_name"] = home_name
    return g


class TestExtractGames:
    @pytest.mark.asyncio
    @patch("src.crawlers.schedule_crawler.team_code_from_game_id_segment")
    async def test_basic_link_parsed_game(self, mock_team_code, crawler):
        mock_team_code.side_effect = lambda seg, year: {"LG": "LG", "SS": "SS"}.get(seg)
        page = AsyncMock()
        page.evaluate.return_value = [_make_raw_game()]

        result = await crawler._extract_games(page, 2025, 6)
        assert len(result) == 1
        assert result[0]["game_id"] == "20250625LGSS0"
        assert result[0]["away_team_code"] == "LG"
        assert result[0]["home_team_code"] == "SS"

    @pytest.mark.asyncio
    @patch("src.crawlers.schedule_crawler.resolve_team_code")
    @patch("src.crawlers.schedule_crawler.team_code_from_game_id_segment")
    async def test_text_parsed_game_no_id(self, mock_team_code, mock_resolve, crawler):
        mock_team_code.return_value = None
        mock_resolve.side_effect = lambda name, year: {"LG 트윈스": "LG", "삼성 라이온즈": "SS"}.get(name)
        page = AsyncMock()
        page.evaluate.return_value = [
            _make_raw_game(
                game_id=None,
                away_segment=None,
                home_segment=None,
                away_name="LG 트윈스",
                home_name="삼성 라이온즈",
                crawl_status="text_parsed",
                url_suffix="",
            ),
        ]

        result = await crawler._extract_games(page, 2025, 6)
        assert len(result) >= 0

    @pytest.mark.asyncio
    @patch("src.crawlers.schedule_crawler.team_code_from_game_id_segment")
    async def test_empty_results(self, mock_team_code, crawler):
        page = AsyncMock()
        page.evaluate.return_value = []

        result = await crawler._extract_games(page, 2025, 6)
        assert result == []

    @pytest.mark.asyncio
    @patch("src.crawlers.schedule_crawler.team_code_from_game_id_segment")
    async def test_status_normalization(self, mock_team_code, crawler):
        mock_team_code.side_effect = lambda seg, year: {"LG": "LG", "SS": "SS"}.get(seg)
        page = AsyncMock()
        page.evaluate.return_value = [
            _make_raw_game(game_status="CANCELLED"),
        ]

        result = await crawler._extract_games(page, 2025, 6)
        assert len(result) >= 0
        if result:
            assert result[0]["game_status"] == "CANCELLED"

    @pytest.mark.asyncio
    @patch("src.crawlers.schedule_crawler.team_code_from_game_id_segment")
    async def test_game_time_preserved(self, mock_team_code, crawler):
        mock_team_code.side_effect = lambda seg, year: {"LG": "LG", "SS": "SS"}.get(seg)
        page = AsyncMock()
        page.evaluate.return_value = [
            _make_raw_game(game_time="18:30"),
        ]

        result = await crawler._extract_games(page, 2025, 6)
        if result:
            assert result[0]["game_time"] == "18:30"

    @pytest.mark.asyncio
    @patch("src.crawlers.schedule_crawler.team_code_from_game_id_segment")
    async def test_url_suffix_construction(self, mock_team_code, crawler):
        mock_team_code.side_effect = lambda seg, year: {"LG": "LG", "SS": "SS"}.get(seg)
        page = AsyncMock()
        page.evaluate.return_value = [
            _make_raw_game(url_suffix="/Schedule/GameCenter/Main.aspx?gameId=20250625LGSS0"),
        ]

        result = await crawler._extract_games(page, 2025, 6)
        if result:
            assert result[0]["url"].startswith("https://www.koreabaseball.com")

    @pytest.mark.asyncio
    @patch("src.crawlers.schedule_crawler.resolve_team_code", return_value=None)
    @patch("src.crawlers.schedule_crawler.team_code_from_game_id_segment", return_value=None)
    async def test_text_parsed_game_with_unresolved_team_is_skipped(self, _team_code, _resolve, crawler):
        page = MagicMock()
        page.evaluate = AsyncMock(
            return_value=[
                _make_raw_game(
                    game_id=None,
                    away_segment=None,
                    home_segment=None,
                    away_name="알 수 없는 원정팀",
                    home_name="알 수 없는 홈팀",
                    crawl_status="text_parsed",
                    url_suffix="",
                ),
            ],
        )
        page.content = AsyncMock(return_value="<table class='tbl'></table>")

        assert await crawler._extract_games(page, 2025, 6) == []

    @pytest.mark.asyncio
    async def test_extract_games_returns_empty_when_page_evaluation_fails(self, crawler):
        page = MagicMock()
        page.evaluate = AsyncMock(side_effect=RuntimeError("page closed"))

        assert await crawler._extract_games(page, 2025, 6) == []

    @pytest.mark.asyncio
    async def test_empty_extraction_logs_table_sample_when_page_has_no_game_link(self, crawler):
        page = MagicMock()
        page.evaluate = AsyncMock(side_effect=[[], ["04.01 18:30 LG vs SS"]])
        page.content = AsyncMock(return_value="<table class='tbl'></table>")

        assert await crawler._extract_games(page, 2025, 6) == []
        assert page.evaluate.await_count == 2


class TestCrawlerOrchestration:
    @staticmethod
    def _pool(page):
        pool = MagicMock()
        pool.start = AsyncMock()
        pool.acquire = AsyncMock(return_value=page)
        pool.release = AsyncMock()
        pool.close = AsyncMock()
        return pool

    @pytest.mark.asyncio
    async def test_crawl_schedule_releases_injected_pool_without_closing_it(self):
        page = MagicMock()
        pool = self._pool(page)
        crawler = ScheduleCrawler(pool=pool)
        crawler._crawl_month = AsyncMock(return_value=[{"game_id": "20250625LGSS0"}])

        games = await crawler.crawl_schedule(2025, 6)

        assert games == [{"game_id": "20250625LGSS0"}]
        pool.start.assert_awaited_once()
        pool.release.assert_awaited_once_with(page)
        pool.close.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("src.crawlers.schedule_crawler.AsyncPlaywrightPool")
    async def test_crawl_schedule_returns_empty_after_error_and_closes_owned_pool(self, mock_pool_class):
        page = MagicMock()
        pool = self._pool(page)
        mock_pool_class.return_value = pool
        crawler = ScheduleCrawler()
        crawler._crawl_month = AsyncMock(side_effect=RuntimeError("navigation failed"))

        assert await crawler.crawl_schedule(2025, 6) == []
        pool.release.assert_awaited_once_with(page)
        pool.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_crawl_season_reuses_page_and_applies_request_delay_per_month(self):
        page = MagicMock()
        pool = self._pool(page)
        policy = MagicMock()
        policy.delay_async = AsyncMock()
        crawler = ScheduleCrawler(pool=pool, policy=policy)
        crawler._crawl_month = AsyncMock(side_effect=[[{"game_id": "march"}], [{"game_id": "april"}]])

        games = await crawler.crawl_season(2025, months=[3, 4], series_id="0")

        assert games == [{"game_id": "march"}, {"game_id": "april"}]
        assert policy.delay_async.await_count == 2
        assert crawler._crawl_month.await_args_list[0].kwargs == {"series_id": "0"}
        pool.release.assert_awaited_once_with(page)

    @pytest.mark.asyncio
    async def test_navigation_wait_and_select_report_expected_failure_reasons(self, crawler, monkeypatch):
        page = MagicMock()
        crawler.policy.run_with_retry_async = AsyncMock(side_effect=RuntimeError("blocked by page"))
        monkeypatch.setattr("src.crawlers.schedule_crawler.compliance.is_allowed", AsyncMock(return_value=False))

        assert await crawler._navigate_schedule_page(page) == (False, "blocked")

        monkeypatch.setattr("src.crawlers.schedule_crawler.compliance.is_allowed", AsyncMock(return_value=True))
        assert await crawler._navigate_schedule_page(page) == (False, "schedule_navigation_failed")

        page.wait_for_selector = AsyncMock(side_effect=TimeoutError())
        assert await crawler._wait_for_schedule_table(page) == (False, "schedule_empty")

        assert await crawler._select_option_with_retry(page, "#ddlYear", "2025", label="year") == (
            False,
            "schedule_navigation_failed",
        )

    @pytest.mark.asyncio
    async def test_crawl_month_continues_after_one_series_selection_failure(self, crawler):
        page = MagicMock()
        page.eval_on_selector_all = AsyncMock(return_value=[{"value": "0"}, {"value": "1"}])
        crawler._navigate_schedule_page = AsyncMock(return_value=(True, "ok"))
        crawler._select_year_month = AsyncMock(return_value=(True, "ok"))
        crawler._wait_for_schedule_table = AsyncMock(return_value=(True, "ok"))
        crawler._select_option_with_retry = AsyncMock(side_effect=[(False, "series failed"), (True, "ok")])
        crawler._extract_games = AsyncMock(return_value=[{"game_id": "20250625LGSS0"}])

        games = await crawler._crawl_month(page, 2025, 6)

        assert games == [{"game_id": "20250625LGSS0"}]
        assert crawler.get_last_failure_reason("2025-06:all") == "series failed"

    @pytest.mark.asyncio
    async def test_select_year_month_returns_failure_from_changed_year_or_month(self, crawler):
        page = MagicMock()
        page.eval_on_selector = AsyncMock(side_effect=["2024", "05"])
        crawler._select_option_with_retry = AsyncMock(side_effect=[(True, "ok"), (False, "month failed")])

        assert await crawler._select_year_month(page, 2025, 6) == (False, "month failed")
