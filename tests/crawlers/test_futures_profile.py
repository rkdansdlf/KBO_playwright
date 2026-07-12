from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from bs4 import BeautifulSoup

from src.crawlers.futures.profile import FuturesProfileCrawler


class TestFuturesProfileTableParsing:
    def test_parses_known_hitter_and_pitcher_tables(self):
        soup = BeautifulSoup(
            """
            <table id="tblHitterRecord"><caption>타자</caption><thead><tr><th>G</th><th>AVG</th></tr></thead>
            <tbody><tr><td>10</td><td>.300</td></tr></tbody></table>
            <table id="tblPitcherRecord" summary="투수"><thead><tr><th>G</th><th>ERA</th></tr></thead>
            <tbody><tr><td>5</td><td>2.00</td></tr></tbody></table>
            """,
            "html.parser",
        )

        tables = FuturesProfileCrawler()._extract_known_futures_tables(soup)

        assert tables == [
            {
                "caption": "타자",
                "summary": "",
                "headers": ["G", "AVG"],
                "rows": [["10", ".300"]],
                "_table_type": "HITTER",
            },
            {
                "caption": None,
                "summary": "투수",
                "headers": ["G", "ERA"],
                "rows": [["5", "2.00"]],
                "_table_type": "PITCHER",
            },
        ]

    def test_fallback_tables_uses_first_row_as_headers(self):
        soup = BeautifulSoup(
            """
            <div id="PlayerFutures"><table><tr><th>G</th><th>AVG</th></tr><tr><td>7</td><td>.250</td></tr></table></div>
            """,
            "html.parser",
        )

        tables = FuturesProfileCrawler()._extract_fallback_futures_tables(soup)

        assert tables == [{"caption": None, "summary": "", "headers": ["G", "AVG"], "rows": [["7", ".250"]]}]

    def test_invalid_table_returns_none(self):
        soup = BeautifulSoup("<table></table>", "html.parser")

        assert FuturesProfileCrawler()._parse_table_with_bs4(soup.table) is None


@pytest.mark.asyncio
class TestFuturesProfileCrawler:
    async def test_extract_profile_text_uses_first_nonempty_selector(self):
        empty = MagicMock()
        empty.inner_text = AsyncMock(return_value="  ")
        profile = MagicMock()
        profile.inner_text = AsyncMock(return_value="  2025 Futures Profile  ")
        page = AsyncMock()
        page.query_selector.side_effect = [None, empty, profile]

        text = await FuturesProfileCrawler()._extract_profile_text(page)

        assert text == "2025 Futures Profile"

    async def test_click_futures_tab_uses_first_available_link(self):
        tab = MagicMock()
        tab.click = AsyncMock()
        page = AsyncMock()
        page.wait_for_selector.side_effect = [None, tab]

        clicked = await FuturesProfileCrawler()._click_futures_tab(page)

        assert clicked is True
        tab.click.assert_awaited_once()

    async def test_scrape_profile_returns_none_when_compliance_blocks_url(self):
        crawler = FuturesProfileCrawler()
        page = AsyncMock()

        with patch("src.crawlers.futures.profile.compliance.is_allowed", new=AsyncMock(return_value=False)):
            result = await crawler._scrape_profile(page, crawler.hitter_profile_url, "123")

        assert result is None
        page.goto.assert_not_awaited()

    async def test_scrape_profile_returns_parsed_payload(self):
        crawler = FuturesProfileCrawler(request_delay=0)
        crawler._wait = AsyncMock()
        crawler._extract_profile_text = AsyncMock(return_value="프로필")
        crawler._extract_futures_tables = AsyncMock(return_value=[{"headers": ["G"], "rows": [["10"]]}])
        page = AsyncMock()

        with patch("src.crawlers.futures.profile.compliance.is_allowed", new=AsyncMock(return_value=True)):
            result = await crawler._scrape_profile(page, crawler.hitter_profile_url, "123")

        assert result == {
            "url": f"{crawler.hitter_profile_url}?playerId=123",
            "profile_text": "프로필",
            "tables": [{"headers": ["G"], "rows": [["10"]]}],
        }
        page.goto.assert_awaited_once()

    async def test_fetch_player_futures_combines_hitter_and_pitcher_payloads(self):
        page = AsyncMock()
        pool = MagicMock()
        pool.start = AsyncMock()
        pool.acquire = AsyncMock(return_value=page)
        pool.release = AsyncMock()
        pool.close = AsyncMock()
        crawler = FuturesProfileCrawler(pool=pool)
        crawler._scrape_profile = AsyncMock(
            side_effect=[
                {"profile_text": "타자 프로필", "tables": [{"_table_type": "HITTER"}]},
                {"profile_text": "투수 프로필", "tables": [{"_table_type": "PITCHER"}]},
            ],
        )

        payload = await crawler.fetch_player_futures("123")

        assert payload == {
            "player_id": "123",
            "profile_text": "투수 프로필",
            "tables": [{"_table_type": "HITTER"}, {"_table_type": "PITCHER"}],
        }
        pool.start.assert_awaited_once()
        pool.release.assert_awaited_once_with(page)
        pool.close.assert_not_awaited()
