from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.crawlers.seat_crawler import TEAM_SEAT_SOURCES, SeatCrawler


class TestParseSeatPage:
    def setup_method(self):
        self.crawler = SeatCrawler()

    def test_parses_seat_sections(self):
        html = "<html><body>블루석 레드존 골드석</body></html>"
        result = self.crawler._parse_seat_page(html, "LG", {"stadium_id": "JAMSIL"})
        assert len(result) >= 1
        assert all(s["stadium_id"] == "JAMSIL" for s in result)

    def test_deduplicates_sections(self):
        html = "<html><body>블루석 블루석 블루석</body></html>"
        result = self.crawler._parse_seat_page(html, "LG", {"stadium_id": "JAMSIL"})
        blues = [s for s in result if s["section_name"] == "블루석"]
        assert len(blues) == 1

    def test_empty_html_returns_empty_list(self):
        result = self.crawler._parse_seat_page("", "LG", {"stadium_id": "JAMSIL"})
        assert result == []


class TestSeatCrawlerOperations:
    @pytest.mark.asyncio
    async def test_crawl_team_fetches_and_tracks_raw_page(self):
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        client.get = AsyncMock(return_value=MagicMock(status_code=200, text="<p>블루석</p>"))
        crawler = SeatCrawler()
        info = TEAM_SEAT_SOURCES["LG"]

        with (
            patch("src.crawlers.seat_crawler.httpx.AsyncClient", return_value=client),
            patch("src.crawlers.seat_crawler.throttle.wait", new=AsyncMock()) as wait,
        ):
            sections = await crawler._crawl_team_seats("LG", info)

        wait.assert_awaited_once_with("www.lgtwins.com")
        client.get.assert_awaited_once_with(info["url"])
        assert sections[0]["stadium_id"] == "JAMSIL"
        assert crawler._raw_pages[0]["source_key"] == "lg_twins_seat"

    @pytest.mark.asyncio
    async def test_run_continues_after_team_failure_and_respects_filter(self):
        crawler = SeatCrawler()
        crawler._crawl_team_seats = AsyncMock(side_effect=RuntimeError("LG unavailable"))

        records = await crawler.run(team_filter="LG")

        assert records == []
        crawler._crawl_team_seats.assert_awaited_once_with("LG", TEAM_SEAT_SOURCES["LG"])

    def test_save_to_db_persists_sections_and_clears_raw_pages(self):
        session = MagicMock()
        repo = MagicMock()
        crawler = SeatCrawler()
        crawler._raw_pages = [{"source_key": "lg_twins_seat"}]
        section = {"section_name": "블루석"}

        with (
            patch("src.crawlers.seat_crawler.SessionLocal") as session_local,
            patch("src.crawlers.seat_crawler.save_raw_snapshots", return_value=1),
            patch("src.crawlers.seat_crawler.StadiumSeatSectionRepository", return_value=repo),
        ):
            session_local.return_value.__enter__.return_value = session
            crawler._save_to_db([section])

        repo.save.assert_called_once_with(section)
        session.commit.assert_called_once()
        assert crawler._raw_pages == []
