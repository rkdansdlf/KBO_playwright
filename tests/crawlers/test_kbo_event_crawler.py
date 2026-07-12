from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.crawlers.kbo_event_crawler import (
    KboEventCrawler,
    KBO_EVENT_DEFAULT_URLS,
    extract_kbo_event_links,
    extract_kbo_event_page,
    _extract_page_title,
    _build_event_payload,
)


class TestKboEventCrawlerFunctions:
    def test_extract_kbo_event_links_matches_keywords(self):
        html = """
        <html>
            <body>
                <a href="/Kbo/Event/Promotion1.aspx">이벤트 1</a>
                <a href="/Kbo/Event/Promotion2.aspx">event 2</a>
                <a href="/Kbo/Event/Page.aspx">일반 페이지</a>
                <a href="/Kbo/Event/Promotion1.aspx">이벤트 1 중복</a>
                <a href="#anchor">이벤트 링크 닻</a>
                <a href="javascript:void(0)">javascript 이벤트</a>
                <a href="/Kbo/Event/Apply.aspx">신청하기</a>
                <a href="/other/page">전혀 무관한 링크</a>
            </body>
        </html>
        """
        events = extract_kbo_event_links(html)
        assert len(events) == 3
        assert events[0]["title"] == "이벤트 1"
        assert events[0]["source_url"] == "https://www.koreabaseball.com/Kbo/Event/Promotion1.aspx"
        assert events[1]["title"] == "event 2"

    def test_extract_kbo_event_page_valid(self):
        html = """
        <html>
            <head>
                <title>KBO MVP 시상식 | KBO | 주요 사업/행사</title>
            </head>
        </html>
        """
        payload = extract_kbo_event_page(html, "https://example.com/mvp")
        assert payload is not None
        assert payload["title"] == "KBO MVP 시상식"
        assert payload["source_url"] == "https://example.com/mvp"

    def test_extract_kbo_event_page_invalid_title(self):
        html = "<html><head><title>메인 | KBO</title></head></html>"
        payload = extract_kbo_event_page(html, "https://example.com")
        assert payload is None

        html_no_title = "<html><head></head></html>"
        payload_no_title = extract_kbo_event_page(html_no_title, "https://example.com")
        assert payload_no_title is None

    def test_extract_kbo_event_links_filters_event_candidates(self):
        html = """
        <html><body>
          <a href="/Event/Detail.aspx?id=1">팬 이벤트 안내</a>
          <a href="/Schedule/Schedule.aspx">경기 일정</a>
          <a href="https://example.com/promotion">프로모션 안내</a>
        </body></html>
        """
        result = extract_kbo_event_links(html, "https://www.koreabaseball.com")
        assert [item["title"] for item in result] == ["팬 이벤트 안내", "프로모션 안내"]
        assert result[0]["source_url"] == "https://www.koreabaseball.com/Event/Detail.aspx?id=1"
        assert result[1]["source_url"] == "https://example.com/promotion"

    def test_extract_kbo_event_page_uses_business_event_title(self):
        html = """
        <html><head><title>신청하기 | 미디어데이&팬페스트 입장권 | 주요 사업/행사 | KBO</title></head></html>
        """
        result = extract_kbo_event_page(html, "https://www.koreabaseball.com/Kbo/BusinessAndEvent/MediaDay.aspx")
        assert result is not None
        assert result["title"] == "미디어데이&팬페스트 입장권"
        assert result["source_url"] == "https://www.koreabaseball.com/Kbo/BusinessAndEvent/MediaDay.aspx"

    def test_kbo_event_crawler_defaults_to_business_event_urls(self):
        crawler = KboEventCrawler()
        assert crawler.urls == KBO_EVENT_DEFAULT_URLS


class TestKboEventCrawlerRun:
    @patch("src.crawlers.kbo_event_crawler.KboEventCrawler._fetch_html", new_callable=AsyncMock)
    @pytest.mark.asyncio
    async def test_run_without_save(self, mock_fetch):
        mock_fetch.return_value = (
            """
            <html>
                <head><title>KBO 공식 행사</title></head>
                <body>
                    <a href="/Kbo/Event/Promotion.aspx">공식 이벤트</a>
                </body>
            </html>
            """,
            "https://www.koreabaseball.com/Kbo/Event/Main.aspx",
        )

        crawler = KboEventCrawler()
        crawler.urls = (
            "https://www.koreabaseball.com/Kbo/Event/Main.aspx",
            "https://www.koreabaseball.com/Kbo/Event/Main.aspx",
        )
        results = await crawler.run(save=False)

        assert len(results) == 2  # page event + event link
        assert results[0]["title"] == "KBO 공식 행사"
        assert results[1]["title"] == "공식 이벤트"

    @patch("src.crawlers.kbo_event_crawler.KboEventCrawler._fetch_html", new_callable=AsyncMock)
    @patch("src.crawlers.kbo_event_crawler.SessionLocal")
    @patch("src.crawlers.kbo_event_crawler.save_raw_snapshots")
    @patch("src.crawlers.kbo_event_crawler.DataSourceRepository")
    @patch("src.crawlers.kbo_event_crawler.TeamEventRepository")
    @pytest.mark.asyncio
    async def test_run_with_save_success(
        self,
        mock_team_event_repo_cls,
        mock_data_source_repo_cls,
        mock_save_snapshots,
        mock_session_local,
        mock_fetch,
    ):
        mock_fetch.return_value = (
            "<html><head><title>KBO 공식 행사</title></head></html>",
            "https://www.koreabaseball.com/Kbo/Event/Main.aspx",
        )

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__.return_value = mock_session

        mock_source = MagicMock()
        mock_source.id = 123
        mock_data_source_repo_cls.return_value.get_by_key.return_value = mock_source

        mock_team_event_repo = mock_team_event_repo_cls.return_value

        crawler = KboEventCrawler(base_url="https://www.koreabaseball.com/Kbo/Event/Main.aspx")
        await crawler.run(save=True)

        mock_save_snapshots.assert_called_once()
        mock_team_event_repo.save.assert_called_once()
        mock_session.commit.assert_called_once()

    @patch("src.crawlers.kbo_event_crawler.KboEventCrawler._fetch_html", new_callable=AsyncMock)
    @patch("src.crawlers.kbo_event_crawler.SessionLocal")
    @patch("src.crawlers.kbo_event_crawler.save_raw_snapshots")
    @pytest.mark.asyncio
    async def test_run_with_save_db_error_rolls_back(
        self,
        mock_save_snapshots,
        mock_session_local,
        mock_fetch,
    ):
        mock_fetch.return_value = (
            "<html><head><title>KBO 공식 행사</title></head></html>",
            "https://www.koreabaseball.com/Kbo/Event/Main.aspx",
        )

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__.return_value = mock_session
        mock_save_snapshots.side_effect = SQLAlchemyError("DB failure")

        crawler = KboEventCrawler(base_url="https://www.koreabaseball.com/Kbo/Event/Main.aspx")
        with pytest.raises(SQLAlchemyError):
            await crawler.run(save=True)

        mock_session.rollback.assert_called_once()

    @patch("src.crawlers.kbo_event_crawler.KboEventCrawler._fetch_html", new_callable=AsyncMock)
    @pytest.mark.asyncio
    async def test_run_with_no_page_event_and_duplicate_urls(self, mock_fetch):
        mock_fetch.return_value = (
            """
            <html>
                <head><title>KBO 공식 행사 | KBO | 주요 사업/행사</title></head>
                <body>
                    <a href="/Kbo/Event/Main.aspx">이벤트 링크 동일</a>
                </body>
            </html>
            """,
            "https://www.koreabaseball.com/Kbo/Event/Main.aspx",
        )

        crawler = KboEventCrawler(base_url="https://www.koreabaseball.com/Kbo/Event/Main.aspx")
        results = await crawler.run(save=False)

        assert len(results) == 1
        assert results[0]["title"] == "KBO 공식 행사"

    @patch("src.crawlers.kbo_event_crawler.KboEventCrawler._fetch_html", new_callable=AsyncMock)
    @patch("src.crawlers.kbo_event_crawler.SessionLocal")
    @patch("src.crawlers.kbo_event_crawler.save_raw_snapshots")
    @patch("src.crawlers.kbo_event_crawler.DataSourceRepository")
    @patch("src.crawlers.kbo_event_crawler.TeamEventRepository")
    @pytest.mark.asyncio
    async def test_run_with_save_no_datasource(
        self,
        mock_team_event_repo_cls,
        mock_data_source_repo_cls,
        mock_save_snapshots,
        mock_session_local,
        mock_fetch,
    ):
        mock_fetch.return_value = (
            "<html><head><title>KBO 공식 행사</title></head></html>",
            "https://www.koreabaseball.com/Kbo/Event/Main.aspx",
        )

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__.return_value = mock_session

        mock_data_source_repo_cls.return_value.get_by_key.return_value = None
        mock_team_event_repo = mock_team_event_repo_cls.return_value

        crawler = KboEventCrawler(base_url="https://www.koreabaseball.com/Kbo/Event/Main.aspx")
        await crawler.run(save=True)

        mock_team_event_repo.save.assert_called_once()
        saved_payload = mock_team_event_repo.save.call_args[0][0]
        assert "source_id" not in saved_payload
        mock_session.commit.assert_called_once()


class TestKboEventCrawlerFetchHtml:
    @patch("src.crawlers.kbo_event_crawler.AsyncPlaywrightPool")
    @pytest.mark.asyncio
    async def test_fetch_html_success(self, mock_pool_cls):
        mock_page = AsyncMock()
        mock_page.content.return_value = "<html>Content</html>"
        mock_page.url = "https://example.com/resolved"

        mock_pool = mock_pool_cls.return_value
        mock_pool.start = AsyncMock()
        mock_pool.acquire = AsyncMock(return_value=mock_page)
        mock_pool.release = AsyncMock()
        mock_pool.close = AsyncMock()

        crawler = KboEventCrawler()
        content, url = await crawler._fetch_html("https://example.com/start")

        assert content == "<html>Content</html>"
        assert url == "https://example.com/resolved"
        mock_page.goto.assert_called_once_with(
            "https://example.com/start", wait_until="domcontentloaded", timeout=30000
        )
        mock_pool.release.assert_called_once_with(mock_page)
        mock_pool.close.assert_called_once()

    @patch("src.crawlers.kbo_event_crawler.AsyncPlaywrightPool")
    @pytest.mark.asyncio
    async def test_fetch_html_failure_closes_pool(self, mock_pool_cls):
        mock_page = AsyncMock()
        mock_page.goto.side_effect = RuntimeError("Navigation failed")

        mock_pool = mock_pool_cls.return_value
        mock_pool.start = AsyncMock()
        mock_pool.acquire = AsyncMock(return_value=mock_page)
        mock_pool.release = AsyncMock()
        mock_pool.close = AsyncMock()

        crawler = KboEventCrawler()
        with pytest.raises(RuntimeError):
            await crawler._fetch_html("https://example.com/start")

        mock_pool.release.assert_called_once_with(mock_page)
        mock_pool.close.assert_called_once()
