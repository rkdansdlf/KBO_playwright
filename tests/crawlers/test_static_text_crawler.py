from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest import mark

from src.crawlers.static_text_crawler import StaticTextCrawler


@pytest.fixture
def crawler():
    return StaticTextCrawler()


class TestParseLocalPdf:
    def test_raises_on_missing_file(self, crawler):
        with pytest.raises(FileNotFoundError):
            crawler.parse_local_pdf("/nonexistent/path.pdf")

    @patch("pathlib.Path.exists", return_value=True)
    @patch("src.crawlers.static_text_crawler.PdfReader")
    def test_returns_chunks_per_page(self, mock_reader_cls, mock_exists, crawler):
        mock_reader = MagicMock()
        mock_page1 = MagicMock()
        mock_page1.extract_text.return_value = "Page 1 content"
        mock_page2 = MagicMock()
        mock_page2.extract_text.return_value = "Page 2 content"
        mock_reader.pages = [mock_page1, mock_page2]
        mock_reader_cls.return_value = mock_reader

        result = crawler.parse_local_pdf("/fake/rules.pdf")

        assert len(result) == 2
        assert result[0]["title"] == "KBO 공식 야구 규칙서 - Page 1"
        assert result[0]["content"] == "Page 1 content"
        assert result[1]["title"] == "KBO 공식 야구 규칙서 - Page 2"

    @patch("pathlib.Path.exists", return_value=True)
    @patch("src.crawlers.static_text_crawler.PdfReader")
    def test_skips_empty_pages(self, mock_reader_cls, mock_exists, crawler):
        mock_reader = MagicMock()
        mock_page1 = MagicMock()
        mock_page1.extract_text.return_value = "   "
        mock_page2 = MagicMock()
        mock_page2.extract_text.return_value = "Real content"
        mock_reader.pages = [mock_page1, mock_page2]
        mock_reader_cls.return_value = mock_reader

        result = crawler.parse_local_pdf("/fake/rules.pdf")

        assert len(result) == 1
        assert result[0]["content"] == "Real content"


class TestCrawlNamuwiki:
    @mark.asyncio
    @patch("src.crawlers.static_text_crawler.AsyncPlaywrightPool")
    async def test_fetches_and_parses_content(self, mock_pool_cls, crawler):
        mock_pool = AsyncMock()
        mock_pool_cls.return_value = mock_pool
        mock_page = AsyncMock()
        mock_pool.acquire = AsyncMock(return_value=mock_page)
        mock_page.goto = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.content = AsyncMock(
            return_value="""
            <html>
            <body>
                <div class="wiki-title">KBO Rules [편집]</div>
                <div class="wiki-content">
                    <p>This is the main content.</p>
                    <div class="wiki-edit-section">edit</div>
                    <div class="wiki-fn-content">footnote</div>
                </div>
                <script>bad</script>
            </body>
            </html>
        """,
        )

        result = await crawler.crawl_namuwiki("https://namu.wiki/w/KBO")

        assert result["title"] == "KBO Rules"
        assert "main content" in result["content"]
        assert "edit" not in result["content"]
        assert "footnote" not in result["content"]

    @mark.asyncio
    @patch("src.crawlers.static_text_crawler.AsyncPlaywrightPool")
    async def test_falls_back_to_body_when_no_wiki_content(self, mock_pool_cls, crawler):
        mock_pool = AsyncMock()
        mock_pool_cls.return_value = mock_pool
        mock_page = AsyncMock()
        mock_pool.acquire = AsyncMock(return_value=mock_page)
        mock_page.goto = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.content = AsyncMock(
            return_value="""
            <html><body>Plain body text</body></html>
        """,
        )

        result = await crawler.crawl_namuwiki("https://namu.wiki/w/Test")

        assert result["content"] == "Plain body text"

    @mark.asyncio
    @patch("src.crawlers.static_text_crawler.AsyncPlaywrightPool")
    async def test_raises_on_empty_html(self, mock_pool_cls, crawler):
        mock_pool = AsyncMock()
        mock_pool_cls.return_value = mock_pool
        mock_page = AsyncMock()
        mock_pool.acquire = AsyncMock(return_value=mock_page)
        mock_page.goto = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.content = AsyncMock(return_value="")

        with pytest.raises(ValueError, match="Failed to fetch content"):
            await crawler.crawl_namuwiki("https://namu.wiki/w/Empty")
