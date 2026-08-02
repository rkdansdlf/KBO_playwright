"""Unit tests for LegacyGameDetailCrawler."""

from unittest.mock import MagicMock

from src.crawlers.legacy_game_detail_crawler import LegacyGameDetailCrawler


class TestLegacyGameDetailCrawler:
    def test_init_default(self):
        crawler = LegacyGameDetailCrawler()
        assert crawler.resolver is None

    def test_extract_from_html(self):
        html = """
        <html>
        <body>
        <table class="tbl-type06">
            <thead>
                <tr><th>팀</th><th>1</th><th>2</th><th>R</th><th>H</th><th>E</th></tr>
            </thead>
            <tbody>
                <tr><td>한화</td><td>1</td><td>0</td><td>1</td><td>5</td><td>0</td></tr>
                <tr><td>SK</td><td>0</td><td>2</td><td>2</td><td>6</td><td>1</td></tr>
            </tbody>
        </table>
        <table>
            <thead>
                <tr><th>선수</th><th>타순</th><th>POS</th><th>타수</th><th>안타</th><th>타점</th></tr>
            </thead>
            <tbody>
                <tr><td>김태균</td><td>4</td><td>1B</td><td>4</td><td>2</td><td>1</td></tr>
            </tbody>
        </table>
        <table>
            <thead>
                <tr><th>선수</th><th>이닝</th><th>피안타</th><th>자책</th><th>삼진</th></tr>
            </thead>
            <tbody>
                <tr><td>류현진</td><td>7</td><td>4</td><td>1</td><td>9</td></tr>
            </tbody>
        </table>
        </body>
        </html>
        """
        crawler = LegacyGameDetailCrawler()
        res = crawler.extract_from_html(html, "20090404HHSK0", "20090404")
        assert res["game_id"] == "20090404HHSK0"
        assert res["teams"]["away"]["name"] == "한화"
        assert res["teams"]["home"]["name"] == "SK"
        assert len(res["hitters"]["away"]) >= 1
        assert res["hitters"]["away"][0]["player_name"] == "김태균"

    def test_extract_game_details_page_mock(self):
        page_mock = MagicMock()
        page_mock.content.return_value = "<html><body><table><thead><tr><th>팀</th><th>R</th><th>H</th><th>E</th></tr></thead><tbody><tr><td>한화</td><td>1</td><td>5</td><td>0</td></tr></tbody></table></body></html>"
        crawler = LegacyGameDetailCrawler()
        res = crawler.extract_game_details(page_mock, "20090404HHSK0", "20090404")
        assert res["game_id"] == "20090404HHSK0"
