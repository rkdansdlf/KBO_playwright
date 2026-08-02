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

    def test_extract_from_html_legacy_2001_format(self):
        html = """
        <html>
        <body>
        <table>
            <thead><tr><th>Unnamed: 0</th><th>TEAM</th></tr></thead>
            <tbody><tr><td>1</td><td>LG</td></tr></tbody>
        </table>
        <table>
            <thead><tr><th>1</th><th>2</th><th>3</th><th>4</th><th>5</th><th>6</th><th>7</th><th>8</th><th>9</th><th>10</th><th>11</th><th>12</th></tr></thead>
            <tbody><tr><td>0</td><td>1</td><td>0</td><td>0</td><td>2</td><td>1</td><td>0</td><td>1</td><td>1</td><td>-</td><td>-</td><td>-</td></tr>
                <tr><td>2</td><td>0</td><td>3</td><td>0</td><td>1</td><td>2</td><td>3</td><td>0</td><td>-</td><td>-</td><td>-</td><td>-</td></tr></tbody>
        </table>
        <table>
            <thead><tr><th>R</th><th>H</th><th>E</th><th>B</th></tr></thead>
            <tbody><tr><td>6</td><td>13</td><td>0</td><td>8</td></tr>
                <tr><td>11</td><td>16</td><td>1</td><td>3</td></tr></tbody>
        </table>
        <table>
            <thead><tr><th>Unnamed: 0</th><th>Unnamed: 1</th><th>선수명</th></tr></thead>
            <tbody>
                <tr><td>1</td><td>중</td><td>이병규</td></tr>
                <tr><td>2</td><td>2루</td><td>류지현</td></tr>
            </tbody>
        </table>
        <table>
            <thead><tr><th>1</th><th>2</th><th>3</th><th>4</th><th>5</th><th>6</th><th>7</th><th>8</th><th>9</th></tr></thead>
            <tbody><tr><td>안</td><td>-</td><td>안</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td></tr>
                <tr><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td></tr></tbody>
        </table>
        <table>
            <thead><tr><th>타수</th><th>안타</th><th>타점</th><th>득점</th><th>타율</th></tr></thead>
            <tbody>
                <tr><td>5</td><td>3</td><td>0</td><td>1</td><td>0.6</td></tr>
                <tr><td>4</td><td>1</td><td>0</td><td>1</td><td>0.25</td></tr>
            </tbody>
        </table>
        <table>
            <thead><tr><th>선수명</th><th>등판</th><th>결과</th><th>승</th><th>패</th><th>세</th><th>이닝</th><th>타자</th><th>투구수</th><th>타수</th><th>피안타</th><th>홈런</th><th>4사구</th><th>삼진</th><th>실점</th><th>자책</th><th>평균자책점</th></tr></thead>
            <tbody>
                <tr><td>해리거</td><td>2</td><td>-</td><td>-</td><td>-</td><td>-</td><td>4</td><td>17</td><td>58</td><td>14</td><td>5</td><td>-</td><td>-</td><td>2</td><td>2</td><td>2</td><td>4.5</td></tr>
                <tr><td>TOTAL</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>8</td><td>43</td><td>164</td><td>36</td><td>16</td><td>-</td><td>-</td><td>5</td><td>11</td><td>11</td><td>-</td></tr>
            </tbody>
        </table>
        </body>
        </html>
        """
        crawler = LegacyGameDetailCrawler()
        res = crawler.extract_from_html(html, "20010405LGSK0", "20010405")
        assert res["game_id"] == "20010405LGSK0"
        assert res["teams"]["away"]["name"] is None
        assert res["teams"]["away"]["code"] == "LG"
        assert res["teams"]["away"]["score"] == 6
        assert res["teams"]["away"]["hits"] == 13
        assert res["teams"]["away"]["errors"] == 0
        assert res["teams"]["home"]["score"] == 11
        assert res["teams"]["home"]["hits"] == 16
        assert res["teams"]["home"]["errors"] == 1
        assert res["teams"]["away"]["line_score"][:2] == [0, 1]
        assert res["teams"]["home"]["line_score"][:2] == [2, 0]
        assert [x["player_name"] for x in res["hitters"]["away"]] == ["이병규", "류지현"]
        assert res["hitters"]["away"][0]["batting_order"] == 1
        assert res["hitters"]["away"][0]["position"] == "중"
        assert res["hitters"]["away"][0]["stats"]["at_bats"] == 5
        assert res["hitters"]["away"][0]["stats"]["hits"] == 3
        assert res["hitters"]["away"][0]["stats"]["runs"] == 1
        assert res["hitters"]["away"][0]["stats"]["rbi"] == 0
        assert [x["player_name"] for x in res["pitchers"]["away"]] == ["해리거"]
        assert res["pitchers"]["away"][0]["stats"]["innings_outs"] == 12
        assert res["pitchers"]["away"][0]["stats"]["pitches"] == 58
