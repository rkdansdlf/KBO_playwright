from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from bs4 import BeautifulSoup
import pytest

from src.crawlers.futures.futures_pitching import (
    _norm_header,
    _parse_pitching_cell_row,
    _parse_table,
    _pick_futures_pitching_table,
    fetch_and_parse_futures_pitching,
)


class TestNormHeader:
    def test_korean_year(self):
        assert _norm_header("연도") == "season"
        assert _norm_header("년도") == "season"
        assert _norm_header("시즌") == "season"

    def test_korean_pitching_stats(self):
        assert _norm_header("평균자책") == "era"
        assert _norm_header("평균자책점") == "era"
        assert _norm_header("경기") == "games"
        assert _norm_header("완투") == "complete_games"
        assert _norm_header("완봉") == "shutouts"
        assert _norm_header("승") == "wins"
        assert _norm_header("패") == "losses"
        assert _norm_header("세이브") == "saves"
        assert _norm_header("홀드") == "holds"
        assert _norm_header("삼진") == "strikeouts"
        assert _norm_header("볼넷") == "walks_allowed"
        assert _norm_header("피안타") == "hits_allowed"
        assert _norm_header("피홈런") == "home_runs_allowed"

    def test_english_pitching_stats(self):
        assert _norm_header("era") == "era"
        assert _norm_header("w") == "wins"
        assert _norm_header("l") == "losses"
        assert _norm_header("sv") == "saves"
        assert _norm_header("so") == "strikeouts"
        assert _norm_header("bb") == "walks_allowed"
        assert _norm_header("ip") == "IP"

    def test_team_name(self):
        assert _norm_header("팀명") == "team_name"
        assert _norm_header("팀") == "team_name"

    def test_innings(self):
        assert _norm_header("이닝") == "IP"

    def test_unknown(self):
        assert _norm_header("xyz") == "xyz"

    def test_whitespace(self):
        assert _norm_header(" 승 ") == "wins"


class TestParsePitchingCellRow:
    def test_basic_row(self):
        headers = ["season", "era", "games", "wins", "losses"]
        cells = ["2023", "3.50", "25", "10", "5"]
        row = _parse_pitching_cell_row(headers, cells)
        assert row["season"] == 2023
        assert row["era"] == 3.5
        assert row["games"] == 25
        assert row["wins"] == 10
        assert row["losses"] == 5

    def test_season_with_text(self):
        headers = ["season"]
        cells = ["2023시즌"]
        row = _parse_pitching_cell_row(headers, cells)
        assert row["season"] == 2023

    def test_ip_preserved(self):
        headers = ["IP"]
        cells = ["5.1"]
        row = _parse_pitching_cell_row(headers, cells)
        assert row["IP"] == "5.1"

    def test_team_name(self):
        headers = ["team_name"]
        cells = ["LG"]
        row = _parse_pitching_cell_row(headers, cells)
        assert row["team_name"] == "LG"

    def test_float_era(self):
        headers = ["era"]
        cells = ["2.75"]
        row = _parse_pitching_cell_row(headers, cells)
        assert row["era"] == 2.75

    def test_empty_values(self):
        headers = ["season", "era", "games"]
        cells = ["", "", ""]
        row = _parse_pitching_cell_row(headers, cells)
        assert row["season"] is None
        assert row["era"] is None
        assert row["games"] is None

    def test_extra_cells(self):
        headers = ["season", "era"]
        cells = ["2023", "3.50", "extra"]
        row = _parse_pitching_cell_row(headers, cells)
        assert len(row) == 2


class TestParseTable:
    def test_basic_table(self):
        html = """
        <table>
            <thead><tr><th>season</th><th>era</th><th>wins</th></tr></thead>
            <tbody>
                <tr><td>2023</td><td>3.50</td><td>10</td></tr>
                <tr><td>2024</td><td>2.80</td><td>12</td></tr>
            </tbody>
        </table>
        """
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        rows = _parse_table(table)
        assert len(rows) == 2
        assert rows[0]["season"] == 2023
        assert rows[0]["era"] == 3.5
        assert rows[1]["season"] == 2024

    def test_skips_total_row(self):
        html = """
        <table>
            <thead><tr><th>season</th><th>era</th></tr></thead>
            <tbody>
                <tr><td>2023</td><td>3.50</td></tr>
                <tr><td>통산</td><td>3.20</td></tr>
            </tbody>
        </table>
        """
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        rows = _parse_table(table)
        assert len(rows) == 1

    def test_no_season_skipped(self):
        html = """
        <table>
            <thead><tr><th>season</th><th>era</th></tr></thead>
            <tbody>
                <tr><td></td><td>3.50</td></tr>
                <tr><td>2023</td><td>2.80</td></tr>
            </tbody>
        </table>
        """
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        rows = _parse_table(table)
        assert len(rows) == 1

    def test_innings_outs_computed(self):
        html = """
        <table>
            <thead><tr><th>season</th><th>IP</th></tr></thead>
            <tbody>
                <tr><td>2023</td><td>5.1</td></tr>
            </tbody>
        </table>
        """
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        rows = _parse_table(table)
        assert len(rows) == 1
        assert "innings_outs" in rows[0]


class TestPickFuturesPitchingTable:
    def test_find_by_id(self):
        html = """
        <html><body>
            <table id="tblPitcherRecord">
                <thead><tr><th>season</th></tr></thead>
            </table>
        </body></html>
        """
        soup = BeautifulSoup(html, "lxml")
        table = _pick_futures_pitching_table(soup)
        assert table is not None

    def test_find_by_headers(self):
        html = """
        <html><body>
            <table>
                <thead><tr><th>season</th><th>era</th><th>games</th><th>wins</th><th>losses</th></tr></thead>
            </table>
        </body></html>
        """
        soup = BeautifulSoup(html, "lxml")
        table = _pick_futures_pitching_table(soup)
        assert table is not None

    def test_no_table(self):
        html = "<html><body><p>No stats</p></body></html>"
        soup = BeautifulSoup(html, "lxml")
        table = _pick_futures_pitching_table(soup)
        assert table is None

    def test_wrong_headers(self):
        html = """
        <html><body>
            <table>
                <thead><tr><th>name</th><th>value</th></tr></thead>
            </table>
        </body></html>
        """
        soup = BeautifulSoup(html, "lxml")
        table = _pick_futures_pitching_table(soup)
        assert table is None


@pytest.mark.asyncio
class TestFetchAndParseFuturesPitching:
    async def test_returns_empty_without_fetching_when_compliance_blocks(self):
        page = AsyncMock()
        pool = MagicMock()
        pool.start = AsyncMock()
        pool.acquire = AsyncMock(return_value=page)
        pool.release = AsyncMock()
        pool.close = AsyncMock()

        with patch("src.crawlers.futures.futures_pitching.compliance.is_allowed", new=AsyncMock(return_value=False)):
            records = await fetch_and_parse_futures_pitching("123", "https://example.test/profile", pool)

        assert records == []
        page.goto.assert_not_awaited()
        pool.release.assert_awaited_once_with(page)
        pool.close.assert_not_awaited()

    async def test_fetches_profile_and_trims_pitching_rows(self):
        page = AsyncMock()
        tab = MagicMock()
        tab.click = AsyncMock()
        page.wait_for_selector = AsyncMock(return_value=tab)
        page.content.return_value = """
            <table id="tblPitcherRecord">
              <thead><tr><th>연도</th><th>팀명</th><th>평균자책</th><th>경기</th><th>승</th><th>패</th><th>이닝</th></tr></thead>
              <tbody><tr><td>2025</td><td>LG</td><td>2.50</td><td>10</td><td>5</td><td>2</td><td>30.1</td></tr></tbody>
            </table>
        """
        pool = MagicMock()
        pool.start = AsyncMock()
        pool.acquire = AsyncMock(return_value=page)
        pool.release = AsyncMock()
        pool.close = AsyncMock()

        with (
            patch("src.crawlers.futures.futures_pitching.compliance.is_allowed", new=AsyncMock(return_value=True)),
            patch("src.crawlers.futures.futures_pitching.throttle.wait", new=AsyncMock()),
        ):
            records = await fetch_and_parse_futures_pitching("123", "https://example.test/profile", pool)

        assert records == [
            {
                "season": 2025,
                "era": 2.5,
                "games": 10,
                "complete_games": None,
                "shutouts": None,
                "wins": 5,
                "losses": 2,
                "saves": None,
                "holds": None,
                "tbf": None,
                "innings_pitched": None,
                "innings_outs": 91,
                "hits_allowed": None,
                "home_runs_allowed": None,
                "walks_allowed": None,
                "hit_batters": None,
                "strikeouts": None,
                "runs_allowed": None,
                "earned_runs": None,
                "team_code": "LG",
            },
        ]
        tab.click.assert_awaited_once()
        pool.release.assert_awaited_once_with(page)
