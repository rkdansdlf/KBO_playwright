from __future__ import annotations

from bs4 import BeautifulSoup

from src.crawlers.futures.futures_batting import (
    _compute_missing,
    _extract_table_headers,
    _norm_header,
    _parse_batting_row,
    _parse_table,
    _pick_futures_table,
)


class TestNormHeader:
    def test_korean_year(self):
        assert _norm_header("연도") == "season"
        assert _norm_header("년도") == "season"
        assert _norm_header("시즌") == "season"

    def test_english_year(self):
        assert _norm_header("year") == "season"

    def test_korean_stats(self):
        assert _norm_header("타율") == "AVG"
        assert _norm_header("장타율") == "SLG"
        assert _norm_header("출루율") == "OBP"
        assert _norm_header("경기") == "G"
        assert _norm_header("타수") == "AB"
        assert _norm_header("안타") == "H"
        assert _norm_header("홈런") == "HR"
        assert _norm_header("볼넷") == "BB"
        assert _norm_header("삼진") == "SO"
        assert _norm_header("도루") == "SB"
        assert _norm_header("타석") == "PA"
        assert _norm_header("희생번트") == "SH"
        assert _norm_header("희생플라이") == "SF"

    def test_english_stats(self):
        assert _norm_header("avg") == "AVG"
        assert _norm_header("slg") == "SLG"
        assert _norm_header("obp") == "OBP"
        assert _norm_header("hr") == "HR"

    def test_whitespace_stripped(self):
        assert _norm_header(" 타율 ") == "AVG"
        assert _norm_header("타 율") == "AVG"

    def test_unknown_header(self):
        assert _norm_header("UnknownHeader") == "UnknownHeader"

    def test_empty_string(self):
        assert _norm_header("") == ""


class TestComputeMissing:
    def test_slg_computed(self):
        row = {"H": 10, "2B": 3, "3B": 1, "HR": 2, "AB": 40}
        result = _compute_missing(row)
        _1B = 10 - 3 - 1 - 2
        tb = _1B + 2 * 3 + 3 * 1 + 4 * 2
        assert result["SLG"] == round(tb / 40, 3)

    def test_obp_computed(self):
        row = {"H": 10, "BB": 5, "HBP": 2, "AB": 40}
        result = _compute_missing(row)
        expected = round((10 + 5 + 2) / (40 + 5 + 2), 3)
        assert result["OBP"] == expected

    def test_slg_not_overwritten(self):
        row = {"H": 10, "2B": 3, "3B": 1, "HR": 2, "AB": 40, "SLG": 0.500}
        result = _compute_missing(row)
        assert result["SLG"] == 0.500

    def test_obp_not_overwritten(self):
        row = {"H": 10, "BB": 5, "HBP": 2, "AB": 40, "OBP": 0.350}
        result = _compute_missing(row)
        assert result["OBP"] == 0.350

    def test_plate_appearances_computed_from_components(self):
        row = {"AB": 40, "BB": 5, "HBP": 2, "SH": 1, "SF": 2}
        result = _compute_missing(row)
        assert result["PA"] == 50

    def test_plate_appearances_not_overwritten(self):
        row = {"PA": 49, "AB": 40, "BB": 5, "HBP": 2, "SH": 1, "SF": 2}
        result = _compute_missing(row)
        assert result["PA"] == 49

    def test_zero_ab_no_slg(self):
        row = {"H": 0, "2B": 0, "3B": 0, "HR": 0, "AB": 0}
        result = _compute_missing(row)
        assert "SLG" not in result

    def test_missing_values_no_slg(self):
        row = {"H": None, "2B": 3, "3B": 1, "HR": 2, "AB": 40}
        result = _compute_missing(row)
        assert "SLG" not in result

    def test_zero_obp_denom(self):
        row = {"H": 0, "BB": 0, "HBP": 0, "AB": 0}
        result = _compute_missing(row)
        assert "OBP" not in result

    def test_returns_same_dict(self):
        row = {"H": 10, "2B": 3, "3B": 1, "HR": 2, "AB": 40}
        result = _compute_missing(row)
        assert result is row


class TestExtractTableHeaders:
    def test_thead_th(self):
        html = "<table><thead><tr><th>연도</th><th>타율</th></tr></thead></table>"
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        headers = _extract_table_headers(table)
        assert headers == ["season", "AVG"]

    def test_thead_td(self):
        html = "<table><thead><tr><td>연도</td><td>타율</td></tr></thead></table>"
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        headers = _extract_table_headers(table)
        assert headers == ["season", "AVG"]

    def test_fallback_to_first_row(self):
        html = "<table><tr><th>연도</th><th>타율</th></tr><tr><td>2023</td><td>0.300</td></tr></table>"
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        headers = _extract_table_headers(table)
        assert headers == ["season", "AVG"]

    def test_empty_table(self):
        html = "<table></table>"
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        headers = _extract_table_headers(table)
        assert headers == []


class TestParseBattingRow:
    def test_basic_row(self):
        headers = ["season", "AVG", "G", "AB"]
        cells = ["2023", "0.300", "120", "450"]
        row = _parse_batting_row(headers, cells)
        assert row["season"] == 2023
        assert row["AVG"] == 0.3
        assert row["G"] == 120
        assert row["AB"] == 450

    def test_plate_appearance_components(self):
        headers = ["season", "PA", "AB", "BB", "HBP", "SH", "SF"]
        cells = ["2023", "20", "15", "3", "1", "0", "1"]
        row = _parse_batting_row(headers, cells)
        assert row["PA"] == 20
        assert row["SH"] == 0
        assert row["SF"] == 1

    def test_season_with_text(self):
        headers = ["season"]
        cells = ["2023시즌"]
        row = _parse_batting_row(headers, cells)
        assert row["season"] == 2023

    def test_no_season(self):
        headers = ["AVG", "G"]
        cells = ["0.300", "120"]
        row = _parse_batting_row(headers, cells)
        assert "season" not in row
        assert row["AVG"] == 0.3

    def test_empty_cells(self):
        headers = ["season", "AVG"]
        cells = ["", ""]
        row = _parse_batting_row(headers, cells)
        assert row["season"] is None
        assert row["AVG"] is None

    def test_float_values(self):
        headers = ["AVG", "SLG", "OBP"]
        cells = ["0.300", "0.500", "0.400"]
        row = _parse_batting_row(headers, cells)
        assert row["AVG"] == 0.3
        assert row["SLG"] == 0.5
        assert row["OBP"] == 0.4

    def test_extra_cells_ignored(self):
        headers = ["season", "AVG"]
        cells = ["2023", "0.300", "extra", "more"]
        row = _parse_batting_row(headers, cells)
        assert len(row) == 2

    def test_missing_cells(self):
        headers = ["season", "AVG", "G", "AB"]
        cells = ["2023", "0.300"]
        row = _parse_batting_row(headers, cells)
        assert row["season"] == 2023
        assert row["AVG"] == 0.3
        assert "G" not in row


class TestParseTable:
    def test_basic_table(self):
        html = """
        <table>
            <thead><tr><th>연도</th><th>AVG</th><th>G</th></tr></thead>
            <tbody>
                <tr><td>2023</td><td>0.300</td><td>120</td></tr>
                <tr><td>2024</td><td>0.280</td><td>110</td></tr>
            </tbody>
        </table>
        """
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        rows = _parse_table(table)
        assert len(rows) == 2
        assert rows[0]["season"] == 2023
        assert rows[0]["AVG"] == 0.3
        assert rows[1]["season"] == 2024

    def test_skips_total_row(self):
        html = """
        <table>
            <thead><tr><th>연도</th><th>AVG</th></tr></thead>
            <tbody>
                <tr><td>2023</td><td>0.300</td></tr>
                <tr><td>통산</td><td>0.290</td></tr>
            </tbody>
        </table>
        """
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        rows = _parse_table(table)
        assert len(rows) == 1
        assert rows[0]["season"] == 2023

    def test_skips_empty_rows(self):
        html = """
        <table>
            <thead><tr><th>연도</th><th>AVG</th></tr></thead>
            <tbody>
                <tr><td></td><td></td></tr>
                <tr><td>2023</td><td>0.300</td></tr>
            </tbody>
        </table>
        """
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        rows = _parse_table(table)
        assert len(rows) == 1

    def test_no_thead(self):
        html = """
        <table>
            <tbody>
                <tr><th>연도</th><th>AVG</th></tr>
                <tr><td>2023</td><td>0.300</td></tr>
            </tbody>
        </table>
        """
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        rows = _parse_table(table)
        assert len(rows) == 1


class TestPickFuturesTable:
    def test_find_by_label(self):
        html = """
        <html><body>
            <h3>퓨처스</h3>
            <table><thead><tr><th>연도</th></tr></thead></table>
        </body></html>
        """
        soup = BeautifulSoup(html, "lxml")
        table = _pick_futures_table(soup)
        assert table is not None

    def test_find_by_headers(self):
        html = """
        <html><body>
            <table>
                <thead><tr><th>season</th><th>AVG</th><th>OBP</th><th>SLG</th></tr></thead>
            </table>
        </body></html>
        """
        soup = BeautifulSoup(html, "lxml")
        table = _pick_futures_table(soup)
        assert table is not None

    def test_no_table(self):
        html = "<html><body><p>No stats</p></body></html>"
        soup = BeautifulSoup(html, "lxml")
        table = _pick_futures_table(soup)
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
        table = _pick_futures_table(soup)
        assert table is None
