from unittest.mock import MagicMock, patch

import pytest

from src.crawlers.team_batting_stats_crawler import (
    BATTING_FIELDS,
    FLOAT_FIELDS,
    HEADER_MAP,
    _add_batting_values,
    _build_column_map,
    _extract_stat_rows,
    _parse_team_batting_row,
    parse_team_batting_html,
)


class TestBuildColumnMap:
    def test_korean_headers(self):
        headers = ["팀명", "경기", "승", "패", "타율"]
        result = _build_column_map(headers)
        assert result["team_name"] == 0
        assert result["games"] == 1

    def test_english_headers(self):
        headers = ["팀", "g", "w", "l", "avg"]
        result = _build_column_map(headers)
        assert result["team_name"] == 0
        assert "games" in result

    def test_empty_headers_fallback(self):
        result = _build_column_map([])
        assert result["team_name"] == 0

    def test_fallback_team_name_position(self):
        headers = ["순위", "unknown1", "unknown2"]
        result = _build_column_map(headers)
        assert result["team_name"] == 1


SAMPLE_BATTING_HTML = """
<html><body>
<table class="tData01">
  <thead>
    <tr>
      <th>팀명</th><th>경기</th><th>타석</th><th>타수</th><th>득점</th><th>안타</th>
      <th>2루타</th><th>3루타</th><th>홈런</th><th>타점</th><th>도루</th><th>도실</th>
      <th>볼넷</th><th>삼진</th><th>타율</th><th>출루율</th><th>장타율</th><th>OPS</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>LG</td><td>144</td><td>6000</td><td>5200</td><td>800</td><td>1400</td>
      <td>250</td><td>30</td><td>200</td><td>750</td><td>100</td><td>50</td>
      <td>500</td><td>1000</td><td>0.269</td><td>0.350</td><td>0.450</td><td>0.800</td>
    </tr>
    <tr>
      <td>KT</td><td>144</td><td>5900</td><td>5100</td><td>750</td><td>1350</td>
      <td>240</td><td>25</td><td>180</td><td>700</td><td>90</td><td>45</td>
      <td>480</td><td>980</td><td>0.265</td><td>0.340</td><td>0.440</td><td>0.780</td>
    </tr>
  </tbody>
</table>
</body></html>
"""

SAMPLE_BATTING_HTML_NO_TABLE = "<html><body><p>No data</p></body></html>"

SAMPLE_BATTING_HTML_NO_HEADERS = """
<html><body>
<table>
  <tr><td>LG</td><td>144</td></tr>
</table>
</body></html>
"""


class TestParseTeamBattingHtml:
    def test_parses_valid_html(self):
        mapping = {"LG": "LG", "KT": "KT"}
        result = parse_team_batting_html(SAMPLE_BATTING_HTML, 2025, "REGULAR", mapping)
        assert len(result) == 2

    def test_first_row_fields(self):
        mapping = {"LG": "LG", "KT": "KT"}
        result = parse_team_batting_html(SAMPLE_BATTING_HTML, 2025, "REGULAR", mapping)
        lg = result[0]
        assert lg["team_id"] == "LG"
        assert lg["team_name"] == "LG"
        assert lg["season"] == 2025
        assert lg["league"] == "REGULAR"
        assert lg["games"] == 144
        assert lg["plate_appearances"] == 6000
        assert lg["at_bats"] == 5200
        assert lg["hits"] == 1400
        assert lg["home_runs"] == 200
        assert lg["avg"] == pytest.approx(0.269)
        assert lg["ops"] == pytest.approx(0.800)

    def test_no_table_returns_empty(self):
        result = parse_team_batting_html(SAMPLE_BATTING_HTML_NO_TABLE, 2025, "REGULAR", {})
        assert result == []

    def test_no_team_name_header_still_parses_with_fallback(self):
        result = parse_team_batting_html(SAMPLE_BATTING_HTML_NO_HEADERS, 2025, "REGULAR", {})
        assert len(result) == 1
        assert result[0]["team_name"] == "LG"


class TestExtractStatRows:
    def test_extracts_tbody_rows(self):
        from bs4 import BeautifulSoup

        html = """
        <table>
          <tbody>
            <tr><td>A</td><td>1</td></tr>
            <tr><td>B</td><td>2</td></tr>
          </tbody>
        </table>
        """
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        rows = _extract_stat_rows(table)
        assert len(rows) == 2

    def test_fallback_to_tr_with_td(self):
        from bs4 import BeautifulSoup

        html = """
        <table>
          <tr><th>H</th><th>V</th></tr>
          <tr><td>A</td><td>1</td></tr>
        </table>
        """
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        rows = _extract_stat_rows(table)
        assert len(rows) == 1


class TestParseTeamBattingRow:
    def test_valid_row(self):
        from bs4 import BeautifulSoup

        mapping = {"LG": "LG"}
        headers = ["팀명", "경기", "타율"]
        indexes = _build_column_map(headers)

        html = "<table><tbody><tr><td>LG</td><td>144</td><td>0.269</td></tr></tbody></table>"
        soup = BeautifulSoup(html, "html.parser")
        row = soup.find("tr")

        result = _parse_team_batting_row(row, indexes, 2025, "REGULAR", mapping)
        assert result is not None
        assert result["team_id"] == "LG"
        assert result["games"] == 144
        assert result["avg"] == pytest.approx(0.269)

    def test_empty_team_name_returns_none(self):
        from bs4 import BeautifulSoup

        headers = ["팀명", "경기"]
        indexes = _build_column_map(headers)

        html = "<table><tbody><tr><td></td><td>144</td></tr></tbody></table>"
        soup = BeautifulSoup(html, "html.parser")
        row = soup.find("tr")

        result = _parse_team_batting_row(row, indexes, 2025, "REGULAR", {})
        assert result is None

    def test_too_few_cells_returns_none(self):
        from bs4 import BeautifulSoup

        headers = ["팀명", "경기", "타석", "타수"]
        indexes = _build_column_map(headers)

        html = "<table><tbody><tr><td>LG</td><td>144</td></tr></tbody></table>"
        soup = BeautifulSoup(html, "html.parser")
        row = soup.find("tr")

        result = _parse_team_batting_row(row, indexes, 2025, "REGULAR", {})
        assert result is None


class TestAddBattingValues:
    def test_known_fields_added(self):
        from bs4 import BeautifulSoup

        headers = ["팀명", "경기", "타율"]
        indexes = _build_column_map(headers)

        html = "<table><tbody><tr><td>LG</td><td>144</td><td>0.269</td></tr></tbody></table>"
        soup = BeautifulSoup(html, "html.parser")
        row = soup.find("tr")
        cells = row.find_all("td")

        payload = {}
        extras = _add_batting_values(payload, cells, indexes)
        assert payload["games"] == 144
        assert payload["avg"] == pytest.approx(0.269)
        assert extras == {}

    def test_unknown_fields_go_to_extras(self):
        from bs4 import BeautifulSoup

        indexes = {"team_name": 0, "games": 1, "statX": 2}

        html = "<table><tbody><tr><td>LG</td><td>144</td><td>99</td></tr></tbody></table>"
        soup = BeautifulSoup(html, "html.parser")
        row = soup.find("tr")
        cells = row.find_all("td")

        payload = {}
        extras = _add_batting_values(payload, cells, indexes)
        assert "games" in payload
        assert extras.get("statX") == 99


class TestHeaderMap:
    def test_float_fields_defined(self):
        assert "avg" in FLOAT_FIELDS
        assert "obp" in FLOAT_FIELDS
        assert "slg" in FLOAT_FIELDS
        assert "ops" in FLOAT_FIELDS

    def test_batting_fields_complete(self):
        expected = {
            "games",
            "plate_appearances",
            "at_bats",
            "runs",
            "hits",
            "doubles",
            "triples",
            "home_runs",
            "rbi",
            "stolen_bases",
            "caught_stealing",
            "walks",
            "strikeouts",
            "avg",
            "obp",
            "slg",
            "ops",
        }
        assert expected == BATTING_FIELDS

    def test_header_map_has_korean_and_english(self):
        assert HEADER_MAP["팀"] == "team_name"
        assert HEADER_MAP["팀명"] == "team_name"
        assert HEADER_MAP["경기"] == "games"
        assert HEADER_MAP["g"] == "games"
        assert HEADER_MAP["타율"] == "avg"
