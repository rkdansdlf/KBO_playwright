import pytest

from src.crawlers.team_pitching_stats_crawler import (
    FLOAT_FIELDS,
    HEADER_MAP,
    PITCHING_FIELDS,
    _add_pitching_values,
    _build_column_map,
    _extract_stat_rows,
    _parse_pitching_value,
    _parse_team_pitching_row,
    parse_team_pitching_html,
)
from src.utils.type_helpers import parse_innings


class TestBuildColumnMap:
    def test_korean_headers(self):
        headers = ["팀명", "경기", "승", "패", "방어율"]
        result = _build_column_map(headers)
        assert result["team_name"] == 0
        assert "games" in result
        assert "era" in result

    def test_english_headers(self):
        headers = ["팀", "g", "w", "l", "era"]
        result = _build_column_map(headers)
        assert result["team_name"] == 0

    def test_empty_headers_fallback(self):
        result = _build_column_map([])
        assert result["team_name"] == 0

    def test_missing_team_name_fallback(self):
        headers = ["a", "b", "c"]
        result = _build_column_map(headers)
        assert result["team_name"] == 1


class TestParseInnings:
    def test_whole_number(self):
        assert parse_innings("9") == 9.0

    def test_decimal_innings(self):
        result = parse_innings("6.1")
        assert result == 6.1

    def test_float_string(self):
        result = parse_innings("0.2")
        assert result == 0.2

    def test_empty_returns_zero(self):
        assert parse_innings("") == 0.0
        assert parse_innings("-") == 0.0


SAMPLE_PITCHING_HTML = """
<html><body>
<table class="tData01">
  <thead>
    <tr>
      <th>팀명</th><th>경기</th><th>승</th><th>패</th><th>이닝</th><th>실점</th>
      <th>자책</th><th>피안타</th><th>피홈런</th><th>볼넷</th><th>탈삼진</th>
      <th>방어율</th><th>WHIP</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>LG</td><td>144</td><td>80</td><td>60</td><td>1200.1</td><td>600</td>
      <td>550</td><td>1100</td><td>150</td><td>400</td><td>1200</td>
      <td>4.50</td><td>1.25</td>
    </tr>
    <tr>
      <td>KT</td><td>144</td><td>75</td><td>65</td><td>1190.2</td><td>580</td>
      <td>530</td><td>1080</td><td>140</td><td>390</td><td>1150</td>
      <td>4.30</td><td>1.20</td>
    </tr>
  </tbody>
</table>
</body></html>
"""

SAMPLE_PITCHING_HTML_NO_TABLE = "<html><body><p>No data</p></body></html>"


class TestParseTeamPitchingHtml:
    def test_parses_valid_html(self):
        mapping = {"LG": "LG", "KT": "KT"}
        result = parse_team_pitching_html(SAMPLE_PITCHING_HTML, 2025, "REGULAR", mapping)
        assert len(result) == 2

    def test_first_row_fields(self):
        mapping = {"LG": "LG", "KT": "KT"}
        result = parse_team_pitching_html(SAMPLE_PITCHING_HTML, 2025, "REGULAR", mapping)
        lg = result[0]
        assert lg["team_id"] == "LG"
        assert lg["team_name"] == "LG"
        assert lg["season"] == 2025
        assert lg["league"] == "REGULAR"
        assert lg["games"] == 144
        assert lg["wins"] == 80
        assert lg["losses"] == 60
        assert lg["era"] == pytest.approx(4.50)

    def test_no_table_returns_empty(self):
        result = parse_team_pitching_html(SAMPLE_PITCHING_HTML_NO_TABLE, 2025, "REGULAR", {})
        assert result == []


class TestParsePitchingValue:
    def test_innings_pitched_uses_parse_innings(self):
        result = _parse_pitching_value("innings_pitched", "6.1")
        assert result == 6.1

    def test_float_field(self):
        result = _parse_pitching_value("era", "4.50")
        assert result == 4.5

    def test_int_field(self):
        result = _parse_pitching_value("wins", "80")
        assert result == 80


class TestExtractStatRows:
    def test_tbody_rows(self):
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
        rows = _extract_stat_rows(soup.find("table"))
        assert len(rows) == 2


class TestParseTeamPitchingRow:
    def test_valid_row(self):
        from bs4 import BeautifulSoup

        mapping = {"LG": "LG"}
        headers = ["팀명", "경기", "이닝", "era"]
        indexes = _build_column_map(headers)

        html = "<table><tbody><tr><td>LG</td><td>144</td><td>1200.1</td><td>4.50</td></tr></tbody></table>"
        soup = BeautifulSoup(html, "html.parser")
        row = soup.find("tr")

        result = _parse_team_pitching_row(row, indexes, 2025, "REGULAR", mapping)
        assert result is not None
        assert result["team_id"] == "LG"
        assert result["games"] == 144
        assert result["era"] == pytest.approx(4.50)

    def test_empty_team_name_returns_none(self):
        from bs4 import BeautifulSoup

        headers = ["팀명", "경기"]
        indexes = _build_column_map(headers)

        html = "<table><tbody><tr><td></td><td>144</td></tr></tbody></table>"
        soup = BeautifulSoup(html, "html.parser")
        row = soup.find("tr")

        result = _parse_team_pitching_row(row, indexes, 2025, "REGULAR", {})
        assert result is None


class TestAddPitchingValues:
    def test_known_fields_added(self):
        from bs4 import BeautifulSoup

        indexes = {"team_name": 0, "games": 1, "era": 2}

        html = "<table><tbody><tr><td>LG</td><td>144</td><td>4.50</td></tr></tbody></table>"
        soup = BeautifulSoup(html, "html.parser")
        cells = soup.find("tr").find_all("td")

        payload = {}
        extras = _add_pitching_values(payload, cells, indexes)
        assert payload["games"] == 144
        assert payload["era"] == pytest.approx(4.50)
        assert extras == {}


class TestHeaderMap:
    def test_pitching_fields_complete(self):
        assert "era" in PITCHING_FIELDS
        assert "whip" in PITCHING_FIELDS
        assert "innings_pitched" in PITCHING_FIELDS
        assert "strikeouts" in PITCHING_FIELDS

    def test_float_fields(self):
        assert "innings_pitched" in FLOAT_FIELDS
        assert "era" in FLOAT_FIELDS
        assert "whip" in FLOAT_FIELDS

    def test_header_map_has_pitching_stats(self):
        assert HEADER_MAP["era"] == "era"
        assert HEADER_MAP["방어율"] == "era"
        assert HEADER_MAP["이닝"] == "innings_pitched"
        assert HEADER_MAP["승"] == "wins"
