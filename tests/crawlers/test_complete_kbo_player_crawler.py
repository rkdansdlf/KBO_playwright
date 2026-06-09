
from src.crawlers.complete_kbo_player_crawler import (
    extract_basic2_stats,
    parse_other_series_stats,
    parse_player_id_from_link,
    parse_regular_season_basic1_stats,
    safe_parse_number,
)


class TestSafeParseNumber:
    def test_int_parsing(self):
        assert safe_parse_number("42", int) == 42

    def test_float_parsing(self):
        assert safe_parse_number("0.315", float) == 0.315

    def test_empty_or_dash_returns_none(self):
        assert safe_parse_number("", int) is None
        assert safe_parse_number("-", int) is None
        assert safe_parse_number("N/A", int) is None

    def test_invalid_returns_none(self):
        assert safe_parse_number("abc", int) is None


class TestParsePlayerIdFromLink:
    def test_basic_link(self):
        href = "/Record/Player/HitterDetail/Basic.aspx?playerId=12345&season=2025"
        assert parse_player_id_from_link(href) == 12345

    def test_no_player_id_returns_none(self):
        assert parse_player_id_from_link("/some/other/page.aspx") is None

    def test_malformed_id_returns_none(self):
        href = "/page.aspx?playerId=abc&season=2025"
        assert parse_player_id_from_link(href) is None


class FakeCell:
    def __init__(self, text):
        self._text = text

    def inner_text(self):
        return self._text


class TestParseRegularSeasonBasic1Stats:
    def test_full_stats(self):
        cells = [FakeCell("")] * 16
        values = ["", "", "", ".300", "50", "200", "180", "30", "60", "8", "2", "5", "90", "25", "10", "8"]
        for i, v in enumerate(values):
            cells[i] = FakeCell(v)
        result = parse_regular_season_basic1_stats(cells)
        assert result["avg"] == 0.3
        assert result["games"] == 50
        assert result["plate_appearances"] == 200
        assert result["at_bats"] == 180
        assert result["runs"] == 30
        assert result["hits"] == 60
        assert result["doubles"] == 8
        assert result["triples"] == 2
        assert result["home_runs"] == 5
        assert result["total_bases"] == 90
        assert result["rbis"] == 25
        assert result["sacrifice_bunts"] == 10
        assert result["sacrifice_flies"] == 8

    def test_too_few_cells_returns_empty(self):
        cells = [FakeCell("")] * 3
        assert parse_regular_season_basic1_stats(cells) == {}


class TestParseOtherSeriesStats:
    def test_full_stats(self):
        cells = [FakeCell("")] * 19
        values = ["", "", "", ".250", "10", "40", "35", "10", "2", "1", "0", "5", "1", "0", "3", "1", "8", "1", "0"]
        for i, v in enumerate(values):
            cells[i] = FakeCell(v)
        result = parse_other_series_stats(cells)
        assert result["avg"] == 0.25
        assert result["games"] == 10
        assert result["stolen_bases"] == 1
        assert result["walks"] == 3
        assert result["errors"] == 0

    def test_too_few_cells_returns_empty(self):
        cells = [FakeCell("")] * 3
        assert parse_other_series_stats(cells) == {}


class TestExtractBasic2Stats:
    def test_extract_bb(self):
        cells = [FakeCell("")] * 15
        for i in range(15):
            cells[i] = FakeCell(str(i))
        result = extract_basic2_stats(cells, "BB")
        assert result["walks"] == 4

    def test_extract_slg(self):
        cells = [FakeCell("")] * 15
        for i in range(15):
            cells[i] = FakeCell(str(i))
        result = extract_basic2_stats(cells, "SLG")
        assert result["slg"] == 9.0

    def test_unknown_field_returns_empty(self):
        cells = [FakeCell("")] * 15
        assert extract_basic2_stats(cells, "NONEXIST") == {}
