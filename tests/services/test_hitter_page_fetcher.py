from __future__ import annotations

from unittest.mock import MagicMock, patch

from bs4 import BeautifulSoup

from src.services.hitter_page_fetcher import (
    _get_column_index_map,
    build_hitter_url,
    derive_sh_sf_from_hitter_page,
    derive_sh_sf_hybrid,
    fetch_and_parse_hitter_sh_sf,
    fetch_hitter_page_sync,
    parse_hitter_sh_sf,
)
from src.utils.type_helpers import to_int

HITTER_HTML_TEMPLATE = """<html>
<body>
<table id="tbl{side}Hitter1">
<thead><tr><th>선수명</th></tr></thead>
<tbody>
<tr><td><a href="?playerId={pid}" title="{pname}">{pname}</a></td></tr>
</tbody>
</table>
<table id="tbl{side}Hitter3">
<thead><tr><th>타수</th><th>희타</th><th>희비</th></tr></thead>
<tbody>
<tr><td>4</td><td>{sh}</td><td>{sf}</td></tr>
</tbody>
</table>
</body>
</html>"""


class TestBuildHitterUrl:
    def test_yyyy_mm_dd(self):
        url = build_hitter_url("game123", "2024-10-15")
        assert "gameDate=20241015" in url
        assert "gameId=game123" in url
        assert "section=HITTER" in url

    def test_yyyymmdd(self):
        url = build_hitter_url("g1", "20240101")
        assert "gameDate=20240101" in url

    def test_base_url(self):
        url = build_hitter_url("g1", "20240101")
        assert url.startswith("https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx")


class TestSafeInt:
    def test_int(self):
        assert to_int(42) == 42

    def test_str(self):
        assert to_int("42") == 42

    def test_none(self):
        assert to_int(None) == 0

    def test_invalid(self):
        assert to_int("abc") == 0

    def test_float_str(self):
        assert to_int("3.14") == 0


class TestGetColumnIndexMap:
    def test_normal_table(self):
        html = "<table><thead><tr><th>희타</th><th>희비</th><th>타수</th></tr></thead></table>"
        tag = BeautifulSoup(html, "html.parser")
        table = tag.find("table")
        col_map = _get_column_index_map(table)
        assert col_map.get("희타") == 0
        assert col_map.get("희비") == 1
        assert col_map.get("타수") == 2

    def test_no_thead(self):
        html = "<table><tbody><tr><td>data</td></tr></tbody></table>"
        tag = BeautifulSoup(html, "html.parser")
        table = tag.find("table")
        assert _get_column_index_map(table) == {}

    def test_no_th(self):
        html = "<table><thead><tr><td>not a header</td></tr></thead></table>"
        tag = BeautifulSoup(html, "html.parser")
        table = tag.find("table")
        assert _get_column_index_map(table) == {}


class TestParseHitterShSf:
    def test_away_and_home_both_have_sh(self):
        away_html = HITTER_HTML_TEMPLATE.format(side="Away", pid="101", pname="Kim", sh="1", sf="0")
        home_html = HITTER_HTML_TEMPLATE.format(side="Home", pid="201", pname="Lee", sh="0", sf="2")
        combined = away_html + home_html
        result = parse_hitter_sh_sf(combined, "g1")
        assert result[101]["sh"] == 1
        assert result[201]["sf"] == 2

    def test_no_tables(self):
        result = parse_hitter_sh_sf("<html></html>", "g1")
        assert result == {}

    def test_missing_columns(self):
        html = """
        <table id="tblAwayHitter1"><thead><tr><th>선수명</th></tr></thead>
        <tbody><tr><td><a href="?playerId=1">Kim</a></td></tr></tbody></table>
        <table id="tblAwayHitter3"><thead><tr><th>타수</th><th>안타</th></tr></thead>
        <tbody><tr><td>4</td><td>2</td></tr></tbody></table>
        """
        result = parse_hitter_sh_sf(html, "g1")
        assert result == {}

    def test_fallback_to_player_name_when_no_id(self):
        html = """
        <table id="tblAwayHitter1"><thead><tr><th>선수명</th></tr></thead>
        <tbody><tr><td><a href="?noid=1" title="Kim">Kim</a></td></tr></tbody></table>
        <table id="tblAwayHitter3"><thead><tr><th>희타</th><th>희비</th></tr></thead>
        <tbody><tr><td>1</td><td>0</td></tr></tbody></table>
        """
        result = parse_hitter_sh_sf(html, "g1")
        assert result.get("Kim", {}).get("sh") == 1

    def test_accumulates_across_away_and_home(self):
        away = '<table id="tblAwayHitter1"><thead><tr><th>선수명</th></tr></thead><tbody><tr><td><a href="?playerId=1">Kim</a></td></tr></tbody></table>'
        away += '<table id="tblAwayHitter3"><thead><tr><th>희타</th><th>희비</th></tr></thead><tbody><tr><td>1</td><td>1</td></tr></tbody></table>'
        home = '<table id="tblHomeHitter1"><thead><tr><th>선수명</th></tr></thead><tbody><tr><td><a href="?playerId=2">Lee</a></td></tr></tbody></table>'
        home += '<table id="tblHomeHitter3"><thead><tr><th>희타</th><th>희비</th></tr></thead><tbody><tr><td>2</td><td>0</td></tr></tbody></table>'
        result = parse_hitter_sh_sf(away + home, "g1")
        assert result[1]["sh"] == 1
        assert result[1]["sf"] == 1
        assert result[2]["sh"] == 2

    def test_zero_values_omitted(self):
        html = HITTER_HTML_TEMPLATE.format(side="Away", pid="1", pname="Kim", sh="0", sf="0")
        result = parse_hitter_sh_sf(html, "g1")
        assert result == {}


class TestFetchHitterPageSync:
    def test_success(self):
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.text = "<html>content</html>"
        mock_response.raise_for_status.return_value = None
        mock_client.get.return_value = mock_response
        result = fetch_hitter_page_sync("g1", "20241015", client=mock_client)
        assert result == "<html>content</html>"

    def test_http_error(self):
        import httpx
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_client.get.side_effect = httpx.HTTPStatusError("not found", request=MagicMock(), response=mock_response)
        result = fetch_hitter_page_sync("g1", "20241015", client=mock_client)
        assert result is None

    def test_timeout(self):
        import httpx
        mock_client = MagicMock()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")
        result = fetch_hitter_page_sync("g1", "20241015", client=mock_client)
        assert result is None

    def test_generic_http_error(self):
        import httpx
        mock_client = MagicMock()
        mock_client.get.side_effect = httpx.HTTPError("generic")
        result = fetch_hitter_page_sync("g1", "20241015", client=mock_client)
        assert result is None

    def test_no_client_creates_own(self):
        with patch("src.services.hitter_page_fetcher.httpx.Client") as MockClient:
            mock_instance = MagicMock()
            mock_response = MagicMock()
            mock_response.text = "html"
            mock_response.raise_for_status.return_value = None
            mock_instance.get.return_value = mock_response
            MockClient.return_value.__enter__.return_value = mock_instance
            result = fetch_hitter_page_sync("g1", "20241015")
            assert result == "html"


class TestFetchAndParseHitterShSf:
    def test_fetch_and_parse(self):
        mock_client = MagicMock()
        mock_response = MagicMock()
        html = HITTER_HTML_TEMPLATE.format(side="Away", pid="1", pname="Kim", sh="1", sf="1")
        mock_response.text = html
        mock_response.raise_for_status.return_value = None
        mock_client.get.return_value = mock_response
        result = fetch_and_parse_hitter_sh_sf("g1", "20241015", client=mock_client)
        assert result[1]["sh"] == 1
        assert result[1]["sf"] == 1

    def test_fetch_failure_returns_empty(self):
        import httpx
        mock_client = MagicMock()
        mock_client.get.side_effect = httpx.HTTPStatusError("error", request=MagicMock(), response=MagicMock())
        result = fetch_and_parse_hitter_sh_sf("g1", "20241015", client=mock_client)
        assert result == {}


class TestDeriveShSfFromHitterPage:
    def test_updates_game_batting_stats(self):
        mock_session = MagicMock()
        mock_client = MagicMock()
        html = HITTER_HTML_TEMPLATE.format(side="Away", pid="1", pname="Kim", sh="1", sf="1")
        mock_response = MagicMock()
        mock_response.text = html
        mock_response.raise_for_status.return_value = None
        mock_client.get.return_value = mock_response
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result
        updated = derive_sh_sf_from_hitter_page(mock_session, "g1", "20241015", client=mock_client)
        assert updated == 1
        mock_session.execute.assert_called()

    def test_no_hitter_data(self):
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.text = "<html></html>"
        mock_response.raise_for_status.return_value = None
        mock_client.get.return_value = mock_response
        updated = derive_sh_sf_from_hitter_page(mock_session, "g1", "20241015", client=mock_client)
        assert updated == 0

    def test_name_fallback_update(self):
        mock_session = MagicMock()
        mock_client = MagicMock()
        html = """
        <table id="tblAwayHitter1"><thead><tr><th>선수명</th></tr></thead>
        <tbody><tr><td><a href="?noid=1" title="Kim">Kim</a></td></tr></tbody></table>
        <table id="tblAwayHitter3"><thead><tr><th>희타</th><th>희비</th></tr></thead>
        <tbody><tr><td>2</td><td>0</td></tr></tbody></table>
        """
        mock_response = MagicMock()
        mock_response.text = html
        mock_response.raise_for_status.return_value = None
        mock_client.get.return_value = mock_response
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result
        updated = derive_sh_sf_from_hitter_page(mock_session, "g1", "20241015", client=mock_client)
        assert updated == 1


class TestDeriveShSfHybrid:
    def test_pbp_success_no_hitter_fetch(self):
        mock_session = MagicMock()
        with patch("src.services.pbp_sh_sf_derivation.apply_sh_sf_to_batting_stats") as mock_pbp:
            mock_pbp.return_value = 3
            updated = derive_sh_sf_hybrid(mock_session, "g1", "20241015")
            assert updated == 3

    def test_pbp_failure_falls_back_to_hitter(self):
        mock_session = MagicMock()
        mock_client = MagicMock()
        html = HITTER_HTML_TEMPLATE.format(side="Away", pid="1", pname="Kim", sh="1", sf="0")
        mock_response = MagicMock()
        mock_response.text = html
        mock_response.raise_for_status.return_value = None
        mock_client.get.return_value = mock_response
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result
        with patch("src.services.pbp_sh_sf_derivation.apply_sh_sf_to_batting_stats") as mock_pbp:
            mock_pbp.return_value = 0
            updated = derive_sh_sf_hybrid(mock_session, "g1", "20241015", client=mock_client)
            assert updated == 1
