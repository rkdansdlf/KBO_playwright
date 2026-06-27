"""Tests for player_batting_all_series_crawler pure functions."""

from __future__ import annotations

import pytest

from src.crawlers.player_batting_all_series_crawler import (
    BattingRowData,
    LegacyRowContext,
    _build_batting_data,
    _extract_player_id_from_href,
    _is_basic2_headers,
    _parse_legacy_row,
    build_batting_crawl_summary,
    get_series_mapping,
    safe_parse_number,
)


class TestGetSeriesMapping:
    def test_returns_all_series(self) -> None:
        mapping = get_series_mapping()
        assert "regular" in mapping
        assert "exhibition" in mapping
        assert "korean_series" in mapping

    def test_regular_has_correct_values(self) -> None:
        mapping = get_series_mapping()
        reg = mapping["regular"]
        assert reg["value"] == "0"
        assert reg["league"] == "REGULAR"

    def test_korean_series_values(self) -> None:
        mapping = get_series_mapping()
        ks = mapping["korean_series"]
        assert ks["value"] == "7"
        assert ks["league"] == "KOREAN_SERIES"

    def test_exhibition_values(self) -> None:
        mapping = get_series_mapping()
        ex = mapping["exhibition"]
        assert ex["value"] == "1"
        assert ex["league"] == "EXHIBITION"


class TestSafeParseNumber:
    def test_int(self) -> None:
        assert safe_parse_number("42", int) == 42

    def test_float(self) -> None:
        assert safe_parse_number("3.14", float) == 3.14

    def test_none(self) -> None:
        assert safe_parse_number(None, int) is None

    def test_empty(self) -> None:
        assert safe_parse_number("", int) is None

    def test_invalid(self) -> None:
        assert safe_parse_number("abc", int) is None

    def test_zero(self) -> None:
        assert safe_parse_number("0", int) == 0

    def test_dash(self) -> None:
        assert safe_parse_number("-", int) is None

    def test_na(self) -> None:
        assert safe_parse_number("N/A", int) is None

    def test_negative(self) -> None:
        assert safe_parse_number("-5", int) == -5

    def test_whitespace(self) -> None:
        assert safe_parse_number("  42  ", int) == 42


class TestExtractPlayerIdFromHref:
    def test_valid_href(self) -> None:
        assert _extract_player_id_from_href("/player.do?playerId=12345") == 12345

    def test_none(self) -> None:
        assert _extract_player_id_from_href(None) is None

    def test_empty(self) -> None:
        assert _extract_player_id_from_href("") is None

    def test_no_player_id(self) -> None:
        assert _extract_player_id_from_href("/player.do") is None

    def test_malformed(self) -> None:
        assert _extract_player_id_from_href("not-a-url") is None


class TestIsBasic2Headers:
    def test_basic2_headers(self) -> None:
        headers = [
            "이름",
            "팀",
            "AVG",
            "G",
            "PA",
            "AB",
            "R",
            "H",
            "2B",
            "3B",
            "HR",
            "RBI",
            "BB",
            "HBP",
            "SO",
            "GDP",
            "SH",
            "SF",
            "SB",
            "CS",
            "E",
        ]
        assert _is_basic2_headers(headers) is True

    def test_basic1_headers(self) -> None:
        headers = ["이름", "팀", "AVG", "G", "PA", "AB", "R", "H", "HR", "RBI", "SB"]
        assert _is_basic2_headers(headers) is False

    def test_empty(self) -> None:
        assert _is_basic2_headers([]) is False


class TestBuildBattingData:
    def test_basic_payload(self) -> None:
        ctx = BattingRowData(
            cells=[],
            player_id=12345,
            player_name="홍길동",
            team_code="LG",
            series_key="regular",
            is_basic2=False,
            year=2025,
        )
        result = _build_batting_data(ctx)
        assert result["player_name"] == "홍길동"
        assert result["team_code"] == "LG"
        assert result["player_id"] == 12345

    def test_with_year(self) -> None:
        ctx = BattingRowData(
            cells=[],
            player_id=999,
            player_name="김철수",
            team_code="OB",
            series_key="regular",
            is_basic2=True,
            year=2020,
        )
        result = _build_batting_data(ctx)
        assert result["season"] == 2020


class TestBuildBattingCrawlSummary:
    def test_empty_rows(self) -> None:
        summary, valid = build_batting_crawl_summary([])
        assert summary["processed_rows"] == 0
        assert summary["valid_rows"] == 0

    def test_with_valid_rows(self) -> None:
        rows = [
            {"player_id": 1, "player_name": "홍길동", "team_code": "LG", "season": 2025, "games": 10},
            {"player_id": 2, "player_name": "김철수", "team_code": "OB", "season": 2025, "games": 10},
        ]
        summary, valid = build_batting_crawl_summary(rows)
        assert summary["processed_rows"] == 2

    def test_with_mixed_rows(self) -> None:
        rows = [
            {"player_id": 1, "player_name": "홍길동", "team_code": "LG", "season": 2025, "games": 10},
            {"player_id": None, "player_name": "김철수", "team_code": "OB", "season": 2025, "games": 10},
        ]
        summary, valid = build_batting_crawl_summary(rows)
        assert summary["processed_rows"] == 2


class TestParseLegacyRow:
    class _MockCell:
        def __init__(self, text: str, has_link: bool = False, href: str | None = None) -> None:
            self._text = text
            self._has_link = has_link
            self._href = href

        def text_content(self) -> str:
            return self._text.strip()

        def query_selector(self, _: str) -> object | None:
            if self._has_link:
                return TestParseLegacyRow._MockLink(self._text, self._href)
            return None

    class _MockLink:
        def __init__(self, text: str, href: str | None) -> None:
            self._text = text
            self._href = href

        def text_content(self) -> str:
            return self._text.strip()

        def get_attribute(self, name: str) -> str | None:
            if name == "href":
                return self._href
            return None

    class _MockRow:
        def __init__(self, cells: list[object]) -> None:
            self._cells = cells

        def query_selector_all(self, _: str) -> list[object]:
            return self._cells

    def _make_row(self, cells: list[object]) -> object:
        return self._MockRow(cells)

    def test_valid_row(self) -> None:
        cells = [
            self._MockCell("1"),
            self._MockCell("홍길동", has_link=True, href="/player.do?playerId=12345"),
            self._MockCell("LG"),
            self._MockCell(".300"),
            self._MockCell("100"),
            self._MockCell("400"),
        ]
        row = self._make_row(cells)
        ctx = LegacyRowContext(
            row=row,
            row_idx=0,
            current_header="",
            description="",
            year=2025,
            team_mapping={},
        )
        result = _parse_legacy_row(ctx)
        assert result is not None
        pid, data = result
        assert pid == 12345
        assert data["player_name"] == "홍길동"

    def test_insufficient_cells(self) -> None:
        row = self._make_row([self._MockCell("1"), self._MockCell("홍길동")])
        ctx = LegacyRowContext(
            row=row,
            row_idx=0,
            current_header="",
            description="",
            year=2025,
            team_mapping={},
        )
        result = _parse_legacy_row(ctx)
        assert result is None

    def test_no_link_returns_none(self) -> None:
        cells = [
            self._MockCell("1"),
            self._MockCell("홍길동"),
            self._MockCell("LG"),
        ]
        row = self._make_row(cells)
        ctx = LegacyRowContext(
            row=row,
            row_idx=0,
            current_header="",
            description="",
            year=2025,
            team_mapping={},
        )
        result = _parse_legacy_row(ctx)
        assert result is None
