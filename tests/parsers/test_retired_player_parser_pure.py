"""Unit tests for retired_player_parser pure functions."""

from __future__ import annotations

import pytest

from src.parsers.retired_player_parser import (
    _clean_header,
    _table_to_dicts,
)


class TestCleanHeader:
    def test_simple(self) -> None:
        assert _clean_header("hello") == "hello"

    def test_newlines(self) -> None:
        assert _clean_header("hello\nworld") == "hello world"

    def test_carriage_returns(self) -> None:
        assert _clean_header("hello\rworld") == "hello world"

    def test_none(self) -> None:
        assert _clean_header(None) == ""

    def test_empty(self) -> None:
        assert _clean_header("") == ""

    def test_whitespace(self) -> None:
        assert _clean_header("  hello  ") == "hello"


class TestTableToDicts:
    def test_basic(self) -> None:
        table = {
            "headers": ["이름", "타수"],
            "rows": [["김철수", "100"]],
        }
        headers, rows = _table_to_dicts(table)
        assert headers == ["이름", "타수"]
        assert rows == [{"이름": "김철수", "타수": "100"}]

    def test_empty_headers(self) -> None:
        table = {
            "headers": [],
            "rows": [["이름", "타수"], ["김철수", "100"]],
        }
        headers, rows = _table_to_dicts(table)
        assert headers == ["이름", "타수"]
        assert rows == [{"이름": "김철수", "타수": "100"}]

    def test_none_headers(self) -> None:
        table = {
            "headers": None,
            "rows": [["이름", "타수"], ["김철수", "100"]],
        }
        headers, rows = _table_to_dicts(table)
        assert headers == ["이름", "타수"]

    def test_empty_rows(self) -> None:
        table = {"headers": ["이름"], "rows": []}
        headers, rows = _table_to_dicts(table)
        assert headers == ["이름"]
        assert rows == []

    def test_mismatched_lengths(self) -> None:
        table = {
            "headers": ["A", "B", "C"],
            "rows": [["1", "2"]],
        }
        headers, rows = _table_to_dicts(table)
        assert rows == []

    def test_whitespace_stripped(self) -> None:
        table = {
            "headers": [" 이름 "],
            "rows": [[" 김철수 "]],
        }
        headers, rows = _table_to_dicts(table)
        assert headers == ["이름"]
        assert rows == [{"이름": "김철수"}]
