"""Tests for dashboard_report pure functions."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.cli.dashboard_report import (
    _date_or_today,
    _row_value,
    _r2dict,
)


class TestR2Dict:
    def test_converts_orm_like_object(self) -> None:
        class MockColumn:
            def __init__(self, name: str) -> None:
                self.name = name

        class MockTable:
            columns = [MockColumn("id"), MockColumn("name"), MockColumn("value")]

        class MockModel:
            __table__ = MockTable()

        obj = MagicMock()
        obj.id = 1
        obj.name = "test"
        obj.value = 42

        result = _r2dict(obj, MockModel)
        assert result == {"id": 1, "name": "test", "value": 42}


class TestDateOrToday:
    def test_returns_today_when_none(self) -> None:
        result = _date_or_today(None)
        assert len(result) == 8
        assert result.isdigit()

    def test_returns_today_when_empty(self) -> None:
        result = _date_or_today("")
        assert len(result) == 8
        assert result.isdigit()

    def test_returns_input_when_valid(self) -> None:
        result = _date_or_today("20250101")
        assert result == "20250101"


class TestRowValue:
    def test_attribute_access(self) -> None:
        class Row:
            name = "test"
            value = 42

        row = Row()
        assert _row_value(row, "name") == "test"
        assert _row_value(row, "value") == 42

    def test_dict_access(self) -> None:
        row = {"name": "test", "value": 42}
        assert _row_value(row, "name") == "test"
        assert _row_value(row, "value") == 42

    def test_default_for_missing_attr(self) -> None:
        class Row:
            name = "test"

        row = Row()
        assert _row_value(row, "missing", "default") == "default"

    def test_default_for_missing_dict_key(self) -> None:
        row = {"name": "test"}
        assert _row_value(row, "missing", "default") == "default"

    def test_none_default(self) -> None:
        class Row:
            pass

        row = Row()
        assert _row_value(row, "missing") is None
