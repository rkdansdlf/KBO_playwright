from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.cli.check_data_status import _safe_first, _safe_rows, _safe_scalar


class TestSafeScalar:
    def test_returns_default_on_error(self) -> None:
        session = MagicMock()
        session.execute.side_effect = SQLAlchemyError("connection lost")
        assert _safe_scalar(session, "SELECT 1") == 0

    def test_returns_custom_default(self) -> None:
        session = MagicMock()
        session.execute.side_effect = SQLAlchemyError("fail")
        assert _safe_scalar(session, "SELECT 1", default=-1) == -1

    def test_returns_none_as_default(self) -> None:
        session = MagicMock()
        result_mock = MagicMock()
        result_mock.scalar.return_value = None
        session.execute.return_value = result_mock
        assert _safe_scalar(session, "SELECT 1") == 0

    def test_returns_value(self) -> None:
        session = MagicMock()
        result_mock = MagicMock()
        result_mock.scalar.return_value = 42
        session.execute.return_value = result_mock
        assert _safe_scalar(session, "SELECT 1") == 42


class TestSafeRows:
    def test_returns_empty_on_error(self) -> None:
        session = MagicMock()
        session.execute.side_effect = SQLAlchemyError("deadlock")
        assert _safe_rows(session, "SELECT 1") == []

    def test_returns_rows(self) -> None:
        session = MagicMock()
        result_mock = MagicMock()
        result_mock.all.return_value = [("a", 1), ("b", 2)]
        session.execute.return_value = result_mock
        assert _safe_rows(session, "SELECT 1") == [("a", 1), ("b", 2)]


class TestSafeFirst:
    def test_returns_none_tuple_on_error(self) -> None:
        session = MagicMock()
        session.execute.side_effect = SQLAlchemyError("timeout")
        assert _safe_first(session, "SELECT 1") == (None, None)

    def test_returns_first_row(self) -> None:
        session = MagicMock()
        result_mock = MagicMock()
        result_mock.first.return_value = ("min_date", "max_date")
        session.execute.return_value = result_mock
        assert _safe_first(session, "SELECT 1") == ("min_date", "max_date")
