"""Regression tests for oracle_writer.count_table failure contract (S-2).

Validates that count_table propagates query exceptions instead of silently
returning 0.  All tests use mock engines — no real Oracle connections.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from src.sync.oracle_writer import OracleWriter


def _make_writer_with_mock_engine() -> OracleWriter:
    """Create an OracleWriter with a mocked engine (no real Oracle connection)."""
    writer = OracleWriter.__new__(OracleWriter)
    writer.engine = MagicMock()
    writer.conn = MagicMock()
    return writer


# ── S-2 reproduction: silent 0 on query failure ─────────────────────────


class TestCountTableFailureContract:
    """count_table must propagate exceptions, not return silent 0."""

    def test_count_table_returns_integer_on_success(self) -> None:
        """Normal path: scalar count → int."""
        writer = _make_writer_with_mock_engine()
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = 42
        writer.engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        writer.engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        result = writer.count_table("game")
        assert result == 42
        assert isinstance(result, int)

    def test_count_table_returns_zero_for_empty_table(self) -> None:
        """Empty table returns 0 (legitimate zero, not error masking)."""
        writer = _make_writer_with_mock_engine()
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = 0
        writer.engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        writer.engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        result = writer.count_table("game")
        assert result == 0

    def test_count_table_raises_on_sqlalchemy_error(self) -> None:
        """S-2: SQLAlchemyError must propagate, not return silent 0."""
        writer = _make_writer_with_mock_engine()
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = OperationalError(
            statement="SELECT COUNT(*)",
            params={},
            orig=Exception("connection refused"),
        )
        writer.engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        writer.engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(OperationalError):
            writer.count_table("game")

    def test_count_table_raises_on_oracledb_error(self) -> None:
        """S-2: oracledb.Error must propagate, not return silent 0."""
        writer = _make_writer_with_mock_engine()
        mock_conn = MagicMock()

        # Create a fake oracledb.Error-like exception
        oracledb_error = type("Error", (Exception,), {})("ORA-01017: invalid credentials")
        mock_conn.execute.side_effect = oracledb_error
        writer.engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        writer.engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(Exception, match="ORA-01017"):
            writer.count_table("game")

    def test_count_table_error_does_not_leak_dsn(self) -> None:
        """Exception messages must not contain DSN or credential fragments."""
        writer = _make_writer_with_mock_engine()
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = OperationalError(
            statement="SELECT COUNT(*)",
            params={},
            orig=Exception("connection refused"),
        )
        writer.engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        writer.engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(OperationalError) as exc_info:
            writer.count_table("game")

        error_str = str(exc_info.value)
        # Should not contain wallet paths or oracle DSN patterns
        assert "wallet" not in error_str.lower() or "password" not in error_str.lower()

    def test_count_table_handles_null_scalar(self) -> None:
        """NULL scalar result (edge case) returns 0."""
        writer = _make_writer_with_mock_engine()
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = None
        writer.engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        writer.engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        result = writer.count_table("game")
        assert result == 0
