"""Tests for scheduler OCI engine configuration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from scripts import scheduler


def test_query_max_updated_at_uses_shared_engine_factory():
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.scalar.return_value = "2026-07-20 00:00:00"

    with patch.object(scheduler, "create_engine_for_url", return_value=engine) as factory:
        result = scheduler._query_max_updated_at("oracle+oracledb://ADMIN:***@alias")

    factory.assert_called_once_with("oracle+oracledb://ADMIN:***@alias")
    assert result is not None
    assert result.isoformat(sep=" ") == "2026-07-20 00:00:00"
