from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from src.cli.fix_player_names import (
    _filter_valid_players,
    _save_players_if_requested,
    _should_sync_to_oci,
    _sync_player_basic_to_oci,
)


class TestFilterValidPlayers:
    def test_filter_counts_branch(self, caplog: pytest.LogCaptureFixture) -> None:
        raw = [
            {"name": "김", "player_id": "1"},
            {"name": "Invalid", "player_id": None},
        ]
        with caplog.at_level(logging.WARNING):
            result = _filter_valid_players(raw)
        assert len(result) <= len(raw)
        assert any("filtered" in record.message for record in caplog.records)


class TestSavePlayersIfRequested:
    def test_save_false_logs(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.INFO):
            _save_players_if_requested([{"name": "test"}], save=False)
        assert any("Skipping save" in record.message for record in caplog.records)


class TestShouldSyncToOci:
    def test_returns_false_when_not_set(self) -> None:
        assert _should_sync_to_oci(sync_oci=False) is False

    def test_returns_true_when_set(self) -> None:
        assert _should_sync_to_oci(sync_oci=True) is True


class TestSyncPlayerBasicToOci:
    def test_no_oci_url(self, caplog: pytest.LogCaptureFixture) -> None:
        with patch("src.cli.fix_player_names.get_oci_url", return_value=None):
            with caplog.at_level(logging.INFO):
                _sync_player_basic_to_oci()
        assert any("OCI_DB_URL not set" in record.message for record in caplog.records)

    def test_connection_failed(self, caplog: pytest.LogCaptureFixture) -> None:
        mock_sync = MagicMock()
        mock_sync.test_connection.return_value = False
        with (
            patch("src.cli.fix_player_names.get_oci_url", return_value="oci://test"),
            patch("src.sync.oci_sync.OCISync", return_value=mock_sync),
        ):
            with caplog.at_level(logging.INFO):
                _sync_player_basic_to_oci()
        assert any("OCI connection failed" in record.message for record in caplog.records)

    def test_sync_success(self, caplog: pytest.LogCaptureFixture) -> None:
        mock_sync = MagicMock()
        mock_sync.test_connection.return_value = True
        mock_sync.sync_player_basic.return_value = 5
        with (
            patch("src.cli.fix_player_names.get_oci_url", return_value="oci://test"),
            patch("src.sync.oci_sync.OCISync", return_value=mock_sync),
        ):
            with caplog.at_level(logging.INFO):
                _sync_player_basic_to_oci()
        assert any("Synced 5 players" in record.message for record in caplog.records)
        mock_sync.close.assert_called_once()
