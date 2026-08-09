from __future__ import annotations

import logging

import pytest

from src.cli.fix_player_names import (
    _filter_valid_players,
    _save_players_if_requested,
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
