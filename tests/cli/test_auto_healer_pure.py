"""Unit tests for auto_healer pure functions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.cli.auto_healer import _apply_heal_outcome


class TestApplyHealOutcome:
    def test_completed_when_detail_saved(self) -> None:
        mock_item = MagicMock()
        mock_item.detail_saved = True
        mock_item.failure_reason = None
        with patch("src.cli.auto_healer.update_game_status") as mock_update:
            result = _apply_heal_outcome("G1", mock_item)
            assert result == "completed"
            mock_update.assert_not_called()

    def test_cancelled_when_failure_reason(self) -> None:
        mock_item = MagicMock()
        mock_item.detail_saved = False
        mock_item.failure_reason = "cancelled"
        with patch("src.cli.auto_healer.update_game_status") as mock_update:
            result = _apply_heal_outcome("G1", mock_item)
            assert result == "cancelled"
            mock_update.assert_called_once()

    def test_unresolved_when_no_item(self) -> None:
        with patch("src.cli.auto_healer.update_game_status") as mock_update:
            result = _apply_heal_outcome("G1", None)
            assert result == "unresolved"
            mock_update.assert_called_once()

    def test_unresolved_with_other_reason(self) -> None:
        mock_item = MagicMock()
        mock_item.detail_saved = False
        mock_item.failure_reason = "other"
        with patch("src.cli.auto_healer.update_game_status") as mock_update:
            result = _apply_heal_outcome("G1", mock_item)
            assert result == "unresolved"
            mock_update.assert_called_once()

    def test_unresolved_with_missing_reason(self) -> None:
        mock_item = MagicMock()
        mock_item.detail_saved = False
        mock_item.failure_reason = None
        with patch("src.cli.auto_healer.update_game_status") as mock_update:
            result = _apply_heal_outcome("G1", mock_item)
            assert result == "unresolved"
            mock_update.assert_called_once()
