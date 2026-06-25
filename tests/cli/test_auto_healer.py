from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.auto_healer import (
    _apply_heal_outcome,
    _find_inconsistent_games,
    _find_recovery_targets,
    _find_stuck_games,
    _log_anomaly_summary,
    _log_healer_summary,
    _pending_recovery_candidates,
    main,
    run_healer_async,
)


class TestAutoHealer:
    def test_dry_run(self):
        with patch("src.cli.auto_healer.run_healer_async", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = 0
            result = main(["--dry-run"])
            assert result == 0

    def test_pbp_dry_run(self):
        with patch("src.cli.auto_healer.run_pbp_healer") as mock_pbp:
            mock_pbp.return_value = 0
            result = main(["--pbp", "--dry-run"])
            assert result == 0

    def test_pbp_with_game_id(self):
        with patch("src.cli.auto_healer.run_pbp_healer") as mock_pbp:
            mock_pbp.return_value = 0
            result = main(["--pbp", "--game-id", "20250401LGSS0"])
            assert result == 0


class TestFindStuckGames:
    def test_returns_empty_when_none(self):
        with patch("src.cli.auto_healer.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalars.return_value.all.return_value = []
            result = _find_stuck_games()
            assert result == []


class TestFindInconsistentGames:
    def test_returns_empty_when_none(self):
        with patch("src.cli.auto_healer.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalars.return_value.all.return_value = []
            result = _find_inconsistent_games()
            assert result == []


class TestApplyHealOutcome:
    def test_completed_when_detail_saved(self):
        mock_item = MagicMock()
        mock_item.detail_saved = True
        mock_item.failure_reason = None
        with patch("src.cli.auto_healer._send_healer_start_alert"):
            result = _apply_heal_outcome("G1", mock_item)
        assert result == "completed"

    def test_cancelled_when_failure_reason(self):
        mock_item = MagicMock()
        mock_item.detail_saved = False
        mock_item.failure_reason = "cancelled"
        with patch("src.cli.auto_healer.update_game_status") as mock_update:
            with patch("src.cli.auto_healer._send_healer_start_alert"):
                result = _apply_heal_outcome("G1", mock_item)
        assert result == "cancelled"
        mock_update.assert_called_once()

    def test_unresolved_when_no_item(self):
        with patch("src.cli.auto_healer.update_game_status") as mock_update:
            with patch("src.cli.auto_healer._send_healer_start_alert"):
                result = _apply_heal_outcome("G1", None)
        assert result == "unresolved"
        mock_update.assert_called_once()

    def test_unresolved_with_other_reason(self):
        mock_item = MagicMock()
        mock_item.detail_saved = False
        mock_item.failure_reason = "timeout"
        with patch("src.cli.auto_healer.update_game_status"):
            with patch("src.cli.auto_healer._send_healer_start_alert"):
                result = _apply_heal_outcome("G1", mock_item)
        assert result == "unresolved"


class TestLogHealerSummary:
    def test_logs_summary(self, caplog):
        with caplog.at_level(logging.INFO):
            _log_healer_summary({"fixed": 5, "failed": 1, "completed": 5}, dry_run=True)
        assert "5" in caplog.text


class TestLogAnomalySummary:
    def test_logs_stuck_games_warning(self, caplog):
        mock_game = MagicMock()
        mock_game.game_id = "G1"
        mock_game.game_status = "SCHEDULED"

        with caplog.at_level(logging.WARNING):
            _log_anomaly_summary(
                all_found=[mock_game],
                inconsistent_games=[],
                pending_ids={"G1"},
                anomaly_dates=[],
            )
        assert "stuck" in caplog.text.lower() or "Anomaly" in caplog.text

    def test_logs_inconsistent_warning(self, caplog):
        mock_game = MagicMock()
        mock_game.game_id = "G2"
        mock_game.game_status = "FINAL"

        with caplog.at_level(logging.WARNING):
            _log_anomaly_summary(
                all_found=[mock_game],
                inconsistent_games=[mock_game],
                pending_ids={"G2"},
                anomaly_dates=["2025-06-15"],
            )
        assert "inconsistent" in caplog.text.lower() or "Anomaly" in caplog.text

    def test_no_warning_when_not_pending(self, caplog):
        mock_game = MagicMock()
        mock_game.game_id = "G3"
        mock_game.game_status = "SCHEDULED"

        with caplog.at_level(logging.WARNING):
            _log_anomaly_summary(
                all_found=[mock_game],
                inconsistent_games=[],
                pending_ids=set(),
                anomaly_dates=[],
            )
        assert "stuck" not in caplog.text.lower()


class TestFindRecoveryTargets:
    def test_with_target_game_ids(self):
        with patch("src.cli.auto_healer.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_game = MagicMock()
            mock_game.game_id = "G1"
            mock_session.execute.return_value.scalars.return_value.all.return_value = [mock_game]

            all_found, stuck, inconsistent = _find_recovery_targets(["G1"])

        assert len(all_found) == 1
        assert stuck == []
        assert inconsistent == []

    def test_without_targets_uses_find_functions(self):
        with patch("src.cli.auto_healer._find_stuck_games", return_value=[]):
            with patch("src.cli.auto_healer._find_inconsistent_games", return_value=[]):
                all_found, stuck, inconsistent = _find_recovery_targets(None)

        assert all_found == []
        assert stuck == []
        assert inconsistent == []


class TestPendingRecoveryCandidates:
    def test_filters_to_pending(self):

        mock_game1 = MagicMock()
        mock_game1.game_id = "G1"
        mock_game2 = MagicMock()
        mock_game2.game_id = "G2"

        mock_mgr = MagicMock()
        mock_mgr.initialize_run.return_value = None
        mock_mgr.get_pending_targets.return_value = ["G1"]

        pending_ids, candidates = _pending_recovery_candidates(mock_mgr, [mock_game1, mock_game2])
        assert pending_ids == {"G1"}
        assert len(candidates) == 1
        assert candidates[0].game_id == "G1"


class TestRunHealerAsync:
    async def test_returns_zero(self):
        with (
            patch("src.cli.auto_healer._find_stuck_games") as mock_stuck,
            patch("src.cli.auto_healer._find_inconsistent_games") as mock_incon,
            patch("src.cli.auto_healer._find_recovery_targets") as mock_targets,
        ):
            mock_stuck.return_value = []
            mock_incon.return_value = []
            mock_targets.return_value = ([], [], [])
            result = await run_healer_async(dry_run=True)
            assert result == 0
