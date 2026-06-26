from __future__ import annotations

import asyncio
import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.auto_healer import (
    _apply_heal_outcome,
    _find_inconsistent_games,
    _find_recovery_targets,
    _find_stuck_games,
    _find_unverified_pbp_games,
    _log_anomaly_summary,
    _log_healer_summary,
    _pending_recovery_candidates,
    _send_healer_start_alert,
    main,
    run_healer_async,
    run_pbp_healer_async,
)


def _mock_game(game_id: str, status: str = "SCHEDULED", game_date: str = "2025-06-15") -> MagicMock:
    game = MagicMock()
    game.game_id = game_id
    game.game_status = status
    game.game_date = game_date
    game.away_team = "LG"
    game.home_team = "SSG"
    game.away_score = 5
    game.home_score = 3
    return game


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

    def test_pbp_with_lookback_days(self):
        with patch("src.cli.auto_healer.run_pbp_healer") as mock_pbp:
            mock_pbp.return_value = 0
            result = main(["--pbp", "--lookback-days", "7"])
            assert result == 0

    def test_reset_flag(self):
        with patch("src.cli.auto_healer.run_healer_async", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = 0
            result = main(["--reset"])
            mock_run.assert_called_once_with(dry_run=False, reset_checkpoint=True)
            assert result == 0

    def test_no_args(self):
        with patch("src.cli.auto_healer.run_healer_async", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = 0
            result = main([])
            assert result == 0


class TestFindStuckGames:
    def test_returns_empty_when_none(self):
        with patch("src.cli.auto_healer.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalars.return_value.all.return_value = []
            result = _find_stuck_games()
            assert result == []

    def test_returns_games_when_found(self):
        with patch("src.cli.auto_healer.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_game = _mock_game("G1")
            mock_session.execute.return_value.scalars.return_value.all.return_value = [mock_game]
            result = _find_stuck_games()
            assert len(result) == 1
            assert result[0].game_id == "G1"


class TestFindInconsistentGames:
    def test_returns_empty_when_none(self):
        with patch("src.cli.auto_healer.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalars.return_value.all.return_value = []
            result = _find_inconsistent_games()
            assert result == []

    def test_returns_games_when_mismatch(self):
        with patch("src.cli.auto_healer.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalars.return_value.all.return_value = ["G1", "G2"]
            mock_game1 = _mock_game("G1", "COMPLETED")
            mock_game2 = _mock_game("G2", "COMPLETED")
            mock_session.execute.return_value.scalars.return_value.all.side_effect = [
                ["G1", "G2"],
                [mock_game1, mock_game2],
            ]
            result = _find_inconsistent_games()
            assert len(result) == 2


class TestApplyHealOutcome:
    def test_completed_when_detail_saved(self):
        mock_item = MagicMock()
        mock_item.detail_saved = True
        mock_item.failure_reason = None
        result = _apply_heal_outcome("G1", mock_item)
        assert result == "completed"

    def test_cancelled_when_failure_reason(self):
        mock_item = MagicMock()
        mock_item.detail_saved = False
        mock_item.failure_reason = "cancelled"
        with patch("src.cli.auto_healer.update_game_status") as mock_update:
            result = _apply_heal_outcome("G1", mock_item)
        assert result == "cancelled"
        mock_update.assert_called_once()

    def test_unresolved_when_no_item(self):
        with patch("src.cli.auto_healer.update_game_status") as mock_update:
            result = _apply_heal_outcome("G1", None)
        assert result == "unresolved"
        mock_update.assert_called_once()

    def test_unresolved_with_other_reason(self):
        mock_item = MagicMock()
        mock_item.detail_saved = False
        mock_item.failure_reason = "timeout"
        with patch("src.cli.auto_healer.update_game_status"):
            result = _apply_heal_outcome("G1", mock_item)
        assert result == "unresolved"

    def test_unresolved_with_missing_reason(self):
        mock_item = MagicMock()
        mock_item.detail_saved = False
        mock_item.failure_reason = "missing"
        with patch("src.cli.auto_healer.update_game_status") as mock_update:
            result = _apply_heal_outcome("G1", mock_item)
        assert result == "unresolved"
        mock_update.assert_called_once_with("G1", "UNRESOLVED_MISSING")


class TestLogHealerSummary:
    def test_logs_summary_dry_run(self, caplog):
        with caplog.at_level(logging.INFO):
            _log_healer_summary({"fixed": 5, "failed": 1, "completed": 5}, dry_run=True)
        assert "5" in caplog.text

    def test_logs_summary_no_dry_run_success(self, caplog):
        with patch("src.cli.auto_healer.SlackWebhookClient") as mock_slack:
            with caplog.at_level(logging.INFO):
                _log_healer_summary({"completed": 5, "unresolved": 0}, dry_run=False)
        assert "Auto-Healer Summary" in caplog.text
        mock_slack.send_alert.assert_called_once()

    def test_logs_summary_no_dry_run_with_failures(self, caplog):
        with patch("src.cli.auto_healer.SlackWebhookClient") as mock_slack:
            with caplog.at_level(logging.INFO):
                _log_healer_summary({"completed": 3, "unresolved": 2}, dry_run=False)
        mock_slack.send_alert.assert_called_once()
        call_args = mock_slack.send_alert.call_args[0][0]
        assert "2" in call_args


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

    def test_logs_anomaly_dates(self, caplog):
        mock_game = MagicMock()
        mock_game.game_id = "G4"
        mock_game.game_status = "SCHEDULED"

        with caplog.at_level(logging.INFO):
            _log_anomaly_summary(
                all_found=[mock_game],
                inconsistent_games=[],
                pending_ids={"G4"},
                anomaly_dates=["2025-06-15", "2025-06-16"],
            )
        assert "2025-06-15" in caplog.text
        assert "2025-06-16" in caplog.text

    def test_no_stuck_games_not_in_pending(self, caplog):
        mock_game = MagicMock()
        mock_game.game_id = "G5"
        mock_game.game_status = "SCHEDULED"

        with caplog.at_level(logging.WARNING):
            _log_anomaly_summary(
                all_found=[mock_game],
                inconsistent_games=[],
                pending_ids={"OTHER"},
                anomaly_dates=[],
            )
        assert "stuck" not in caplog.text.lower()


class TestSendHealerStartAlert:
    def test_sends_with_stuck_games(self):
        mock_game = MagicMock()
        mock_game.game_id = "G1"
        mock_game.game_status = "SCHEDULED"

        with patch("src.cli.auto_healer.SlackWebhookClient") as mock_slack:
            _send_healer_start_alert(
                total=1,
                stuck_games=[mock_game],
                inconsistent_games=[],
                anomaly_dates=["2025-06-15"],
            )
        mock_slack.send_alert.assert_called_once()
        call_args = mock_slack.send_alert.call_args[0]
        assert "1" in call_args[0] or "1" in str(call_args[1])

    def test_sends_with_inconsistent_games(self):
        mock_game = MagicMock()
        mock_game.game_id = "G2"
        mock_game.game_status = "COMPLETED"

        with patch("src.cli.auto_healer.SlackWebhookClient") as mock_slack:
            _send_healer_start_alert(
                total=1,
                stuck_games=[],
                inconsistent_games=[mock_game],
                anomaly_dates=["2025-06-15"],
            )
        mock_slack.send_alert.assert_called_once()

    def test_sends_with_both(self):
        mock_game = MagicMock()

        with patch("src.cli.auto_healer.SlackWebhookClient") as mock_slack:
            _send_healer_start_alert(
                total=2,
                stuck_games=[mock_game],
                inconsistent_games=[mock_game],
                anomaly_dates=["2025-06-15"],
            )
        mock_slack.send_alert.assert_called_once()

    def test_date_range_format(self):
        mock_game = MagicMock()

        with patch("src.cli.auto_healer.SlackWebhookClient") as mock_slack:
            _send_healer_start_alert(
                total=1,
                stuck_games=[mock_game],
                inconsistent_games=[],
                anomaly_dates=["2025-06-15", "2025-06-16"],
            )
        mock_slack.send_alert.assert_called_once()
        blocks = mock_slack.send_alert.call_args[1].get("blocks", [])
        if blocks:
            text = str(blocks[1])
            assert "~" in text or "2025-06-15" in text


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

    def test_with_stuck_games(self):
        mock_game = _mock_game("G1")
        with patch("src.cli.auto_healer._find_stuck_games", return_value=[mock_game]):
            with patch("src.cli.auto_healer._find_inconsistent_games", return_value=[]):
                all_found, stuck, inconsistent = _find_recovery_targets(None)

        assert len(all_found) == 1
        assert len(stuck) == 1
        assert inconsistent == []

    def test_with_inconsistent_games(self):
        mock_game = _mock_game("G1", "COMPLETED")
        with patch("src.cli.auto_healer._find_stuck_games", return_value=[]):
            with patch("src.cli.auto_healer._find_inconsistent_games", return_value=[mock_game]):
                all_found, stuck, inconsistent = _find_recovery_targets(None)

        assert len(all_found) == 1
        assert stuck == []
        assert len(inconsistent) == 1

    def test_deduplicates_games(self):
        mock_game = _mock_game("G1")
        with patch("src.cli.auto_healer._find_stuck_games", return_value=[mock_game]):
            with patch("src.cli.auto_healer._find_inconsistent_games", return_value=[mock_game]):
                all_found, stuck, inconsistent = _find_recovery_targets(None)

        assert len(all_found) == 1


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

    def test_all_already_processed(self):
        mock_game = MagicMock()
        mock_game.game_id = "G1"

        mock_mgr = MagicMock()
        mock_mgr.initialize_run.return_value = None
        mock_mgr.get_pending_targets.return_value = []

        pending_ids, candidates = _pending_recovery_candidates(mock_mgr, [mock_game])
        assert pending_ids == set()
        assert candidates == []


class TestFindUnverifiedPbpGames:
    def test_returns_empty_when_none(self):
        with patch("src.cli.auto_healer.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.fetchall.return_value = []
            result = _find_unverified_pbp_games(lookback_days=3)
            assert result == []

    def test_returns_games_with_unverified_pbp(self):
        with patch("src.cli.auto_healer.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_row = MagicMock()
            mock_row.game_id = "20250615LGSS0"
            mock_row.game_date = "2025-06-15"
            mock_row.away_team = "LG"
            mock_row.home_team = "SSG"
            mock_row.source_payload = json.dumps(
                {"pbp_validation_status": "unverified", "pbp_validation_error": "missing innings"}
            )
            mock_session.execute.return_value.fetchall.return_value = [mock_row]
            result = _find_unverified_pbp_games(lookback_days=3)
            assert len(result) == 1
            assert result[0]["game_id"] == "20250615LGSS0"
            assert result[0]["error_reason"] == "missing innings"

    def test_handles_string_payload(self):
        with patch("src.cli.auto_healer.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_row = MagicMock()
            mock_row.game_id = "G1"
            mock_row.game_date = "2025-06-15"
            mock_row.away_team = "LG"
            mock_row.home_team = "SSG"
            mock_row.source_payload = json.dumps({"pbp_validation_status": "unverified"})
            mock_session.execute.return_value.fetchall.return_value = [mock_row]
            result = _find_unverified_pbp_games(lookback_days=3)
            assert len(result) == 1
            assert result[0]["error_reason"] == "unknown"

    def test_handles_null_teams(self):
        with patch("src.cli.auto_healer.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_row = MagicMock()
            mock_row.game_id = "G1"
            mock_row.game_date = "2025-06-15"
            mock_row.away_team = None
            mock_row.home_team = None
            mock_row.source_payload = json.dumps({"pbp_validation_status": "unverified"})
            mock_session.execute.return_value.fetchall.return_value = [mock_row]
            result = _find_unverified_pbp_games(lookback_days=3)
            assert result[0]["away_team"] == "?"
            assert result[0]["home_team"] == "?"

    def test_handles_invalid_json_payload(self):
        with patch("src.cli.auto_healer.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_row = MagicMock()
            mock_row.game_id = "G1"
            mock_row.game_date = "2025-06-15"
            mock_row.away_team = "LG"
            mock_row.home_team = "SSG"
            mock_row.source_payload = "not valid json"
            mock_session.execute.return_value.fetchall.return_value = [mock_row]
            result = _find_unverified_pbp_games(lookback_days=3)
            assert len(result) == 1
            assert result[0]["error_reason"] == "unknown"


class TestRunHealerAsync:
    async def test_returns_zero_when_no_anomalies(self):
        with (
            patch("src.cli.auto_healer._find_stuck_games") as mock_stuck,
            patch("src.cli.auto_healer._find_inconsistent_games") as mock_incon,
            patch("src.cli.auto_healer.RecoveryManager") as mock_mgr_cls,
        ):
            mock_stuck.return_value = []
            mock_incon.return_value = []
            mock_mgr = MagicMock()
            mock_mgr_cls.return_value = mock_mgr
            result = await run_healer_async(dry_run=True)
            assert result == 0
            mock_mgr.clear.assert_called_once()

    async def test_returns_zero_when_all_processed(self):
        mock_game = _mock_game("G1")
        with (
            patch("src.cli.auto_healer._find_stuck_games", return_value=[mock_game]),
            patch("src.cli.auto_healer._find_inconsistent_games", return_value=[]),
            patch("src.cli.auto_healer.RecoveryManager") as mock_mgr_cls,
        ):
            mock_mgr = MagicMock()
            mock_mgr_cls.return_value = mock_mgr
            mock_mgr.get_pending_targets.return_value = []
            result = await run_healer_async(dry_run=True)
            assert result == 0

    async def test_dry_run_counts_candidates(self):
        mock_game = _mock_game("G1")
        with (
            patch("src.cli.auto_healer._find_stuck_games", return_value=[mock_game]),
            patch("src.cli.auto_healer._find_inconsistent_games", return_value=[]),
            patch("src.cli.auto_healer.RecoveryManager") as mock_mgr_cls,
            patch("src.cli.auto_healer._run_recovery", new_callable=AsyncMock) as mock_recovery,
        ):
            mock_mgr = MagicMock()
            mock_mgr_cls.return_value = mock_mgr
            mock_mgr.get_pending_targets.return_value = ["G1"]
            mock_recovery.return_value = {"dry_run": 1, "unresolved": 0}
            result = await run_healer_async(dry_run=True)
            assert result == 0

    async def test_reset_checkpoint_clears(self):
        with (
            patch("src.cli.auto_healer._find_stuck_games", return_value=[]),
            patch("src.cli.auto_healer._find_inconsistent_games", return_value=[]),
            patch("src.cli.auto_healer.RecoveryManager") as mock_mgr_cls,
        ):
            mock_mgr = MagicMock()
            mock_mgr_cls.return_value = mock_mgr
            await run_healer_async(dry_run=True, reset_checkpoint=True)
            mock_mgr.clear.assert_called()

    async def test_with_target_game_ids(self):
        with patch("src.cli.auto_healer.RecoveryManager") as mock_mgr_cls:
            mock_mgr = MagicMock()
            mock_mgr_cls.return_value = mock_mgr
            mock_mgr.get_pending_targets.return_value = []
            result = await run_healer_async(dry_run=True, target_game_ids=["G1"])
            assert result == 0


class TestRunPbpHealerAsync:
    async def test_returns_zero_when_no_games(self):
        with patch("src.cli.auto_healer._find_unverified_pbp_games", return_value=[]):
            result = await run_pbp_healer_async(dry_run=False)
            assert result == {"found": 0, "recovered": 0, "failed": 0, "skipped": 0}

    async def test_dry_run_returns_found(self):
        with patch(
            "src.cli.auto_healer._find_unverified_pbp_games",
            return_value=[{"game_id": "G1", "away_team": "LG", "home_team": "SSG", "error_reason": "x"}],
        ):
            with patch("src.cli.auto_healer.TelegramBotClient"):
                result = await run_pbp_healer_async(dry_run=True)
                assert result["found"] == 1
                assert result["skipped"] == 1

    async def test_targeted_mode(self):
        with patch("src.cli.auto_healer.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_row = MagicMock()
            mock_row.game_id = "20250615LGSS0"
            mock_row.game_date = "2025-06-15"
            mock_row.away_team = "LG"
            mock_row.home_team = "SSG"
            mock_row.source_payload = None
            mock_session.execute.return_value.fetchall.return_value = [mock_row]

            with patch("src.cli.auto_healer.TelegramBotClient") as mock_tg:
                with patch(
                    "src.services.relay_recovery_service.recover_relay_data", new_callable=AsyncMock
                ) as mock_recover:
                    mock_recovery_result = MagicMock()
                    mock_recovery_result.saved_games = 1
                    mock_recovery_result.report_rows = [{"game_id": "20250615LGSS0", "status": "saved"}]
                    mock_recover.return_value = mock_recovery_result
                    result = await run_pbp_healer_async(dry_run=False, target_game_ids=["20250615LGSS0"])
                    assert result["found"] == 1
                    assert result["recovered"] == 1
                    mock_tg.send_message.assert_called()

    async def test_partial_recovery(self):
        with patch(
            "src.cli.auto_healer._find_unverified_pbp_games",
            return_value=[
                {
                    "game_id": "G1",
                    "game_date": "2025-06-15",
                    "away_team": "LG",
                    "home_team": "SSG",
                    "error_reason": "x",
                },
                {"game_id": "G2", "game_date": "2025-06-15", "away_team": "KT", "home_team": "NC", "error_reason": "y"},
            ],
        ):
            with patch("src.cli.auto_healer.TelegramBotClient"):
                with patch(
                    "src.services.relay_recovery_service.recover_relay_data", new_callable=AsyncMock
                ) as mock_recover:
                    mock_recovery_result = MagicMock()
                    mock_recovery_result.saved_games = 1
                    mock_recovery_result.report_rows = [{"game_id": "G1", "status": "saved"}]
                    mock_recover.return_value = mock_recovery_result
                    with patch("src.sources.relay.derive_bucket_id", return_value="bucket"):
                        result = await run_pbp_healer_async(dry_run=False)
                        assert result["found"] == 2
                        assert result["recovered"] == 1
                        assert result["failed"] == 1
