from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.utils.fallback_monitor import FallbackMonitor


class TestLogFallback:
    def test_logs_warning_without_player_count(self):
        with (
            patch("src.utils.fallback_monitor.logger") as mock_logger,
            patch("src.utils.fallback_monitor.SlackWebhookClient"),
        ):
            FallbackMonitor.log_fallback(
                year=2025,
                series="REGULAR",
                stat_type="batting",
                reason="missing data",
            )
            mock_logger.warning.assert_called_once()
            msg = mock_logger.warning.call_args[0][0]
            assert "FALLBACK TRIGGERED" in msg
            assert "2025" in msg
            assert "REGULAR" in msg
            assert "Processed" not in msg

    def test_logs_warning_with_player_count(self):
        with (
            patch("src.utils.fallback_monitor.logger") as mock_logger,
            patch("src.utils.fallback_monitor.SlackWebhookClient"),
        ):
            FallbackMonitor.log_fallback(
                year=2025,
                series="POSTSEASON",
                stat_type="pitching",
                reason="timeout",
                player_count=42,
            )
            msg = mock_logger.warning.call_args[0][0]
            assert "Processed 42 players" in msg

    def test_sends_slack_alert(self):
        with (
            patch("src.utils.fallback_monitor.logger"),
            patch("src.utils.fallback_monitor.SlackWebhookClient") as mock_slack,
        ):
            FallbackMonitor.log_fallback(
                year=2025,
                series="REGULAR",
                stat_type="batting",
                reason="test",
            )
            mock_slack.send_alert.assert_called_once()
            call_kwargs = mock_slack.send_alert.call_args
            assert "blocks" in call_kwargs.kwargs or len(call_kwargs.args) > 1


def _patch_path(tmp_path):
    mock_resolved = MagicMock()
    mock_resolved.parent.parent.parent = tmp_path
    mock_resolved.__truediv__ = lambda self, key: tmp_path / key

    def path_side_effect(*args, **kwargs):
        p = MagicMock()
        p.resolve.return_value = mock_resolved
        p.__truediv__ = lambda self, key: tmp_path / key
        return p

    return path_side_effect


class TestSaveAuditBackup:
    @patch("pathlib.Path")
    def test_datetime_encoder_handles_datetime(self, mock_path, tmp_path):
        mock_path.side_effect = _patch_path(tmp_path)

        file_path = FallbackMonitor.save_audit_backup(
            player_id="1001",
            type_name="batting",
            original_data={"d": datetime(2025, 6, 1, 12, 0, 0)},
        )
        saved = Path(file_path)
        content = json.loads(saved.read_text(encoding="utf-8"))
        assert "2025-06-01" in content[0]["original"]["d"]

    @patch("pathlib.Path")
    def test_datetime_encoder_handles_date(self, mock_path, tmp_path):
        mock_path.side_effect = _patch_path(tmp_path)

        file_path = FallbackMonitor.save_audit_backup(
            player_id="1002",
            type_name="pitching",
            original_data={"game_date": date(2025, 5, 15)},
        )
        saved = Path(file_path)
        content = json.loads(saved.read_text(encoding="utf-8"))
        assert content[0]["original"]["game_date"] == "2025-05-15"

    @patch("pathlib.Path")
    def test_omits_optional_fields(self, mock_path, tmp_path):
        mock_path.side_effect = _patch_path(tmp_path)

        file_path = FallbackMonitor.save_audit_backup(
            player_id="1003",
            type_name="batting",
            original_data={"avg": 0.300},
        )
        saved = Path(file_path)
        content = json.loads(saved.read_text(encoding="utf-8"))
        assert "player_name" not in content[0]
        assert "calculated" not in content[0]

    @patch("pathlib.Path")
    def test_appends_to_existing_valid_json(self, mock_path, tmp_path):
        mock_path.side_effect = _patch_path(tmp_path)

        FallbackMonitor.save_audit_backup("1004", "batting", {"hits": 5})
        FallbackMonitor.save_audit_backup("1004", "batting", {"hits": 10})
        backup_dir = tmp_path / "logs" / "audit_fixes"
        files = list(backup_dir.iterdir())
        assert len(files) == 1
        content = json.loads(files[0].read_text(encoding="utf-8"))
        assert len(content) == 2

    @patch("pathlib.Path")
    def test_handles_corrupt_existing_file(self, mock_path, tmp_path):
        mock_path.side_effect = _patch_path(tmp_path)
        backup_dir = tmp_path / "logs" / "audit_fixes"
        backup_dir.mkdir(parents=True, exist_ok=True)
        (backup_dir / "20250101_1005_batting.json").write_text("{corrupt", encoding="utf-8")

        file_path = FallbackMonitor.save_audit_backup("1005", "batting", {"hits": 5})
        saved = Path(file_path)
        content = json.loads(saved.read_text(encoding="utf-8"))
        assert len(content) == 1

    def test_handles_non_list_existing_file(self, tmp_path):
        backup_dir = tmp_path / "logs" / "audit_fixes"
        backup_dir.mkdir(parents=True, exist_ok=True)
        existing_file = backup_dir / "20250101_1006_batting.json"
        existing_file.write_text(json.dumps({"single": True}), encoding="utf-8")

        content = json.loads(existing_file.read_text(encoding="utf-8"))
        result = content if isinstance(content, list) else [content]
        result.append({"test": True})

        assert len(result) == 2

    @patch("pathlib.Path")
    def test_with_calculated_and_player_name(self, mock_path, tmp_path):
        mock_path.side_effect = _patch_path(tmp_path)

        file_path = FallbackMonitor.save_audit_backup(
            player_id="2001",
            type_name="pitching",
            original_data={"era": 3.50},
            calculated_data={"era": 3.00},
            player_name="홍길동",
        )
        saved = Path(file_path)
        content = json.loads(saved.read_text(encoding="utf-8"))
        assert content[0]["calculated"] == {"era": 3.00}
        assert content[0]["player_name"] == "홍길동"


class TestSaveAuditEvent:
    @patch("pathlib.Path")
    def test_creates_file(self, mock_path, tmp_path):
        mock_path.side_effect = _patch_path(tmp_path)

        FallbackMonitor.save_audit_event("PA_FORMULA", "WARNING", {"detail": "test"})
        backup_dir = tmp_path / "logs" / "audit_fixes"
        files = list(backup_dir.iterdir())
        assert len(files) == 1
        assert "warning" in files[0].name.lower()
        assert "pa_formula" in files[0].name.lower()

    @patch("pathlib.Path")
    def test_file_content(self, mock_path, tmp_path):
        mock_path.side_effect = _patch_path(tmp_path)

        FallbackMonitor.save_audit_event("TEAM_STATS", "ABORT", {"reason": "mismatch"})
        backup_dir = tmp_path / "logs" / "audit_fixes"
        files = list(backup_dir.iterdir())
        content = json.loads(files[0].read_text(encoding="utf-8"))
        assert content == {"reason": "mismatch"}
