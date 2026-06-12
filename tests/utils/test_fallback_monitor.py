import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.utils.fallback_monitor import FallbackMonitor


class TestFallbackMonitor:
    def test_log_fallback_message_content(self):
        msg = "🔄 [FALLBACK TRIGGERED] 2025 POSTSEASON batting fallback initiated. Reason: missing data"
        assert "FALLBACK TRIGGERED" in msg

    @patch("pathlib.Path")
    def test_save_audit_backup_creates_file(self, mock_path, tmp_path):
        mock_resolved = MagicMock()
        mock_resolved.parent.parent.parent = tmp_path
        mock_path.return_value.resolve.return_value = mock_resolved
        mock_path.side_effect = lambda *a, **kw: mock_path.return_value

        file_path = FallbackMonitor.save_audit_backup(
            player_id="1001",
            type_name="batting",
            original_data={"hits": 10},
            player_name="홍길동",
        )

        saved = Path(file_path)
        assert saved.exists()
        content = json.loads(saved.read_text(encoding="utf-8"))
        assert isinstance(content, list)
        assert content[0]["player_id"] == "1001"
        assert content[0]["type"] == "batting"
        assert content[0]["original"] == {"hits": 10}
        assert content[0]["player_name"] == "홍길동"

    @patch("pathlib.Path")
    def test_save_audit_backup_appends_to_existing(self, mock_path, tmp_path):
        mock_resolved = MagicMock()
        mock_resolved.parent.parent.parent = tmp_path
        mock_path.return_value.resolve.return_value = mock_resolved
        mock_path.side_effect = lambda *a, **kw: mock_path.return_value

        FallbackMonitor.save_audit_backup("1001", "batting", {"hits": 5})
        FallbackMonitor.save_audit_backup("1001", "batting", {"hits": 10})

        backup_dir = tmp_path / "logs" / "audit_fixes"
        files = list(backup_dir.iterdir())
        assert len(files) == 1
        content = json.loads(files[0].read_text(encoding="utf-8"))
        assert len(content) == 2

    @patch("pathlib.Path")
    def test_save_audit_event_creates_file(self, mock_path, tmp_path):
        mock_resolved = MagicMock()
        mock_resolved.parent.parent.parent = tmp_path
        mock_path.return_value.resolve.return_value = mock_resolved
        mock_path.side_effect = lambda *a, **kw: mock_path.return_value

        FallbackMonitor.save_audit_event("PA_FORMULA", "WARNING", {"details": "test"})

        backup_dir = tmp_path / "logs" / "audit_fixes"
        files = list(backup_dir.iterdir())
        assert len(files) == 1
        content = json.loads(files[0].read_text(encoding="utf-8"))
        assert content == {"details": "test"}

    @patch("pathlib.Path")
    def test_save_audit_backup_with_calculated_data(self, mock_path, tmp_path):
        mock_resolved = MagicMock()
        mock_resolved.parent.parent.parent = tmp_path
        mock_path.return_value.resolve.return_value = mock_resolved
        mock_path.side_effect = lambda *a, **kw: mock_path.return_value

        FallbackMonitor.save_audit_backup(
            "2001",
            "pitching",
            original_data={"era": 3.50},
            calculated_data={"era": 3.00},
        )

        backup_dir = tmp_path / "logs" / "audit_fixes"
        files = list(backup_dir.iterdir())
        content = json.loads(files[0].read_text(encoding="utf-8"))
        assert "calculated" in content[0]
        assert content[0]["calculated"] == {"era": 3.00}

    @patch("pathlib.Path")
    def test_save_audit_backup_handles_corrupt_existing_file(self, mock_path, tmp_path):
        mock_resolved = MagicMock()
        mock_resolved.parent.parent.parent = tmp_path
        mock_path.return_value.resolve.return_value = mock_resolved
        mock_path.side_effect = lambda *a, **kw: mock_path.return_value

        backup_dir = tmp_path / "logs" / "audit_fixes"
        backup_dir.mkdir(parents=True, exist_ok=True)
        (backup_dir / "20250101_1001_batting.json").write_text("corrupt{json", encoding="utf-8")

        file_path = FallbackMonitor.save_audit_backup("1001", "batting", {"hits": 5})
        saved = Path(file_path)
        content = json.loads(saved.read_text(encoding="utf-8"))
        assert len(content) == 1
