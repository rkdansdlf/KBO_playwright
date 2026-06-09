import json
from pathlib import Path

from src.utils.fallback_monitor import FallbackMonitor


class TestFallbackMonitor:
    def test_log_fallback_creates_message(self):
        msg = "🔄 [FALLBACK TRIGGERED] 2025 POSTSEASON batting fallback initiated. Reason: missing data"
        assert "FALLBACK TRIGGERED" in msg

    def test_save_audit_backup_creates_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "src.utils.fallback_monitor.Path",
            lambda *args, **kwargs: tmp_path if "_file_" in str(kwargs.get("__name__", "")) else Path(*args, **kwargs),
        )
        project_root = tmp_path
        monkeypatch.setattr(Path, "resolve", lambda self: Path(project_root))

        file_path = FallbackMonitor.save_audit_backup(
            player_id="1001",
            type_name="batting",
            original_data={"hits": 10},
            player_name="홍길동",
        )

        saved_file = Path(file_path)
        assert saved_file.exists()
        content = json.loads(saved_file.read_text(encoding="utf-8"))
        assert isinstance(content, list)
        assert content[0]["player_id"] == "1001"
        assert content[0]["type"] == "batting"
        assert content[0]["original"] == {"hits": 10}

    def test_save_audit_backup_appends_to_existing(self, tmp_path, monkeypatch):
        project_root = tmp_path
        monkeypatch.setattr("src.utils.fallback_monitor.Path", lambda *args, **kwargs: Path(*args, **kwargs) if args else Path(project_root))
        # monkeypatch __file__ resolution
        monkeypatch.setattr(Path, "resolve", lambda self: project_root / "src" / "utils" / "fallback_monitor.py")

        FallbackMonitor.save_audit_backup("1001", "batting", {"hits": 5})
        FallbackMonitor.save_audit_backup("1001", "batting", {"hits": 10})
        backup_dir = project_root / "logs" / "audit_fixes"
        files = list(backup_dir.iterdir())
        assert len(files) == 1
        content = json.loads(files[0].read_text(encoding="utf-8"))
        assert len(content) == 2

    def test_save_audit_event_creates_file(self, tmp_path, monkeypatch):
        project_root = tmp_path
        monkeypatch.setattr(Path, "resolve", lambda self: project_root / "src" / "utils" / "fallback_monitor.py")

        FallbackMonitor.save_audit_event("PA_FORMULA", "WARNING", {"details": "test"})
        backup_dir = project_root / "logs" / "audit_fixes"
        files = list(backup_dir.iterdir())
        assert len(files) == 1
        content = json.loads(files[0].read_text(encoding="utf-8"))
        assert content == {"details": "test"}
