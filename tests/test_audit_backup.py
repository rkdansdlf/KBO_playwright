import json
from datetime import date, datetime
from pathlib import Path

import pytest

from src.utils.fallback_monitor import FallbackMonitor


@pytest.fixture
def clean_audit_dir():
    """Ensure the audit_fixes log directory is clean before/after tests."""
    project_root = Path(__file__).resolve().parent.parent
    audit_dir = project_root / "logs" / "audit_fixes"

    # We back up existing logs if they exist, or just clear them
    backup_existing = []
    if audit_dir.exists():
        for item in audit_dir.glob("*.json"):
            backup_existing.append((item, item.read_text(encoding="utf-8")))
            item.unlink()

    yield audit_dir

    # Clean up test files
    if audit_dir.exists():
        for item in audit_dir.glob("*.json"):
            item.unlink()

    # Restore original files
    if backup_existing:
        audit_dir.mkdir(parents=True, exist_ok=True)
        for path, content in backup_existing:
            path.write_text(content, encoding="utf-8")


def test_save_audit_backup_creates_file_and_serializes_dates(clean_audit_dir):
    player_id = "test_player_123"
    type_name = "batting"

    original_data = {
        "player_id": 123,
        "team_code": "OB",
        "avg": 0.312,
        "created_at": datetime(2026, 5, 20, 10, 0, 0),
        "updated_at": date(2026, 5, 20),
    }

    calculated_data = {"player_id": 123, "team_code": "OB", "avg": 0.315}

    # 1. Trigger backup
    file_path_str = FallbackMonitor.save_audit_backup(
        player_id=player_id, type_name=type_name, original_data=original_data, calculated_data=calculated_data
    )

    file_path = Path(file_path_str)
    assert file_path.exists()

    # Validate expected filename format: {date}_{player_id}_{type}.json
    date_str = datetime.now().strftime("%Y%m%d")
    expected_filename = f"{date_str}_{player_id}_{type_name}.json"
    assert file_path.name == expected_filename

    # 2. Read and parse the written file
    with file_path.open(encoding="utf-8") as f:
        data = json.load(f)

    assert isinstance(data, list)
    assert len(data) == 1

    snapshot = data[0]
    assert snapshot["player_id"] == player_id
    assert snapshot["type"] == type_name
    assert "timestamp" in snapshot

    # Verify date and datetime objects were serialized to ISO format strings
    assert snapshot["original"]["created_at"] == "2026-05-20T10:00:00"
    assert snapshot["original"]["updated_at"] == "2026-05-20"
    assert snapshot["original"]["avg"] == 0.312
    assert snapshot["calculated"]["avg"] == 0.315


def test_save_audit_backup_appends_to_existing_file(clean_audit_dir):
    player_id = "test_player_456"
    type_name = "fielding"

    original_1 = {"position_id": "C", "errors": 2}
    calc_1 = {"position_id": "C", "errors": 1}

    original_2 = {"position_id": "1B", "errors": 0}
    calc_2 = {"position_id": "1B", "errors": 0}

    # Save first snapshot
    file_path_str_1 = FallbackMonitor.save_audit_backup(
        player_id=player_id, type_name=type_name, original_data=original_1, calculated_data=calc_1
    )

    # Save second snapshot for the same player, type, and day
    file_path_str_2 = FallbackMonitor.save_audit_backup(
        player_id=player_id, type_name=type_name, original_data=original_2, calculated_data=calc_2
    )

    assert file_path_str_1 == file_path_str_2
    file_path = Path(file_path_str_1)

    # Read file and verify both snapshots are saved in list
    with file_path.open(encoding="utf-8") as f:
        data = json.load(f)

    assert isinstance(data, list)
    assert len(data) == 2

    assert data[0]["original"]["position_id"] == "C"
    assert data[0]["calculated"]["errors"] == 1

    assert data[1]["original"]["position_id"] == "1B"
    assert data[1]["calculated"]["errors"] == 0
