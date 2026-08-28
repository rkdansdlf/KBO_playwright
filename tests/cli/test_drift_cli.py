"""Tests for Schema Drift CLI and Master CLI routing."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from src.cli.detect_schema_drift import main as drift_main
from src.cli.kbo import main as kbo_main
from src.db.drift_dto import DriftSeverity, DriftType, SchemaDriftItem, SchemaDriftReport


def test_drift_cli_json_output(capsys) -> None:
    """Test schema drift CLI JSON output."""
    mock_report = SchemaDriftReport(
        dialect="oracle",
        database_url="oracle+oracledb://mock",
        total_tables_checked=10,
        drift_count=1,
        drifts=[
            SchemaDriftItem(
                drift_type=DriftType.MISSING_COLUMN,
                table_name="kbo_games",
                object_name="weather",
                expected="VARCHAR2(50)",
                actual="MISSING",
                severity=DriftSeverity.MEDIUM,
                ddl_statement="ALTER TABLE kbo_games ADD (weather VARCHAR2(50));",
            )
        ],
        generated_ddl=["ALTER TABLE kbo_games ADD (weather VARCHAR2(50));"],
        is_synced=False,
    )

    with patch("src.cli.detect_schema_drift.SchemaDriftDetector") as mock_cls:
        instance = MagicMock()
        instance.detect_drift.return_value = mock_report
        mock_cls.return_value = instance

        exit_code = drift_main(["--dialect", "oracle", "--json"])
        assert exit_code == 0

        captured = capsys.readouterr().out
        data = json.loads(captured)
        assert data["dialect"] == "oracle"
        assert data["drift_count"] == 1
        assert data["is_synced"] is False


def test_drift_cli_strict_exit_code() -> None:
    """Test strict mode returns exit code 1 when drift is detected."""
    mock_report = SchemaDriftReport(
        dialect="sqlite",
        database_url="sqlite:///:memory:",
        total_tables_checked=5,
        drift_count=2,
        is_synced=False,
    )

    with patch("src.cli.detect_schema_drift.SchemaDriftDetector") as mock_cls:
        instance = MagicMock()
        instance.detect_drift.return_value = mock_report
        mock_cls.return_value = instance

        exit_code = drift_main(["--strict"])
        assert exit_code == 1


def test_kbo_master_cli_drift_dispatch() -> None:
    """Test Master CLI routing kbo drift and kbo detect-drift."""
    with patch("src.cli.detect_schema_drift.main", return_value=0) as mock_main:
        exit_code = kbo_main(["drift", "--dialect", "oracle"])
        assert exit_code == 0
        mock_main.assert_called_once_with(["--dialect", "oracle"])

    with patch("src.cli.detect_schema_drift.main", return_value=0) as mock_main:
        exit_code = kbo_main(["detect-drift", "--strict"])
        assert exit_code == 0
        mock_main.assert_called_once_with(["--strict"])
