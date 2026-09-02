"""Unit and integration tests for src.cli.run_workflow and MasterWorkflowOrchestrator."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from src.cli.run_workflow import build_arg_parser, main

if TYPE_CHECKING:
    import pytest


def test_build_arg_parser() -> None:
    """Test argument parser for workflow subcommands."""
    parser = build_arg_parser()
    args = parser.parse_args(["--workflow", "daily_sync", "--date", "20260401", "--dry-run", "--json"])
    assert args.workflow == "daily_sync"
    assert args.date == "20260401"
    assert args.dry_run is True
    assert args.json is True


def test_main_cli_execution_json(capsys: pytest.CaptureFixture[str]) -> None:
    """Test daily_sync master workflow execution in dry-run mode with JSON output."""
    exit_code = main(["--workflow", "daily_sync", "--dry-run", "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert data["total_stages"] == 6
    assert data["overall_status"] == "SUCCESS"


def test_main_cli_historical_recovery_dry_run(capsys: pytest.CaptureFixture[str]) -> None:
    """Test historical_recovery master workflow execution in dry-run mode."""
    exit_code = main(["--workflow", "historical_recovery", "--dry-run", "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert data["total_stages"] == 4
    assert data["overall_status"] == "SUCCESS"
    stage_ids = [s["stage_id"] for s in data["stage_results"]]
    assert stage_ids == ["hist_scan", "hist_parse", "hist_audit", "hist_sync"]


def test_main_cli_bulk_load_dry_run(capsys: pytest.CaptureFixture[str]) -> None:
    """Test bulk_load master workflow execution in dry-run mode."""
    exit_code = main(["--workflow", "bulk_load", "--dry-run", "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert data["total_stages"] == 4
    assert data["overall_status"] == "SUCCESS"
    stage_ids = [s["stage_id"] for s in data["stage_results"]]
    assert stage_ids == ["bulk_manifest", "bulk_ingest", "bulk_audit", "bulk_sync"]


def test_main_cli_text_output_formatting(capsys: pytest.CaptureFixture[str]) -> None:
    """Test master workflow execution human-readable text output."""
    exit_code = main(["--workflow", "daily_sync", "--dry-run"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "=== Master Workflow Execution [DRY-RUN]" in captured.out
    assert "Overall Status: SUCCESS" in captured.out
    assert "Stage 'ingestion" in captured.out
