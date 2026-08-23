"""Integration tests for Pipeline API Router."""

from __future__ import annotations

from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from src.api.app import app
from src.api.auth import get_api_key
from src.api.dependencies import (
    get_auto_healer,
    get_pipeline_detector,
    get_pipeline_orchestrator,
    get_quality_hub,
)
from src.pipeline.dto import (
    DefectItem,
    DefectReport,
    HealingActionSummary,
    PipelineDefectType,
    PipelineRunSummary,
    PipelineStageResult,
)
from src.services.quality_hub import UnifiedQualityReport

client = TestClient(app)


def test_get_pipeline_defects() -> None:
    mock_detector = MagicMock()
    mock_detector.detect_all.return_value = DefectReport(
        target_date="2025-06-15",
        defects=[
            DefectItem(
                game_id="20250615LGSS0",
                defect_type=PipelineDefectType.STUCK_SCHEDULED,
                severity="ERROR",
                description="Game stuck",
            )
        ],
        timestamp="2025-06-16T00:00:00",
    )

    app.dependency_overrides[get_api_key] = lambda: "test_key"
    app.dependency_overrides[get_pipeline_detector] = lambda: mock_detector

    try:
        response = client.get("/api/pipeline/defects?target_date=2025-06-15")
        assert response.status_code == 200
        data = response.json()
        assert data["target_date"] == "2025-06-15"
        assert data["total_defects"] == 1
        assert data["defects"][0]["game_id"] == "20250615LGSS0"
    finally:
        app.dependency_overrides.clear()


def test_execute_pipeline_healing() -> None:
    mock_healer = MagicMock()
    mock_healer.heal_stuck_game.return_value = HealingActionSummary(
        game_id="20250615LGSS0",
        action_taken="update_status_to_COMPLETED",
        status="SUCCESS",
        elapsed_seconds=0.04,
    )

    app.dependency_overrides[get_api_key] = lambda: "test_key"
    app.dependency_overrides[get_auto_healer] = lambda: mock_healer

    try:
        response = client.post("/api/pipeline/heal", json={"game_id": "20250615LGSS0"})
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["game_id"] == "20250615LGSS0"
        assert data[0]["status"] == "SUCCESS"
    finally:
        app.dependency_overrides.clear()


def test_get_quality_hub_report() -> None:
    mock_qhub = MagicMock()
    mock_qhub.run_full_audit.return_value = UnifiedQualityReport(
        timestamp="2025-06-16T00:00:00",
        overall_status="PASS",
        quality_score=98,
        remediation_hints=["Hint 1"],
    )

    app.dependency_overrides[get_api_key] = lambda: "test_key"
    app.dependency_overrides[get_quality_hub] = lambda: mock_qhub

    try:
        response = client.get("/api/pipeline/quality?season=2025")
        assert response.status_code == 200
        data = response.json()
        assert data["overall_status"] == "PASS"
        assert data["quality_score"] == 98
        assert data["remediation_hints"] == ["Hint 1"]
    finally:
        app.dependency_overrides.clear()


def test_run_daily_pipeline() -> None:
    mock_orchestrator = MagicMock()
    mock_orchestrator.run_pipeline.return_value = PipelineRunSummary(
        run_id="pipe_123",
        target_date="2025-06-15",
        overall_status="SUCCESS",
        total_duration_seconds=3.2,
        timestamp="2025-06-16T00:00:00",
        stages=[PipelineStageResult(stage_name="Stage 1", status="SUCCESS")],
    )

    app.dependency_overrides[get_api_key] = lambda: "test_key"
    app.dependency_overrides[get_pipeline_orchestrator] = lambda: mock_orchestrator

    try:
        response = client.post("/api/pipeline/run", json={"target_date": "2025-06-15"})
        assert response.status_code == 200
        data = response.json()
        assert data["run_id"] == "pipe_123"
        assert data["overall_status"] == "SUCCESS"
        assert len(data["stages"]) == 1
    finally:
        app.dependency_overrides.clear()
