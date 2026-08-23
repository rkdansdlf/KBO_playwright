"""Unit tests for src.pipeline.dto."""

from __future__ import annotations

from src.pipeline.dto import (
    DefectItem,
    DefectReport,
    HealingActionSummary,
    PipelineDefectType,
    PipelineRunSummary,
    PipelineStageResult,
)


def test_defect_item_and_report_to_dict() -> None:
    item1 = DefectItem(
        game_id="20250615LGSS0",
        defect_type=PipelineDefectType.STUCK_SCHEDULED,
        severity="ERROR",
        description="Game stuck in scheduled",
        details={"home_team": "SS", "away_team": "LG"},
    )
    item2 = DefectItem(
        game_id="20250615KIOB0",
        defect_type=PipelineDefectType.SCORE_MISMATCH,
        severity="ERROR",
        description="Board score mismatch",
    )

    report = DefectReport(
        target_date="2025-06-15",
        defects=[item1, item2],
        timestamp="2025-06-16T04:00:00",
    )

    assert report.total_defects == 2
    assert report.has_defects is True
    summary_map = report.summary_by_type
    assert summary_map[PipelineDefectType.STUCK_SCHEDULED.value] == 1
    assert summary_map[PipelineDefectType.SCORE_MISMATCH.value] == 1

    data = report.to_dict()
    assert data["target_date"] == "2025-06-15"
    assert data["total_defects"] == 2
    assert len(data["defects"]) == 2
    assert data["defects"][0]["game_id"] == "20250615LGSS0"


def test_healing_action_summary_to_dict() -> None:
    heal = HealingActionSummary(
        game_id="20250615LGSS0",
        action_taken="update_status_to_COMPLETED",
        status="SUCCESS",
        elapsed_seconds=0.1234,
        details={"previous": "SCHEDULED", "new": "COMPLETED"},
    )
    data = heal.to_dict()
    assert data["game_id"] == "20250615LGSS0"
    assert data["status"] == "SUCCESS"
    assert data["elapsed_seconds"] == 0.12


def test_pipeline_run_summary_to_dict() -> None:
    stage1 = PipelineStageResult(
        stage_name="Stage 1: Finalize",
        status="SUCCESS",
        duration_seconds=1.5,
        metrics={"updated_games": 5},
    )
    stage2 = PipelineStageResult(
        stage_name="Stage 2: Aggregate",
        status="SUCCESS",
        duration_seconds=2.0,
    )
    summary = PipelineRunSummary(
        run_id="pipe_20250615_001",
        target_date="2025-06-15",
        overall_status="SUCCESS",
        stages=[stage1, stage2],
        total_duration_seconds=3.5,
        timestamp="2025-06-16T04:05:00",
    )
    data = summary.to_dict()
    assert data["run_id"] == "pipe_20250615_001"
    assert data["overall_status"] == "SUCCESS"
    assert len(data["stages"]) == 2
