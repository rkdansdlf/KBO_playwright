"""Unit and integration tests for KBO Bulk Chunk Loader & Checkpoint Batch Pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from src.cli.bulk_load import main as bulk_load_cli_main
from src.cli.kbo import main as kbo_master_main
from src.orchestration.master import MasterWorkflowOrchestrator
from src.services.bulk_loader import BulkChunkLoader, CheckpointManager
from src.services.bulk_loader_dto import (
    BulkLoadManifest,
    ChunkCheckpoint,
    ChunkPartition,
    ChunkProgressStatus,
)

if TYPE_CHECKING:
    import pytest


def test_checkpoint_manager_atomic_save_and_load(tmp_path: Path) -> None:
    """Test CheckpointManager atomic persistence and deserialization."""
    mgr = CheckpointManager(checkpoint_dir=tmp_path)
    job_id = "test_pbp_2020_2022"

    manifest = BulkLoadManifest(
        job_id=job_id,
        category="PBP",
        start_year=2020,
        end_year=2022,
        concurrency=2,
        partitions=[
            ChunkCheckpoint(partition_id="pbp_2020", status=ChunkProgressStatus.COMPLETED, items_completed=500),
            ChunkCheckpoint(partition_id="pbp_2021", status=ChunkProgressStatus.IN_PROGRESS, items_completed=200),
            ChunkCheckpoint(partition_id="pbp_2022", status=ChunkProgressStatus.PENDING),
        ],
    )

    path = mgr.save_manifest(manifest)
    assert path.exists()
    assert path.name == f"bulk_load_{job_id}.json"

    loaded = mgr.load_manifest(job_id)
    assert loaded is not None
    assert loaded.job_id == job_id
    assert loaded.category == "PBP"
    assert loaded.total_partitions == 3
    assert loaded.completed_partitions == 1
    assert loaded.pending_partitions == 1
    assert loaded.total_records_processed == 700


def test_checkpoint_manager_delete_manifest(tmp_path: Path) -> None:
    """Test CheckpointManager deleting checkpoint files."""
    mgr = CheckpointManager(checkpoint_dir=tmp_path)
    manifest = BulkLoadManifest(
        job_id="delete_me",
        category="BOXSCORE",
        start_year=2020,
        end_year=2020,
    )
    mgr.save_manifest(manifest)
    assert mgr.load_manifest("delete_me") is not None

    deleted = mgr.delete_manifest("delete_me")
    assert deleted is True
    assert mgr.load_manifest("delete_me") is None
    assert mgr.delete_manifest("non_existing") is False


def test_bulk_chunk_loader_partition_generation() -> None:
    """Test partition generation with single and multi-year chunking."""
    loader = BulkChunkLoader()

    # 1-year chunk size
    p1 = loader.generate_partitions(category="PBP", start_year=2020, end_year=2023, chunk_size=1)
    assert len(p1) == 4
    assert p1[0].partition_id == "pbp_2020"
    assert p1[0].start_key == 2020
    assert p1[0].end_key == 2020
    assert p1[3].partition_id == "pbp_2023"

    # 2-year chunk size
    p2 = loader.generate_partitions(category="BOXSCORE", start_year=2020, end_year=2023, chunk_size=2)
    assert len(p2) == 2
    assert p2[0].partition_id == "boxscore_2020_2021"
    assert p2[0].start_key == 2020
    assert p2[0].end_key == 2021
    assert p2[1].partition_id == "boxscore_2022_2023"


def test_bulk_chunk_loader_parallel_execution(tmp_path: Path) -> None:
    """Test concurrent chunk loading with thread pool executor."""
    mgr = CheckpointManager(checkpoint_dir=tmp_path)
    loader = BulkChunkLoader(checkpoint_manager=mgr, concurrency=3)

    processed_keys: list[str] = []

    def mock_worker(p: ChunkPartition) -> int:
        processed_keys.append(p.partition_id)
        return int(p.start_key) * 10

    manifest = loader.run_bulk_load(
        category="PBP",
        start_year=2021,
        end_year=2023,
        worker_func=mock_worker,
    )

    assert manifest.total_partitions == 3
    assert manifest.completed_partitions == 3
    assert manifest.failed_partitions == 0
    assert manifest.total_records_processed == (20210 + 20220 + 20230)
    assert len(processed_keys) == 3


def test_bulk_chunk_loader_fault_isolation_and_resume(tmp_path: Path) -> None:
    """Test that a failing chunk does not stop the batch and is safely resumed."""
    mgr = CheckpointManager(checkpoint_dir=tmp_path)
    loader = BulkChunkLoader(checkpoint_manager=mgr, concurrency=2)
    job_id = "resume_test_job"

    # 1. First run: 2022 fails with an intentional error
    def flaky_worker(p: ChunkPartition) -> int:
        if p.start_key == 2022:
            msg = "Intentional network timeout during 2022 crawl"
            raise RuntimeError(msg)
        return 150

    m1 = loader.run_bulk_load(
        category="PBP",
        start_year=2021,
        end_year=2023,
        worker_func=flaky_worker,
        job_id=job_id,
    )

    assert m1.total_partitions == 3
    assert m1.completed_partitions == 2
    assert m1.failed_partitions == 1

    failed_chunk = next(p for p in m1.partitions if p.partition_id == "pbp_2022")
    assert failed_chunk.status == ChunkProgressStatus.FAILED
    assert "Intentional network timeout" in (failed_chunk.error_message or "")

    # 2. Resume run: Fix the issue and resume
    def fixed_worker(p: ChunkPartition) -> int:
        assert p.start_key == 2022  # Only the failed partition should be processed
        return 150

    m2 = loader.run_bulk_load(
        category="PBP",
        start_year=2021,
        end_year=2023,
        worker_func=fixed_worker,
        job_id=job_id,
        resume=True,
    )

    assert m2.is_resumed is True
    assert m2.total_partitions == 3
    assert m2.completed_partitions == 3
    assert m2.failed_partitions == 0
    assert m2.total_records_processed == 450


def test_bulk_loader_manifest_ascii_summary() -> None:
    """Test terminal ASCII progress summary rendering."""
    manifest = BulkLoadManifest(
        job_id="summary_test",
        category="SEASON_STATS",
        start_year=2020,
        end_year=2024,
        concurrency=4,
        partitions=[
            ChunkCheckpoint("stats_2020", ChunkProgressStatus.COMPLETED, items_completed=100, duration_seconds=1.2),
            ChunkCheckpoint("stats_2021", ChunkProgressStatus.COMPLETED, items_completed=100, duration_seconds=1.1),
            ChunkCheckpoint("stats_2022", ChunkProgressStatus.FAILED, error_message="HTTP 500"),
            ChunkCheckpoint("stats_2023", ChunkProgressStatus.PENDING),
            ChunkCheckpoint("stats_2024", ChunkProgressStatus.PENDING),
        ],
        total_duration_seconds=2.5,
    )

    card = manifest.to_ascii_summary()
    assert "KBO BULK LOADER MANIFEST" in card
    assert "SEASON_STATS" in card
    assert "Progress:  40.0%" in card
    assert "stats_2020" in card
    assert "stats_2022" in card


def test_bulk_loader_cli_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test dedicated bulk_load CLI execution in text and JSON formats."""
    monkeypatch.chdir(tmp_path)

    # Text format
    code1 = bulk_load_cli_main(
        [
            "--category",
            "pbp",
            "--start-year",
            "2023",
            "--end-year",
            "2024",
            "--concurrency",
            "2",
        ]
    )
    assert code1 == 0
    cap1 = capsys.readouterr()
    assert "KBO BULK LOADER MANIFEST" in cap1.out
    assert "PBP" in cap1.out

    # JSON format
    code2 = bulk_load_cli_main(
        [
            "--category",
            "boxscore",
            "--start-year",
            "2024",
            "--end-year",
            "2024",
            "--json",
        ]
    )
    assert code2 == 0
    cap2 = capsys.readouterr()
    json_str = cap2.out[cap2.out.find("{") :]
    data = json.loads(json_str)
    assert data["category"] == "boxscore"
    assert data["total_partitions"] == 1
    assert data["completed_partitions"] == 1


def test_kbo_master_cli_bulk_load(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test kbo master CLI bulk-load subcommand."""
    monkeypatch.chdir(tmp_path)

    code = kbo_master_main(
        [
            "bulk-load",
            "--category",
            "pbp",
            "--start-year",
            "2024",
            "--end-year",
            "2024",
        ]
    )
    assert code == 0
    cap = capsys.readouterr()
    assert "KBO BULK LOADER MANIFEST" in cap.out


def test_master_workflow_bulk_load_dag() -> None:
    """Test executing bulk_load pipeline within MasterWorkflowOrchestrator DAG."""
    orch = MasterWorkflowOrchestrator.build_bulk_load_workflow()
    context = {
        "category": "PBP",
        "start_year": 2023,
        "end_year": 2024,
        "concurrency": 2,
    }

    report = orch.execute_workflow("bulk_load_test_dag", context=context, dry_run=False)
    assert report.overall_status == "SUCCESS"
    assert report.total_stages == 4
    assert report.completed_stages == 4
    assert report.failed_stages == 0
    assert report.skipped_stages == 0
