"""KBO Bulk Data Parallel Chunk Loader & Checkpoint Batch Pipeline Engine."""

from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from src.services.bulk_loader_dto import (
    BulkLoadManifest,
    ChunkCheckpoint,
    ChunkPartition,
    ChunkProgressStatus,
)

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

DEFAULT_CHECKPOINT_DIR = Path("data/checkpoints")
DEFAULT_CONCURRENCY = 4


class CheckpointManager:
    """Manages file-based atomic checkpoints for bulk loading jobs."""

    def __init__(self, checkpoint_dir: Path | str = DEFAULT_CHECKPOINT_DIR) -> None:
        """Initialize CheckpointManager."""
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _get_manifest_path(self, job_id: str) -> Path:
        """Get file path for a job manifest."""
        safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in job_id)
        return self.checkpoint_dir / f"bulk_load_{safe_id}.json"

    def save_manifest(self, manifest: BulkLoadManifest) -> Path:
        """Atomically persist manifest to disk via temporary file rename."""
        target_path = self._get_manifest_path(manifest.job_id)
        temp_path = self.checkpoint_dir / f".tmp_{manifest.job_id}_{int(time.time() * 1000)}.json"

        payload = manifest.to_dict()
        with self._lock:
            with temp_path.open("w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            temp_path.replace(target_path)

        return target_path

    def load_manifest(self, job_id: str) -> BulkLoadManifest | None:
        """Load an existing manifest by job ID."""
        target_path = self._get_manifest_path(job_id)
        if not target_path.exists():
            return None

        with self._lock:
            try:
                with target_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
            except (json.JSONDecodeError, OSError):
                logger.exception("Failed to load checkpoint manifest %s", target_path)
                return None

        partitions = [
            ChunkCheckpoint(
                partition_id=p["partition_id"],
                status=ChunkProgressStatus(p["status"]),
                items_total=p.get("items_total", 0),
                items_completed=p.get("items_completed", 0),
                error_message=p.get("error_message"),
                started_at=p.get("started_at"),
                completed_at=p.get("completed_at"),
                duration_seconds=p.get("duration_seconds", 0.0),
                metadata=p.get("metadata", {}),
            )
            for p in data.get("partitions", [])
        ]

        return BulkLoadManifest(
            job_id=data["job_id"],
            category=data["category"],
            start_year=data["start_year"],
            end_year=data["end_year"],
            concurrency=data.get("concurrency", DEFAULT_CONCURRENCY),
            chunk_size=data.get("chunk_size", 1),
            partitions=partitions,
            is_resumed=data.get("is_resumed", False),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            total_duration_seconds=data.get("total_duration_seconds", 0.0),
        )

    def delete_manifest(self, job_id: str) -> bool:
        """Delete checkpoint file."""
        target_path = self._get_manifest_path(job_id)
        with self._lock:
            if target_path.exists():
                target_path.unlink()
                return True
        return False


class BulkChunkLoader:
    """Parallel chunk loader with checkpoint persistence and failure recovery."""

    def __init__(
        self,
        checkpoint_manager: CheckpointManager | None = None,
        concurrency: int = DEFAULT_CONCURRENCY,
    ) -> None:
        """Initialize BulkChunkLoader."""
        self.checkpoint_manager = checkpoint_manager or CheckpointManager()
        self.concurrency = max(1, concurrency)

    def generate_partitions(
        self,
        category: str,
        start_year: int,
        end_year: int,
        chunk_size: int = 1,
    ) -> list[ChunkPartition]:
        """Divide year range into discrete chunk partitions."""
        partitions: list[ChunkPartition] = []
        cur_year = start_year
        cat_prefix = category.lower()
        while cur_year <= end_year:
            chunk_end = min(end_year, cur_year + chunk_size - 1)
            pid = f"{cat_prefix}_{cur_year}_{chunk_end}" if chunk_end > cur_year else f"{cat_prefix}_{cur_year}"
            partitions.append(
                ChunkPartition(
                    partition_id=pid,
                    category=category,
                    start_key=cur_year,
                    end_key=chunk_end,
                )
            )
            cur_year = chunk_end + 1
        return partitions

    def _execute_partition(
        self,
        partition: ChunkPartition,
        handler: Callable[[ChunkPartition], int],
        checkpoint: ChunkCheckpoint,
        manifest: BulkLoadManifest,
    ) -> ChunkCheckpoint:
        """Execute processing for a single partition and update checkpoint."""
        checkpoint.status = ChunkProgressStatus.IN_PROGRESS
        checkpoint.started_at = datetime.now(UTC).isoformat()
        t0 = time.monotonic()

        try:
            records = handler(partition)
            checkpoint.items_completed = records
            checkpoint.status = ChunkProgressStatus.COMPLETED
            checkpoint.error_message = None
        except Exception as exc:
            logger.exception("Bulk load partition '%s' failed", partition.partition_id)
            checkpoint.status = ChunkProgressStatus.FAILED
            checkpoint.error_message = str(exc)
        finally:
            t1 = time.monotonic()
            checkpoint.duration_seconds = round(t1 - t0, 3)
            checkpoint.completed_at = datetime.now(UTC).isoformat()
            self.checkpoint_manager.save_manifest(manifest)

        return checkpoint

    def run_bulk_load(  # noqa: PLR0913
        self,
        category: str,
        start_year: int,
        end_year: int,
        worker_func: Callable[[ChunkPartition], int] | None = None,
        *,
        job_id: str | None = None,
        chunk_size: int = 1,
        resume: bool = False,
    ) -> BulkLoadManifest:
        """Execute parallel bulk loading across partitions with checkpointing."""
        jid = job_id or f"{category.lower()}_{start_year}_{end_year}"
        manifest = self.checkpoint_manager.load_manifest(jid) if resume else None

        if manifest and resume:
            manifest.is_resumed = True
            logger.info("Resuming existing bulk load job '%s' with %d partitions", jid, manifest.total_partitions)
        else:
            partitions = self.generate_partitions(category, start_year, end_year, chunk_size)
            checkpoints = [
                ChunkCheckpoint(
                    partition_id=p.partition_id,
                    status=ChunkProgressStatus.PENDING,
                    metadata={"start_key": p.start_key, "end_key": p.end_key},
                )
                for p in partitions
            ]
            manifest = BulkLoadManifest(
                job_id=jid,
                category=category,
                start_year=start_year,
                end_year=end_year,
                concurrency=self.concurrency,
                chunk_size=chunk_size,
                partitions=checkpoints,
                started_at=datetime.now(UTC).isoformat(),
            )
            self.checkpoint_manager.save_manifest(manifest)

        def _default_worker(_p: ChunkPartition) -> int:
            time.sleep(0.01)
            return 100

        handler = worker_func or _default_worker
        partition_map = {
            p.partition_id: p for p in self.generate_partitions(category, start_year, end_year, chunk_size)
        }

        # Filter pending or failed partitions to process
        to_process = [
            cp for cp in manifest.partitions if cp.status in {ChunkProgressStatus.PENDING, ChunkProgressStatus.FAILED}
        ]

        start_time = time.monotonic()

        if to_process:
            with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
                futures = {
                    executor.submit(
                        self._execute_partition,
                        partition_map.get(
                            cp.partition_id,
                            ChunkPartition(
                                partition_id=cp.partition_id,
                                category=category,
                                start_key=cp.metadata.get("start_key", start_year),
                                end_key=cp.metadata.get("end_key", end_year),
                            ),
                        ),
                        handler,
                        cp,
                        manifest,
                    ): cp
                    for cp in to_process
                }

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception:
                        logger.exception("Worker execution uncaught error")

        manifest.completed_at = datetime.now(UTC).isoformat()
        manifest.total_duration_seconds = round(time.monotonic() - start_time, 2)
        self.checkpoint_manager.save_manifest(manifest)

        return manifest


__all__ = [
    "DEFAULT_CHECKPOINT_DIR",
    "DEFAULT_CONCURRENCY",
    "BulkChunkLoader",
    "CheckpointManager",
]
