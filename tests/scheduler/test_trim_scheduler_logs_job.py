"""The weekly scheduler log trim must archive the bytes it discards.

Regression: ``trim_scheduler_logs_job`` called ``trim_log`` without an
``archive_dir``, so the 2026-09-27 and 2026-10-04 trims destroyed the only
evidence of the period under investigation.
"""

from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from src.scheduler.jobs import maintenance

HEAD_MARKER = b"H" * 4096
KEEP_BYTES = 16 * 1024 * 1024


def _write_log(root: Path, *, size: int = KEEP_BYTES) -> Path:
    log_dir = root / "logs"
    log_dir.mkdir()
    log_path = log_dir / "scheduler.launchd.err.log"
    log_path.write_bytes(HEAD_MARKER + b"T" * size)
    return log_path


def test_trim_job_archives_the_discarded_head(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    log_path = _write_log(tmp_path)

    maintenance.trim_scheduler_logs_job()

    archives = sorted((tmp_path / "data" / "archive" / "logs").glob("*.gz"))
    assert len(archives) == 1
    assert gzip.decompress(archives[0].read_bytes()) == HEAD_MARKER
    assert log_path.read_bytes() == b"T" * KEEP_BYTES


def test_trim_job_is_a_noop_when_the_log_is_small(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    log_path = _write_log(tmp_path, size=1024)

    maintenance.trim_scheduler_logs_job()

    assert not (tmp_path / "data" / "archive" / "logs").exists()
    assert log_path.read_bytes() == HEAD_MARKER + b"T" * 1024
