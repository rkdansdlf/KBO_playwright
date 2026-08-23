"""Tests for scripts.maintenance.trim_scheduler_log."""

from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from scripts.maintenance.trim_scheduler_log import _parse_size, main, trim_log

HEAD_MARKER = b"A" * 4096
TAIL_MARKER = b"B" * 2048


@pytest.fixture()
def big_log(tmp_path: Path) -> Path:
    """Create a log larger than the trim threshold with distinct head/tail."""
    path = tmp_path / "scheduler.launchd.err.log"
    path.write_bytes(HEAD_MARKER + TAIL_MARKER)
    return path


class TestParseSize:
    def test_units(self) -> None:
        assert _parse_size("8K") == 8 * 1024
        assert _parse_size("16M") == 16 * 1024**2
        assert _parse_size("1G") == 1024**3

    def test_plain_bytes(self) -> None:
        assert _parse_size("1024") == 1024


class TestTrimLog:
    def test_trims_and_archives_head(self, big_log: Path, tmp_path: Path) -> None:
        archive_dir = tmp_path / "archive"
        original = big_log.read_bytes()

        result = trim_log(big_log, keep_bytes=1024, archive_dir=archive_dir)

        assert result["trimmed"] is True
        assert result["kept"] == 1024
        archived = Path(str(result["archived"]))
        assert archived.exists()
        assert gzip.decompress(archived.read_bytes()) == original[:-1024]
        remaining = big_log.read_bytes()
        assert len(remaining) == 1024
        assert remaining == TAIL_MARKER[-1024:]

    def test_no_op_under_threshold(self, big_log: Path, tmp_path: Path) -> None:
        result = trim_log(big_log, keep_bytes=10 * 1024 * 1024)

        assert result == {"trimmed": False, "size": big_log.stat().st_size}
        assert big_log.read_bytes() == HEAD_MARKER + TAIL_MARKER

    def test_without_archive_dir_discards_head(self, big_log: Path) -> None:
        result = trim_log(big_log, keep_bytes=1024, archive_dir=None)

        assert result["trimmed"] is True
        assert "archived" not in result
        assert len(big_log.read_bytes()) == 1024


class TestMainCli:
    def test_dry_run_reports_without_writing(self, big_log: Path, capsys: pytest.CaptureFixture[str]) -> None:
        exit_code = main(["--file", str(big_log), "--keep", "1024", "--dry-run"])
        out = capsys.readouterr().out

        assert exit_code == 0
        assert "would_trim=True" in out
        assert big_log.read_bytes() == HEAD_MARKER + TAIL_MARKER

    def test_missing_file_returns_one(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        exit_code = main(["--file", str(tmp_path / "nope.log")])

        assert exit_code == 1
        assert "not found" in capsys.readouterr().err

    def test_end_to_end_trim_via_cli(self, big_log: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        archive_dir = tmp_path / "arch"

        exit_code = main(["--file", str(big_log), "--keep", "512", "--archive-dir", str(archive_dir)])
        out = capsys.readouterr().out

        assert exit_code == 0
        assert '"trimmed": true' in out or "'trimmed': True" in out
        assert list(archive_dir.glob("*.gz"))
