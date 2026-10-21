"""The nightly PostgreSQL backup must keep running when one input is wrong.

A missing ``LOCAL_PG_URL_DOCKER`` used to raise ``KeyError`` before any dump, so ``bega_prod`` went
unbacked-up for days. Retention also matched every ``<label>_*.dump`` by name, so hand-made dumps
(``bega_prod_after_recovery_*``) sorted ahead of the real nightly ones and displaced them.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from scripts.maintenance import backup_postgres_databases as backup

URL = "postgresql://postgres:pw@127.0.0.1:5432/db"


@pytest.fixture
def backup_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(backup, "BACKUP_DIR", tmp_path)
    return tmp_path


def _make(directory: Path, name: str, size: int = 10) -> Path:
    path = directory / name
    path.write_bytes(b"x" * size)
    return path


class TestRetention:
    def test_keeps_the_newest_dated_dumps(self, backup_dir: Path) -> None:
        for day in ("20261018", "20261019", "20261020", "20261021"):
            _make(backup_dir, f"bega_prod_{day}.dump")

        backup._enforce_retention("bega_prod", 3, dry_run=False)

        assert sorted(p.name for p in backup_dir.iterdir()) == [
            "bega_prod_20261019.dump",
            "bega_prod_20261020.dump",
            "bega_prod_20261021.dump",
        ]

    def test_hand_made_dumps_are_never_candidates_and_do_not_displace_real_ones(self, backup_dir: Path) -> None:
        """``after_recovery`` sorts after ``2026...`` by name; it must not push the nightly dumps out."""
        for name in (
            "bega_prod_20261018.dump",
            "bega_prod_20261022.dump",
            "bega_prod_after_recovery_20261013.dump",
            "bega_prod_recovered_20261013.dump",
            "kbo_rag_bega_prod_20261011.dump",
        ):
            _make(backup_dir, name)

        backup._enforce_retention("bega_prod", 2, dry_run=False)

        assert {p.name for p in backup_dir.iterdir()} == {
            "bega_prod_20261018.dump",
            "bega_prod_20261022.dump",
            "bega_prod_after_recovery_20261013.dump",
            "bega_prod_recovered_20261013.dump",
            "kbo_rag_bega_prod_20261011.dump",
        }

    def test_an_empty_dump_does_not_count_as_a_backup(self, backup_dir: Path) -> None:
        _make(backup_dir, "kbo_rag_20261013.dump", size=0)
        _make(backup_dir, "kbo_rag_20261019.dump")
        _make(backup_dir, "kbo_rag_20261022.dump")

        backup._enforce_retention("kbo_rag", 2, dry_run=False)

        assert {p.name for p in backup_dir.iterdir()} == {
            "kbo_rag_20261013.dump",
            "kbo_rag_20261019.dump",
            "kbo_rag_20261022.dump",
        }

    def test_label_prefix_of_another_label_is_not_matched(self, backup_dir: Path) -> None:
        for day in ("20261019", "20261020", "20261021"):
            _make(backup_dir, f"bega_dev_{day}.dump")
        _make(backup_dir, "bega_prod_20261021.dump")

        backup._enforce_retention("bega_dev", 1, dry_run=False)

        assert (backup_dir / "bega_prod_20261021.dump").exists()
        assert sorted(p.name for p in backup_dir.glob("bega_dev_*")) == ["bega_dev_20261021.dump"]

    def test_dry_run_deletes_nothing(self, backup_dir: Path) -> None:
        for day in ("20261019", "20261020", "20261021"):
            _make(backup_dir, f"bega_prod_{day}.dump")

        backup._enforce_retention("bega_prod", 1, dry_run=True)

        assert len(list(backup_dir.iterdir())) == 3


class TestMainKeepsGoing:
    def _run(self, monkeypatch: pytest.MonkeyPatch, urls: dict[str, str | None], dumped: list[str]) -> int:
        monkeypatch.setattr(backup, "_load_env", lambda: urls)

        def fake_dump(label: str, _url: str, _stamp: str, *, dry_run: bool) -> None:
            assert not dry_run
            dumped.append(label)

        monkeypatch.setattr(backup, "_dump", fake_dump)
        return backup.main([])

    def test_a_missing_url_skips_only_that_database(
        self,
        backup_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        dumped: list[str] = []

        code = self._run(monkeypatch, {"bega_prod": URL, "bega_dev": None, "kbo_rag": URL}, dumped)

        assert dumped == ["bega_prod", "kbo_rag"]
        assert code == 1, "the missing database must still make the run exit non-zero"

    def test_all_urls_present_exits_zero(self, backup_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        dumped: list[str] = []

        code = self._run(monkeypatch, {"bega_prod": URL, "bega_dev": URL, "kbo_rag": URL}, dumped)

        assert dumped == ["bega_prod", "bega_dev", "kbo_rag"]
        assert code == 0

    def test_a_failed_dump_does_not_prune_existing_backups(
        self,
        backup_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        for day in ("20261019", "20261020", "20261021"):
            _make(backup_dir, f"bega_prod_{day}.dump")

        def failing_dump(_label: str, _url: str, _stamp: str, *, dry_run: bool) -> None:
            raise subprocess.CalledProcessError(1, ["pg_dump"])

        monkeypatch.setattr(backup, "_load_env", lambda: {"bega_prod": URL})
        monkeypatch.setattr(backup, "_dump", failing_dump)

        assert backup.main(["--keep", "1"]) == 1
        assert len(list(backup_dir.glob("bega_prod_*.dump"))) == 3


class TestEmptyDumpIsAFailure:
    def test_zero_byte_artifact_is_removed_and_reported(
        self,
        backup_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def fake_run(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
            if cmd[:2] == ["docker", "cp"]:
                Path(cmd[3]).write_bytes(b"")
            return subprocess.CompletedProcess(cmd, 0)

        monkeypatch.setattr(backup.subprocess, "run", fake_run)

        with pytest.raises(RuntimeError, match="empty"):
            backup._dump("bega_prod", URL, "20261022", dry_run=False)

        assert list(backup_dir.iterdir()) == []
