from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.cli import sqlite_integrity_guard as module
from src.cli.sqlite_integrity_guard import main
from src.db.sqlite_integrity import SqliteIntegrityReport, check_sqlite_database, sqlite_path_from_url


def _sqlite_url(path: Path) -> str:
    return f"sqlite:///{path}"


@pytest.fixture(autouse=True)
def quarantine_incidents(monkeypatch: pytest.MonkeyPatch) -> list[list[object]]:
    """Record quarantine incidents instead of delivering them to real channels.

    ``sqlite_integrity_guard.main`` opens an incident whenever a file is
    quarantined or a quarantine fails, without requiring ``--notify``. Every
    test in this module drives exactly those paths, so without this fixture a
    test run pages the production channels with ``pytest-*/corrupt.db`` paths.

    Returns:
        One list of published events per ``apply_incidents`` call.

    """
    recorded: list[list[object]] = []
    monkeypatch.setattr(module, "apply_incidents", lambda events, **_kwargs: recorded.append(list(events)))
    return recorded


def _create_valid_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE crawl_runs (id INTEGER PRIMARY KEY)")
        connection.commit()
    finally:
        connection.close()


def _create_invalid_rootpage_db(path: Path) -> None:
    _create_valid_db(path)
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA writable_schema=ON")
        connection.execute("UPDATE sqlite_schema SET rootpage=-1 WHERE name='crawl_runs'")
        connection.commit()
    finally:
        connection.close()


class TestSqlitePathFromUrl:
    def test_absolute_sqlite_path(self, tmp_path: Path):
        db_path = tmp_path / "kbo_dev.db"
        assert sqlite_path_from_url(f"sqlite:///{db_path}") == db_path

    def test_relative_sqlite_path(self):
        assert sqlite_path_from_url("sqlite:///data/kbo_dev.db") == Path("data/kbo_dev.db")

    def test_memory_is_not_file_backed(self):
        assert sqlite_path_from_url("sqlite:///:memory:") is None

    def test_postgres_is_skipped(self):
        assert sqlite_path_from_url("postgresql://host/db") is None


class TestCheckSqliteDatabase:
    def test_valid_database_ok(self, tmp_path: Path):
        db_path = tmp_path / "valid.db"
        _create_valid_db(db_path)

        report = check_sqlite_database(_sqlite_url(db_path))

        assert report.status == "ok"
        assert report.ok is True
        assert report.quick_check == "ok"

    def test_missing_database_ok_by_default(self, tmp_path: Path):
        report = check_sqlite_database(_sqlite_url(tmp_path / "missing.db"))

        assert report.status == "missing"
        assert report.ok is True

    def test_missing_database_fails_when_strict(self, tmp_path: Path):
        report = check_sqlite_database(_sqlite_url(tmp_path / "missing.db"), strict=True)

        assert report.status == "missing"
        assert report.ok is False

    def test_empty_database_ok_by_default(self, tmp_path: Path):
        db_path = tmp_path / "empty.db"
        db_path.write_bytes(b"")

        report = check_sqlite_database(_sqlite_url(db_path))

        assert report.status == "empty"
        assert report.ok is True

    def test_random_bytes_database_is_corrupt(self, tmp_path: Path):
        db_path = tmp_path / "corrupt.db"
        db_path.write_bytes(b"not a sqlite database")

        report = check_sqlite_database(_sqlite_url(db_path), action="none")

        assert report.status == "corrupt"
        assert report.ok is False
        assert "file is not a database" in str(report.error)

    def test_invalid_rootpage_database_is_corrupt(self, tmp_path: Path):
        db_path = tmp_path / "invalid_rootpage.db"
        _create_invalid_rootpage_db(db_path)

        report = check_sqlite_database(_sqlite_url(db_path), action="none")

        assert report.status == "corrupt"
        assert report.ok is False
        assert "invalid rootpage" in str(report.error)

    def test_quarantine_moves_sqlite_family(self, tmp_path: Path):
        db_path = tmp_path / "corrupt.db"
        db_path.write_bytes(b"not a sqlite database")
        db_path.with_name("corrupt.db-wal").write_bytes(b"wal")
        db_path.with_name("corrupt.db-shm").write_bytes(b"shm")
        quarantine_root = tmp_path / "archive"

        report = check_sqlite_database(
            _sqlite_url(db_path),
            action="quarantine",
            quarantine_root=quarantine_root,
        )

        assert report.status == "quarantined"
        assert report.ok is True
        assert not db_path.exists()
        assert not db_path.with_name("corrupt.db-wal").exists()
        assert not db_path.with_name("corrupt.db-shm").exists()
        assert len(report.moved_files) == 3


class TestSqliteIntegrityGuardCli:
    def test_parser_uses_environment_defaults(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "sqlite:///from-env.db")
        monkeypatch.setenv("SQLITE_CORRUPT_ACTION", "quarantine")

        args = module.build_arg_parser().parse_args([])

        assert args.database_url == "sqlite:///from-env.db"
        assert args.action == "quarantine"
        assert args.strict is False
        assert args.json is False

    def test_cli_forwards_options_to_integrity_check(self, monkeypatch, tmp_path: Path, capsys):
        report = SqliteIntegrityReport(
            database_url="sqlite:///mock.db",
            database_path="mock.db",
            status="ok",
            ok=True,
            reason="SQLite integrity check passed",
            quick_check="ok",
        )
        check = MagicMock(return_value=report)
        exit_code = MagicMock(return_value=0)
        quarantine_root = tmp_path / "quarantine"
        monkeypatch.setattr(module, "check_sqlite_database", check)
        monkeypatch.setattr(module, "sqlite_guard_exit_code", exit_code)

        result = main(
            [
                "--database-url",
                "sqlite:///mock.db",
                "--action",
                "quarantine",
                "--quarantine-root",
                str(quarantine_root),
                "--strict",
                "--json",
            ],
        )

        assert result == 0
        check.assert_called_once_with(
            "sqlite:///mock.db",
            strict=True,
            action="quarantine",
            quarantine_root=quarantine_root,
        )
        exit_code.assert_called_once_with(report, strict=True)
        assert json.loads(capsys.readouterr().out)["status"] == "ok"

    def test_cli_json_success(self, tmp_path: Path, capsys):
        db_path = tmp_path / "valid.db"
        _create_valid_db(db_path)

        exit_code = main(["--database-url", _sqlite_url(db_path), "--json"])
        payload = json.loads(capsys.readouterr().out)

        assert exit_code == 0
        assert payload["status"] == "ok"

    def test_cli_corrupt_exit_code(self, tmp_path: Path, capsys):
        db_path = tmp_path / "corrupt.db"
        db_path.write_bytes(b"not a sqlite database")

        exit_code = main(["--database-url", _sqlite_url(db_path), "--action", "none", "--json"])
        payload = json.loads(capsys.readouterr().out)

        assert exit_code == 2
        assert payload["status"] == "corrupt"

    def test_cli_quarantine_success_exit_code(self, tmp_path: Path, capsys, quarantine_incidents: list[list[object]]):
        db_path = tmp_path / "corrupt.db"
        db_path.write_bytes(b"not a sqlite database")

        exit_code = main(
            [
                "--database-url",
                _sqlite_url(db_path),
                "--action",
                "quarantine",
                "--quarantine-root",
                str(tmp_path / "archive"),
                "--json",
            ],
        )
        out = capsys.readouterr().out.strip()
        payload = json.loads(out.splitlines()[-1])

        assert exit_code == 0
        assert payload["status"] == "quarantined"
        # The incident is raised by the code under test but must never leave the process.
        assert len(quarantine_incidents) == 1
        event = quarantine_incidents[0][0]
        assert event.incident_key == f"integrity:sqlite:quarantine:{db_path}"
        assert event.metadata["database_path"] == str(db_path)

    def test_cli_human_output_includes_paths(self, monkeypatch, capsys, quarantine_incidents: list[list[object]]):
        report = SimpleNamespace(
            database_url="sqlite:///broken.db",
            database_path="broken.db",
            status="quarantined",
            ok=True,
            reason="moved to archive",
            quarantine_dir="archive/123",
        )
        monkeypatch.setattr(module, "check_sqlite_database", lambda *_args, **_kwargs: report)
        monkeypatch.setattr(module, "sqlite_guard_exit_code", lambda *_args, **_kwargs: 0)

        exit_code = main(["--database-url", "sqlite:///broken.db"])
        output = capsys.readouterr().out

        assert exit_code == 0
        assert "quarantined: moved to archive" in output
        assert "database_path=broken.db" in output
        assert "quarantine_dir=archive/123" in output
        assert quarantine_incidents[0][0].metadata["database_path"] == "broken.db"

    def test_cli_human_output_without_optional_paths(self, monkeypatch, capsys):
        report = SimpleNamespace(
            status="skipped",
            reason="not a file-backed SQLite database",
            database_path=None,
            quarantine_dir=None,
        )
        check = MagicMock(return_value=report)
        monkeypatch.setattr(module, "check_sqlite_database", check)
        monkeypatch.setattr(module, "sqlite_guard_exit_code", lambda *_args, **_kwargs: 0)

        result = main(["--database-url", "postgresql://host/db"])
        output = capsys.readouterr().out

        assert result == 0
        assert output == "skipped: not a file-backed SQLite database\n"
        check.assert_called_once()

    def test_cli_returns_quarantine_failure_exit_code(
        self, monkeypatch, capsys, quarantine_incidents: list[list[object]]
    ):
        report = SqliteIntegrityReport(
            database_url="sqlite:///broken.db",
            database_path="broken.db",
            status="quarantine_failed",
            ok=False,
            reason="SQLite database is corrupt and quarantine failed",
            quick_check="file is not a database",
            error="permission denied",
        )
        check = MagicMock(return_value=report)
        monkeypatch.setattr(module, "check_sqlite_database", check)

        result = main(["--database-url", "sqlite:///broken.db", "--action", "quarantine"])
        output = capsys.readouterr().out

        assert result == 3
        assert "quarantine_failed: SQLite database is corrupt and quarantine failed" in output
        event = quarantine_incidents[0][0]
        assert event.metadata["status"] == "quarantine_failed"
        assert event.metadata["database_path"] == "broken.db"

    def test_cli_propagates_integrity_dependency_error(self, monkeypatch):
        check = MagicMock(side_effect=OSError("integrity helper unavailable"))
        monkeypatch.setattr(module, "check_sqlite_database", check)

        with pytest.raises(OSError, match="integrity helper unavailable"):
            main(["--database-url", "sqlite:///broken.db"])

    def test_cli_opens_quarantine_incident_when_quarantined(
        self,
        monkeypatch,
        quarantine_incidents: list[list[object]],
    ):
        report = SqliteIntegrityReport(
            database_url="sqlite:///corrupt.db",
            database_path="corrupt.db",
            status="quarantined",
            ok=True,
            reason="quarantined successfully",
            quarantine_dir="archive/20260729_000000",
            moved_files=("archive/20260729_000000/corrupt.db",),
        )
        monkeypatch.setattr(module, "check_sqlite_database", MagicMock(return_value=report))
        monkeypatch.setattr(module, "sqlite_guard_exit_code", MagicMock(return_value=0))

        result = main(["--database-url", "sqlite:///corrupt.db", "--action", "quarantine"])

        assert result == 0
        event = quarantine_incidents[0][0]
        assert event.metadata["database_path"] == "corrupt.db"
        assert event.metadata["moved_files"] == ["corrupt.db"]

    def test_cli_does_not_alert_on_ok_status(self, tmp_path: Path, capsys, quarantine_incidents: list[list[object]]):
        db_path = tmp_path / "healthy.db"
        _create_valid_db(db_path)

        exit_code = main(["--database-url", _sqlite_url(db_path), "--json"])
        payload = json.loads(capsys.readouterr().out)

        assert exit_code == 0
        assert payload["status"] == "ok"
        assert quarantine_incidents == []

    def test_cli_corrupt_without_action_does_not_alert(
        self, tmp_path: Path, capsys, quarantine_incidents: list[list[object]]
    ):
        db_path = tmp_path / "corrupt.db"
        db_path.write_bytes(b"not a sqlite database")

        exit_code = main(["--database-url", _sqlite_url(db_path), "--action", "none", "--json"])
        payload = json.loads(capsys.readouterr().out)

        assert exit_code == 2
        assert payload["status"] == "corrupt"
        assert quarantine_incidents == []


class TestQuarantineIncidentIdentity:
    """A healthy run recovers the database it inspected — and only that one.

    Incident identity is the database path, so recovery must be addressed by key.
    Reconciling the whole ``integrity:sqlite:quarantine:`` namespace would let a
    healthy run for one file silently clear a different file's open quarantine.
    """

    @staticmethod
    def _report(database_path: str, status: str) -> SimpleNamespace:
        return SimpleNamespace(
            database_path=database_path,
            status=status,
            reason=f"{status} for {database_path}",
            quarantine_dir="archive/run",
            moved_files=(),
        )

    def test_healthy_run_does_not_recover_another_database(self) -> None:
        with patch.object(module, "apply_incidents") as apply:
            # DB-A is quarantined.
            module.apply_quarantine_incident(self._report("a.db", "quarantined"), notify=False)
            # A later healthy run inspects a *different* file.
            module.apply_quarantine_incident(self._report("b.db", "ok"), notify=True)

        opened = [event.incident_key for event in apply.call_args_list[0].args[0]]
        assert opened == [module.quarantine_incident_key("a.db")]

        # DB-B's recovery names DB-B only; DB-A's incident is left untouched.
        assert apply.call_args_list[1].args[0] == []
        assert apply.call_args_list[1].kwargs["resolve_keys"] == [module.quarantine_incident_key("b.db")]
        assert module.quarantine_incident_key("a.db") not in apply.call_args_list[1].kwargs["resolve_keys"]

    def test_healthy_run_recovers_its_own_database(self) -> None:
        with patch.object(module, "apply_incidents") as apply:
            module.apply_quarantine_incident(self._report("a.db", "quarantined"), notify=False)
            module.apply_quarantine_incident(self._report("a.db", "ok"), notify=True)

        assert apply.call_args_list[1].kwargs["resolve_keys"] == [module.quarantine_incident_key("a.db")]

    def test_healthy_run_without_notify_leaves_the_ledger_alone(self) -> None:
        with patch.object(module, "apply_incidents") as apply:
            module.apply_quarantine_incident(self._report("a.db", "ok"), notify=False)

        apply.assert_not_called()
