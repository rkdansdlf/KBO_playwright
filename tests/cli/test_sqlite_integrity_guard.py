from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from src.cli.sqlite_integrity_guard import main
from src.db.sqlite_integrity import check_sqlite_database, sqlite_path_from_url


def _sqlite_url(path: Path) -> str:
    return f"sqlite:///{path}"


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

    def test_cli_quarantine_success_exit_code(self, tmp_path: Path, capsys):
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
        payload = json.loads(capsys.readouterr().out)

        assert exit_code == 0
        assert payload["status"] == "quarantined"
