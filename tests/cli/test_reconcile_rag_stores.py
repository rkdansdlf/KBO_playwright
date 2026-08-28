"""CLI tests for src.cli.rag.reconcile_rag_stores (in-process, no live DB)."""

from __future__ import annotations

import json
from datetime import datetime, timezone, UTC
from pathlib import Path
from typing import Any
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.cli.rag.reconcile_rag_stores import fetch_identity_entries, main
from src.services.rag_reconciliation import ManifestEntry, write_manifest


def _row(
    table: str,
    row_id: str,
    *,
    content_hash: str = "h1",
    version: str = "v1",
    status: str = "ACTIVE",
    embedding_present: int = 1,
    updated_at: str | None = None,
) -> dict[str, Any]:
    """Build a DB identity row mapping."""
    return {
        "source_table": table,
        "source_row_id": row_id,
        "content_hash": content_hash,
        "index_version": version,
        "index_status": status,
        "embedding_present": embedding_present,
        "updated_at": updated_at,
    }


class _FakeResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _FakeResult:
        return self

    def all(self) -> list[dict[str, Any]]:
        return self._rows


def _manifest_file(path: Path, entries: list[ManifestEntry]) -> Path:
    write_manifest(entries, path)
    return path


@pytest.fixture()
def _clean_reports_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Redirect the default reports root into a temporary directory."""
    from src.cli.rag import reconcile_rag_stores as cli

    monkeypatch.setattr(cli, "_DEFAULT_OUTPUT_ROOT", tmp_path / "reports")
    return tmp_path / "reports"


class TestFetchIdentityEntries:
    def test_prefers_timestamped_query_with_fallback(self) -> None:
        session = Mock()
        session.execute.side_effect = [
            SQLAlchemyError("no such column: created_at"),
            _FakeResult([_row("awards", "1")]),
        ]

        entries = fetch_identity_entries(session)

        assert len(entries) == 1
        assert entries[0].key == "awards:1"
        assert session.execute.call_count == 2

    def test_raises_runtime_error_when_both_variants_fail(self) -> None:
        session = Mock()
        session.execute.side_effect = SQLAlchemyError("boom")

        with pytest.raises(RuntimeError, match="identity query failed"):
            fetch_identity_entries(session)

    def test_datetime_timestamp_serialized_to_iso(self) -> None:
        stamp = datetime(2026, 8, 22, 17, 40, 39, tzinfo=UTC)
        session = Mock()
        session.execute.return_value = _FakeResult([_row("game", "9", updated_at=stamp.isoformat())])

        entries = fetch_identity_entries(session)

        assert entries[0].updated_at is not None

    def test_oracle_projection_uses_native_vector_column(self) -> None:
        session = Mock()
        session.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="oracle"))
        session.execute.return_value = _FakeResult([_row("game", "9")])

        fetch_identity_entries(session)

        assert "embedding_vector" in str(session.execute.call_args.args[0])

    def test_postgresql_projection_uses_embedding_column(self) -> None:
        session = Mock()
        session.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
        session.execute.return_value = _FakeResult([_row("game", "9")])

        fetch_identity_entries(session)

        query = str(session.execute.call_args.args[0])
        assert "CASE WHEN embedding IS NULL" in query
        assert "embedding_vector" not in query


class TestExportCommand:
    @pytest.fixture()
    def _primary_session(self, monkeypatch: pytest.MonkeyPatch) -> Mock:
        rows = [_row("awards", "1"), _row("game", "abc", embedding_present=0)]
        session = Mock()
        session.execute.side_effect = [
            SQLAlchemyError("no such column"),
            _FakeResult(rows),
        ]
        import src.db.engine as engine_mod

        monkeypatch.setattr(engine_mod, "get_rag_index_session", lambda: _NullContext(session))
        return session

    def test_export_writes_manifest_and_reports_rows(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        _primary_session: Mock,
    ) -> None:
        out_path = tmp_path / "out.ndjson"

        exit_code = main(["export", "--side", "primary", "--out", str(out_path)])
        captured = json.loads(capsys.readouterr().out)

        assert exit_code == 0
        assert captured["rows"] == 2
        assert out_path.exists()

    def test_unknown_side_is_rejected_by_argparse(self) -> None:
        with pytest.raises(SystemExit):
            main(["export", "--side", "bogus"])

    def test_export_side_guard_raises_for_unknown_side(self) -> None:
        from src.cli.rag.reconcile_rag_stores import _export_side

        with pytest.raises(RuntimeError, match="unknown side"):
            _export_side("bogus")

    def test_staging_failure_returns_one(
        self, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import src.db.vector_engine as vector_mod

        class _ExplodingContext:
            def __enter__(self) -> object:
                message = "pgvector unavailable"
                raise RuntimeError(message)

            def __exit__(self, *args: object) -> None:
                return None

        monkeypatch.setattr(vector_mod, "get_vector_session", _ExplodingContext)
        exit_code = main(["export", "--side", "staging"])

        assert exit_code == 1
        assert "pgvector unavailable" in capsys.readouterr().err


class _NullContext:
    def __init__(self, value: object) -> None:
        self._value = value

    def __enter__(self) -> object:
        return self._value

    def __exit__(self, *args: object) -> None:
        return None


class TestCompareCommand:
    def test_compare_without_as_of_reports_drift(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        _clean_reports_root: Path,
    ) -> None:
        left = _manifest_file(tmp_path / "left.ndjson", [ManifestEntry("awards", "1", content_hash="h1")])
        right = _manifest_file(tmp_path / "right.ndjson", [ManifestEntry("awards", "1", content_hash="h2")])
        output_dir = tmp_path / "report"

        exit_code = main(["compare", "--left", str(left), "--right", str(right), "--output-dir", str(output_dir)])
        payload = json.loads(capsys.readouterr().out)

        assert exit_code == 0
        assert payload["unexplained"] == 1
        summary = json.loads((output_dir / "comparison_summary.json").read_text(encoding="utf-8"))
        assert summary["is_clean"] is False
        assert (output_dir / "unexplained_keys.txt").read_text(encoding="utf-8") == "awards:1\n"

    def test_compare_with_as_of_explains_recent_changes(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        left = _manifest_file(
            tmp_path / "l.ndjson",
            [ManifestEntry("awards", "1", content_hash="old", updated_at=datetime(2026, 8, 20, tzinfo=UTC))],
        )
        right = _manifest_file(
            tmp_path / "r.ndjson",
            [ManifestEntry("awards", "1", content_hash="new", updated_at=datetime(2026, 8, 25, tzinfo=UTC))],
        )
        output_dir = tmp_path / "report"

        exit_code = main(
            [
                "compare",
                "--left",
                str(left),
                "--right",
                str(right),
                "--as-of",
                "2026-08-21T00:00:00+00:00",
                "--output-dir",
                str(output_dir),
            ]
        )
        payload = json.loads(capsys.readouterr().out)

        assert exit_code == 0
        assert payload["clean"] is True
        assert (output_dir / "time_explainable_keys.txt").read_text(encoding="utf-8") == "awards:1\n"

    def test_fail_on_unexplained_exit_code(self, tmp_path: Path) -> None:
        left = _manifest_file(tmp_path / "l.ndjson", [ManifestEntry("awards", "1", content_hash="h1")])
        right = _manifest_file(tmp_path / "r.ndjson", [ManifestEntry("awards", "2", content_hash="h2")])

        exit_code = main(
            [
                "compare",
                "--left",
                str(left),
                "--right",
                str(right),
                "--output-dir",
                str(tmp_path / "rep"),
                "--fail-on-unexplained",
            ]
        )

        assert exit_code == 1
