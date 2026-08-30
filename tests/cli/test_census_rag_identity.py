"""CLI tests for src.cli.rag.census_rag_identity (in-process, no live DB)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

from src.cli.rag.census_rag_identity import main


def _row(
    table: str,
    row_id: str,
    *,
    content_hash: str = "h1",
    status: str = "ACTIVE",
) -> tuple:
    """Build a DB identity row as a tuple (id, source_table, source_row_id, content_hash, index_status)."""
    return (1, table, row_id, content_hash, status)


class _FakeResult:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def mappings(self) -> _FakeResult:
        return self

    def all(self) -> list[tuple]:
        return self._rows

    def yield_per(self, _: int) -> _FakeResult:
        return self

    def __iter__(self):
        return iter(self._rows)


class _FakeExecuteResult:
    """Mock for session.execute() that supports yield_per()."""

    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def yield_per(self, _: int) -> _FakeResult:
        return _FakeResult(self._rows)


class _NullContext:
    def __init__(self, value: object) -> None:
        self._value = value

    def __enter__(self) -> object:
        return self._value

    def __exit__(self, *args: object) -> None:
        return None


@pytest.fixture()
def mock_sessions(monkeypatch: pytest.MonkeyPatch) -> tuple[Mock, Mock]:
    """Mock both source and index sessions."""
    import src.db.engine as engine_mod

    source_session = Mock()
    index_session = Mock()

    source_session.execute.return_value = _FakeExecuteResult([])
    index_session.execute.return_value = _FakeResult([])

    def _source_context():
        return _NullContext(source_session)

    def _index_context():
        return _NullContext(index_session)

    monkeypatch.setattr(engine_mod, "get_rag_source_session", _source_context)
    monkeypatch.setattr(engine_mod, "get_rag_index_session", _index_context)

    return source_session, index_session


class TestCensusCLI:
    def test_help_exits_zero(self, capsys: pytest.CaptureFixture[str]) -> None:
        with pytest.raises(SystemExit) as exc:
            main(["--help"])
        assert exc.value.code == 0

    def test_dry_run_basic(self, mock_sessions: tuple[Mock, Mock], capsys: pytest.CaptureFixture[str]) -> None:
        source_session, index_session = mock_sessions
        source_session.execute.return_value = _FakeExecuteResult([])
        index_session.execute.return_value = _FakeResult([_row("awards", "1")])

        exit_code = main(["--dry-run", "--json", "--sample", "5"])
        assert exit_code == 0
        output = json.loads(capsys.readouterr().out)
        assert output["read_only"] is True
        assert output["target_index_version"] == "rag-v2"

    def test_source_filter(self, mock_sessions: tuple[Mock, Mock], capsys: pytest.CaptureFixture[str]) -> None:
        source_session, index_session = mock_sessions
        source_session.execute.return_value = _FakeExecuteResult([])
        index_session.execute.return_value = _FakeResult([_row("awards", "1")])

        exit_code = main(["--dry-run", "--json", "--source", "awards"])
        assert exit_code == 0
        output = json.loads(capsys.readouterr().out)
        assert output["source_tables"] == ["awards"]

    def test_fail_on_unsafe_returns_one_when_unsafe(
        self, mock_sessions: tuple[Mock, Mock], capsys: pytest.CaptureFixture[str]
    ) -> None:
        source_session, index_session = mock_sessions
        source_session.execute.return_value = _FakeExecuteResult([])
        index_session.execute.return_value = _FakeResult([_row("awards", "999")])

        exit_code = main(["--dry-run", "--fail-on-unsafe"])
        assert exit_code == 1

    def test_fail_on_unsafe_returns_zero_when_safe(
        self, mock_sessions: tuple[Mock, Mock], capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import src.cli.rag.census_rag_identity as cli_mod
        from src.services.rag_identity_census import SourceIdentityRecord

        source_session, index_session = mock_sessions
        source_session.execute.return_value = _FakeExecuteResult([])
        index_session.execute.return_value = _FakeResult([_row("awards", "1")])

        def _mock_iter(session, source_table, season=None):
            if source_table == "awards":
                return iter([SourceIdentityRecord("awards", "1", "2025_골든글러브_투수_원태인")])
            return iter([])

        monkeypatch.setattr(cli_mod, "iter_source_identity_records", _mock_iter)

        exit_code = main(["--dry-run", "--fail-on-unsafe", "--source", "awards"])
        assert exit_code == 0

    def test_invalid_source_rejected(self, capsys: pytest.CaptureFixture[str]) -> None:
        with pytest.raises(SystemExit) as exc:
            main(["--dry-run", "--source", "invalid_table"])
        assert exc.value.code == 2

    def test_negative_sample_rejected(self, capsys: pytest.CaptureFixture[str]) -> None:
        exit_code = main(["--dry-run", "--sample", "-1"])
        assert exit_code == 2


class TestIterSourceIdentityRecords:
    def test_source_iterator_called_per_table(
        self, mock_sessions: tuple[Mock, Mock], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify that source iterator is invoked for each requested table."""
        import src.cli.rag.census_rag_identity as cli_mod

        source_session, index_session = mock_sessions
        source_session.execute.return_value = _FakeExecuteResult([])
        index_session.execute.return_value = _FakeResult([])

        call_count = {"count": 0}

        def _tracking_iter(session, source_table, season=None):
            call_count["count"] += 1
            return iter([])

        monkeypatch.setattr(cli_mod, "iter_source_identity_records", _tracking_iter)

        main(["--dry-run", "--json", "--source", "awards", "--source", "team_history"])
        assert call_count["count"] == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
