"""Tests for the read-only RAG corpus inventory CLI."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.inventory_rag_corpus import main


def test_inventory_reports_database_failures_as_exit_two(capsys) -> None:
    """Classify schema or connection errors as infrastructure failures."""
    session = MagicMock()
    session.__enter__.side_effect = RuntimeError("schema unavailable")
    with (
        patch("src.cli.inventory_rag_corpus.get_db_session", return_value=session),
        patch("src.cli.inventory_rag_corpus.get_rag_index_session"),
    ):
        assert main(["--source", "players", "--json"]) == 2

    assert "schema unavailable" in capsys.readouterr().out


def test_inventory_can_require_nonempty_sources(capsys) -> None:
    """Treat an explicitly required empty source as a corpus defect."""
    session = MagicMock()
    session.__enter__.return_value = session
    session.execute.return_value.scalars.return_value.all.return_value = []
    with (
        patch("src.cli.inventory_rag_corpus.get_db_session", return_value=session),
        patch("src.cli.inventory_rag_corpus.get_rag_index_session", return_value=session),
        patch.dict("src.cli.inventory_rag_corpus.build_rag_index._SOURCE_MAP", {"players": lambda *_: iter(())}),
    ):
        assert main(["--source", "players", "--require-source", "players", "--json"]) == 1

    assert "required source produced no chunks" in capsys.readouterr().out


def test_inventory_writes_json_artifact(tmp_path) -> None:
    """Persist an inventory report without changing database state."""
    session = MagicMock()
    session.__enter__.return_value = session
    session.execute.return_value.scalars.return_value.all.return_value = []
    output = tmp_path / "inventory.json"
    with (
        patch("src.cli.inventory_rag_corpus.get_db_session", return_value=session),
        patch("src.cli.inventory_rag_corpus.get_rag_index_session", return_value=session),
        patch.dict(
            "src.cli.inventory_rag_corpus.build_rag_index._SOURCE_MAP",
            {
                "players": lambda *_: iter(
                    ({"source_table": "player_basic", "source_row_id": "1", "document_type": "player"},),
                ),
            },
        ),
    ):
        assert main(["--source", "players", "--output", str(output)]) == 0

    assert '"chunks_generated": 1' in output.read_text(encoding="utf-8")


def test_inventory_validates_profile_with_separate_source_and_index_sessions() -> None:
    """Use the source session for generation and the index session for comparison."""
    source_session = MagicMock()
    source_session.__enter__.return_value = source_session
    index_session = MagicMock()
    index_session.__enter__.return_value = index_session
    index_session.execute.return_value.scalars.return_value.all.return_value = []

    def source_chunks(session, _year, _limit):
        assert session is source_session
        yield {"source_table": "awards", "source_row_id": "1", "document_type": "award"}

    with (
        patch("src.cli.inventory_rag_corpus.get_db_session", return_value=source_session),
        patch("src.cli.inventory_rag_corpus.get_rag_index_session", return_value=index_session),
        patch.dict("src.cli.inventory_rag_corpus.build_rag_index._SOURCE_MAP", {"awards": source_chunks}),
    ):
        assert main(["--source", "awards", "--profile", "production", "--json"]) == 0

    index_session.execute.assert_called_once()


def test_inventory_passes_season_and_limit_to_source_iterators() -> None:
    """Mark bounded inventories incomplete and preserve iterator arguments."""
    source_session = MagicMock()
    source_session.__enter__.return_value = source_session
    index_session = MagicMock()
    index_session.__enter__.return_value = index_session
    index_session.execute.return_value.scalars.return_value.all.return_value = []
    calls = []

    def source_chunks(session, season, limit):
        calls.append((session, season, limit))
        return iter(({"source_table": "player_basic", "source_row_id": "1", "document_type": "player"},))

    with (
        patch("src.cli.inventory_rag_corpus.get_db_session", return_value=source_session),
        patch("src.cli.inventory_rag_corpus.get_rag_index_session", return_value=index_session),
        patch.dict("src.cli.inventory_rag_corpus.build_rag_index._SOURCE_MAP", {"players": source_chunks}),
    ):
        assert main(["--source", "players", "--season", "2025", "--limit", "1", "--json"]) == 0

    assert calls == [(source_session, 2025, 1)]
