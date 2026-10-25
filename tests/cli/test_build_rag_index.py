from __future__ import annotations

from contextlib import nullcontext
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import argparse
import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.cli import build_rag_index
from src.models.rag_chunk import RagChunk
from src.models.rankings import StatRanking
from src.services.rag_index_identity import chunk_content_hash


@pytest.mark.parametrize("season", [1988, 2025])
def test_game_season_filter_uses_regular_season_metadata(season) -> None:
    statement = str(build_rag_index._regular_season_ids(season))

    assert "kbo_seasons.season_year" in statement
    assert "kbo_seasons.league_type_code" in statement


@pytest.mark.parametrize(
    ("season_id", "expected"),
    [(None, None), (1988, 1988), (198800, 1988), (202500, 2025)],
)
def test_game_chunk_season_metadata_uses_season_year(season_id, expected) -> None:
    assert build_rag_index._season_year_from_id(season_id) == expected


def test_local_markdown_iterators_emit_the_three_vector_sources(tmp_path, monkeypatch):
    docs_root = tmp_path / "baseball"
    (docs_root / "kbo_rulebook" / "league_regulations").mkdir(parents=True)
    (docs_root / "glossary").mkdir()
    (docs_root / "kbo_knowledge").mkdir()
    (docs_root / "kbo_rulebook" / "league_regulations" / "rules.md").write_text(
        "# Rules\n\n## Article 1\n\nRegular season rule.",
        encoding="utf-8",
    )
    (docs_root / "glossary" / "terms.md").write_text(
        "# Terms\n\nBatting average means hits divided by at bats.",
        encoding="utf-8",
    )
    (docs_root / "kbo_knowledge" / "history.md").write_text(
        "# History\n\nKBO history and culture.",
        encoding="utf-8",
    )
    monkeypatch.setenv("KBO_MARKDOWN_DOCS_DIR", str(docs_root))

    rows = []
    for source in ("markdown_docs", "kbo_definitions", "kbo_regulations"):
        iterator = build_rag_index._SOURCE_MAP[source]
        rows.extend(iterator(None, None, None))

    assert {row["source_table"] for row in rows} == {
        "markdown_docs",
        "kbo_definitions",
        "kbo_regulations",
    }
    assert all(row["document_type"] == "markdown_doc" for row in rows)
    assert all(row["source_row_id"] for row in rows)
    assert "kbo_rulebook/league_regulations/rules.md_0" in {row["source_row_id"] for row in rows}


def test_local_markdown_iterator_honors_limit(tmp_path, monkeypatch):
    (tmp_path / "doc.md").write_text(
        "# Document\n\nFirst paragraph.\n\nSecond paragraph.",
        encoding="utf-8",
    )
    monkeypatch.setenv("KBO_MARKDOWN_DOCS_DIR", str(tmp_path))

    rows = list(build_rag_index._iter_markdown_chunks(None, None, 1))

    assert len(rows) == 1
    assert rows[0]["source_table"] == "markdown_docs"


def test_staging_rag_chunk_iterator_reembeds_content_without_reusing_vectors() -> None:
    """Read compatible staging schemas while leaving their old vectors behind."""
    session = MagicMock()
    session.execute.return_value.mappings.return_value = [
        {
            "id": 1,
            "source_table": "kbo_definitions",
            "source_row_id": "rules-1",
            "title": "규정",
            "content": "정규 시즌 규정 본문",
            "embedding": [0.1, 0.2],
            "metadata": {"document_type": "regulation"},
            "is_active": True,
        },
        {"id": 2, "content": "비활성", "is_active": False},
        {"id": 3, "content": "삭제됨", "index_status": "DELETED"},
    ]

    rows = list(build_rag_index._iter_staging_rag_chunks(session, None, None))

    assert len(rows) == 1
    assert rows[0]["content"] == "정규 시즌 규정 본문"
    assert rows[0]["document_type"] == "regulation"
    assert "embedding" not in rows[0]


def test_staging_rag_chunk_iterator_can_skip_populated_oracle_vectors() -> None:
    """Avoid materializing populated single-store Oracle vectors during resume."""
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "oracle"
    session.execute.return_value.mappings.return_value = []

    rows = list(build_rag_index._iter_staging_rag_chunks(session, None, None, skip_populated=True))

    assert rows == []
    statement = str(session.execute.call_args.args[0])
    assert "embedding_vector IS NULL" in statement


def test_staging_candidate_iterator_uses_lightweight_projection() -> None:
    """Avoid selecting vector payloads during content-aware staging selection."""
    session = MagicMock()
    session.execute.return_value.mappings.return_value = [
        {
            "id": 1,
            "source_table": "player_basic",
            "source_row_id": "1",
            "title": "선수",
            "content": "내용",
            "team_id": "LG",
            "player_id": "1",
            "season_year": 2025,
            "document_type": "player_profile",
            "source_url": "https://example.test/player/1",
            "is_active": True,
        }
    ]

    rows = list(build_rag_index._iter_staging_candidate_chunks(session, None, None))

    assert rows[0]["team_id"] == "LG"
    assert rows[0]["player_id"] == "1"
    assert rows[0]["season_year"] == 2025
    assert rows[0]["document_type"] == "player_profile"
    statement = str(session.execute.call_args.args[0])
    assert "embedding" not in statement
    assert "content_hash" in statement
    assert "team_id" in statement


def test_staging_candidate_iterator_rolls_back_before_legacy_fallback() -> None:
    """Reset a failed lightweight query and re-raise (no SELECT * fallback, OOM risk)."""
    session = MagicMock()
    session.execute.side_effect = SQLAlchemyError("missing optional column")

    with pytest.raises(SQLAlchemyError):
        list(build_rag_index._iter_staging_candidate_chunks(session, None, None))

    session.rollback.assert_called_once_with()


def test_staging_query_applies_keyset_cursor() -> None:
    """Resume the staging scan past rows the previous run already consumed."""
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "postgresql"

    without_cursor = str(build_rag_index._staging_query(session, skip_populated=True, lightweight=True))
    with_cursor = str(build_rag_index._staging_query(session, skip_populated=True, lightweight=True, last_id=42))

    assert "id > 42" not in without_cursor
    assert "id > 42" in with_cursor
    assert "embedding IS NULL" in with_cursor
    assert "AND id > 42" in with_cursor


def test_staging_cursor_round_trips_through_the_file(monkeypatch, tmp_path) -> None:
    """Persist and read the keyset cursor so a crashed scan can resume."""
    monkeypatch.delenv("RAG_STAGING_LAST_ID", raising=False)
    cursor_file = tmp_path / "staging_cursor.json"
    monkeypatch.setenv("RAG_STAGING_CURSOR_FILE", str(cursor_file))

    build_rag_index._save_staging_cursor(1234)

    assert build_rag_index._read_staging_cursor() == 1234


def test_staging_cursor_prefers_the_environment_override(monkeypatch, tmp_path) -> None:
    """An explicit env cursor wins so an operator can replay or rewind."""
    cursor_file = tmp_path / "staging_cursor.json"
    cursor_file.write_text('{"last_id": 10}', encoding="utf-8")
    monkeypatch.setenv("RAG_STAGING_CURSOR_FILE", str(cursor_file))
    monkeypatch.setenv("RAG_STAGING_LAST_ID", "77")

    assert build_rag_index._read_staging_cursor() == 77


@pytest.mark.parametrize("payload", ["{not json", '{"last_id": "nope"}'])
def test_staging_cursor_treats_a_corrupt_file_as_unset(monkeypatch, tmp_path, payload) -> None:
    """A damaged cursor must not stop a build -- restart from the beginning."""
    monkeypatch.delenv("RAG_STAGING_LAST_ID", raising=False)
    cursor_file = tmp_path / "staging_cursor.json"
    cursor_file.write_text(payload, encoding="utf-8")
    monkeypatch.setenv("RAG_STAGING_CURSOR_FILE", str(cursor_file))

    assert build_rag_index._read_staging_cursor() == 0


def test_staging_cursor_reseeds_per_call(monkeypatch, tmp_path) -> None:
    """A short run must not inherit an earlier call's high-water mark.

    The holder is module-level, so without re-seeding, a second call in the same
    process would keep the previous (larger) value and save it -- skipping rows
    that were never processed. Silent data loss, so it is pinned here.
    """
    monkeypatch.delenv("RAG_STAGING_LAST_ID", raising=False)
    cursor_file = tmp_path / "staging_cursor.json"
    cursor_file.write_text('{"last_id": 100}', encoding="utf-8")
    monkeypatch.setenv("RAG_STAGING_CURSOR_FILE", str(cursor_file))

    session = MagicMock()
    session.execute.return_value.mappings.return_value = [
        {"id": 150, "source_table": "player_basic", "source_row_id": "1", "content": "본문", "is_active": True}
    ]

    build_rag_index._STAGING_CURSOR_MAX_ID[0] = 999  # leakage from a previous call
    rows = list(build_rag_index._iter_staging_candidate_chunks(session, None, None))

    assert len(rows) == 1
    assert build_rag_index._STAGING_CURSOR_MAX_ID[0] == 150


def test_staging_cursor_seeds_from_file_when_nothing_is_read(monkeypatch, tmp_path) -> None:
    """A query that yields nothing must leave the cursor where it was found."""
    monkeypatch.delenv("RAG_STAGING_LAST_ID", raising=False)
    cursor_file = tmp_path / "staging_cursor.json"
    cursor_file.write_text('{"last_id": 100}', encoding="utf-8")
    monkeypatch.setenv("RAG_STAGING_CURSOR_FILE", str(cursor_file))

    session = MagicMock()
    session.execute.return_value.mappings.return_value = []

    build_rag_index._STAGING_CURSOR_MAX_ID[0] = 999
    assert list(build_rag_index._iter_staging_candidate_chunks(session, None, None)) == []

    assert build_rag_index._STAGING_CURSOR_MAX_ID[0] == 100


def test_staging_cursor_does_not_advance_for_an_abandoned_scan(monkeypatch, tmp_path) -> None:
    """An interrupted scan must resume from where it started, never skip ahead.

    The embedding call can fail mid-batch. The generator is then dropped without
    running its final bookkeeping, so the cursor stays put and the next run
    re-reads the same rows (the upserts are idempotent). Advancing here would
    turn a transient API error into permanently unindexed rows.
    """
    monkeypatch.delenv("RAG_STAGING_LAST_ID", raising=False)
    cursor_file = tmp_path / "staging_cursor.json"
    cursor_file.write_text('{"last_id": 100}', encoding="utf-8")
    monkeypatch.setenv("RAG_STAGING_CURSOR_FILE", str(cursor_file))

    session = MagicMock()
    session.execute.return_value.mappings.return_value = [
        {"id": 150, "source_table": "player_basic", "source_row_id": "1", "content": "본문", "is_active": True},
        {"id": 160, "source_table": "player_basic", "source_row_id": "2", "content": "다음", "is_active": True},
    ]

    scan = build_rag_index._iter_staging_candidate_chunks(session, None, None)
    assert next(scan)["source_row_id"] == "1"
    assert build_rag_index._STAGING_CURSOR_MAX_ID[0] == 100
    scan.close()

    assert build_rag_index._STAGING_CURSOR_MAX_ID[0] == 100


def test_prepare_staging_chunks_uses_populated_vector_filter() -> None:
    """Apply the single-store staging optimization through source preparation."""
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "oracle"
    session.execute.return_value.mappings.return_value = []

    prepared = build_rag_index._prepare_source_chunks(
        "staging_rag_chunks",
        build_rag_index._iter_staging_missing_rag_chunks,
        session,
        None,
        None,
    )

    assert list(prepared) == []
    assert "embedding_vector IS NULL" in str(session.execute.call_args.args[0])


def test_rankings_iterator_orders_ties_deterministically() -> None:
    """Include stable tie-break columns in ranking source queries."""
    query = MagicMock()
    query.order_by.return_value = query
    query.yield_per.return_value = []
    session = MagicMock()
    session.query.return_value = query

    list(build_rag_index._iter_rankings_chunks(session, None, None))

    order_columns = query.order_by.call_args.args
    assert order_columns[-3] is StatRanking.entity_label
    assert order_columns[-2] is StatRanking.entity_id
    assert order_columns[-1] is StatRanking.team_id


def test_deterministic_embedding_mode_is_available_for_staging() -> None:
    """Build the non-network provider used only for infrastructure acceptance."""
    service = build_rag_index._embedding_service("deterministic")

    assert service.dimension == 1536
    assert len(service.get_embedding("staging smoke")) == 1536


def test_process_source_persists_using_index_session(monkeypatch) -> None:
    index_session = MagicMock(name="index_session")
    embedding_service = MagicMock()
    embedding_service.get_embeddings_batch.return_value = [[0.1]]
    persisted = []

    monkeypatch.setattr(
        build_rag_index,
        "_persist_index_batch",
        lambda batch, session: persisted.append((batch, session)),
    )

    count = build_rag_index._process_source(
        "players",
        iter([{"source_table": "player_basic", "source_row_id": "1", "content": "player"}]),
        embedding_service,
        index_session,
        dry_run=False,
    )

    assert count == 1
    assert persisted[0][1] is index_session
    assert persisted[0][0][0]["content_hash"] == chunk_content_hash(None, "player")


def test_count_only_names_the_mode_so_a_count_is_never_read_as_a_write() -> None:
    """The final line must say no embedding call and no write happened."""
    args = argparse.Namespace(count_only=True, dry_run=True)

    assert "count-only" in build_rag_index._completion_suffix(args)
    assert "임베딩 호출 없음" in build_rag_index._completion_suffix(args)


def test_real_run_has_no_completion_suffix() -> None:
    """A real build keeps its original, unmarked completion line."""
    args = argparse.Namespace(count_only=False, dry_run=False)

    assert build_rag_index._completion_suffix(args) == ""


def test_process_source_recomputes_hash_and_records_embedding_fingerprint(monkeypatch) -> None:
    index_session = MagicMock(name="index_session")
    embedding_service = MagicMock(model_name="model-a", dimension=1536)
    embedding_service.get_embeddings_batch.return_value = [[0.1]]
    persisted = []
    monkeypatch.setenv("RAG_CHUNKING_VERSION", "chunk-v1")
    monkeypatch.setattr(
        build_rag_index,
        "_persist_index_batch",
        lambda batch, session: persisted.append((batch, session)),
    )

    build_rag_index._process_source(
        "players",
        iter([{"source_table": "player_basic", "source_row_id": "1", "title": "선수", "content": "내용"}]),
        embedding_service,
        index_session,
        dry_run=False,
    )

    chunk = persisted[0][0][0]
    assert chunk["content_hash"] == chunk_content_hash("선수", "내용")
    assert chunk["meta"]["embedding_fingerprint"] == "model-a:1536:chunk-v1"


def test_persist_index_batch_routes_by_actual_index_dialect(monkeypatch) -> None:
    """Route writes using the opened sparse session rather than the source dialect."""
    batch = [{"source_table": "game", "source_row_id": "1", "content": "본문"}]
    oracle_session = MagicMock()
    oracle_session.get_bind.return_value.dialect.name = "oracle"
    single_store = MagicMock()
    monkeypatch.setattr("src.services.rag_index_propagation.publish_single_store_batch", single_store)

    build_rag_index._persist_index_batch(batch, oracle_session)

    single_store.assert_called_once_with(oracle_session, batch)

    postgres_session = MagicMock()
    postgres_session.get_bind.return_value.dialect.name = "postgresql"
    vector_session = MagicMock()
    publish_index_batch = MagicMock()
    monkeypatch.setattr("src.db.vector_engine.get_vector_session", lambda: nullcontext(vector_session))
    monkeypatch.setattr("src.services.rag_index_propagation.publish_index_batch", publish_index_batch)

    build_rag_index._persist_index_batch(batch, postgres_session)

    publish_index_batch.assert_called_once_with(postgres_session, vector_session, batch)


def test_persist_index_batch_uses_the_single_store_when_the_dense_index_is_here(monkeypatch) -> None:
    """A shared store has no second ``rag_chunks``, so the dense row builder must never run."""
    batch = [{"source_table": "game", "source_row_id": "1", "content": "본문"}]
    postgres_session = MagicMock()
    postgres_session.get_bind.return_value.dialect.name = "postgresql"
    single_store = MagicMock()
    monkeypatch.setattr("src.services.rag_index_propagation.publish_single_store_batch", single_store)
    monkeypatch.setattr("src.db.vector_engine.vector_store_shares_database_with", lambda engine: True)

    def _refuse_second_session():
        message = "a colocated dense index must not open a second session"
        raise AssertionError(message)

    monkeypatch.setattr("src.db.vector_engine.get_vector_session", _refuse_second_session)

    build_rag_index._persist_index_batch(batch, postgres_session)

    single_store.assert_called_once_with(postgres_session, batch)


def test_skip_existing_index_rows_filters_healthy_identities() -> None:
    """Resume a source build without re-writing healthy vectors."""
    index_session = MagicMock()
    index_session.execute.return_value.all.return_value = [
        ("player_basic", "1", chunk_content_hash("", ""), "rag-v1", "ACTIVE", True, {})
    ]
    chunks = iter(
        [
            {"source_table": "player_basic", "source_row_id": "1", "content": ""},
            {"source_table": "player_basic", "source_row_id": "2", "content": ""},
        ],
    )

    result = list(build_rag_index._skip_existing_index_rows(chunks, index_session))

    assert result == [{"source_table": "player_basic", "source_row_id": "2", "content": ""}]


def test_full_rebuild_yields_healthy_rows_but_blocks_terminal_rows() -> None:
    """Keep full rebuild semantics while preventing tombstone reactivation."""
    index_session = MagicMock()
    index_session.execute.return_value.all.return_value = [
        ("player_basic", "1", chunk_content_hash("", "same"), "rag-v1", "ACTIVE", True, {}),
        ("player_basic", "2", chunk_content_hash("", "deleted"), "rag-v1", "DELETED", False, {}),
    ]
    chunks = iter(
        [
            {"source_table": "player_basic", "source_row_id": "1", "content": "same"},
            {"source_table": "player_basic", "source_row_id": "2", "content": "deleted"},
        ],
    )

    result = list(
        build_rag_index._skip_existing_index_rows(
            chunks,
            index_session,
            build_rag_index.IncrementalSelectionOptions(include_healthy=True),
        )
    )

    assert result == [{"source_table": "player_basic", "source_row_id": "1", "content": "same"}]


def test_full_rebuild_reports_but_does_not_apply_metadata_only_gap() -> None:
    """Leave a legacy fingerprint gap alone unless the repair gate is opened."""
    index_session = MagicMock()
    index_session.execute.return_value.all.return_value = [
        ("player_basic", "1", chunk_content_hash("", "same"), "rag-v1", "ACTIVE", True, {})
    ]
    chunks = iter([{"source_table": "player_basic", "source_row_id": "1", "content": "same"}])

    result = list(
        build_rag_index._skip_existing_index_rows(
            chunks,
            index_session,
            build_rag_index.IncrementalSelectionOptions(
                include_healthy=True,
                desired_embedding_fingerprint="model-a:1536:rag-v1",
            ),
        )
    )

    assert result == []


def test_repair_gate_reembeds_a_legacy_fingerprint_row() -> None:
    """Open the gate so a fingerprint-less row is re-embedded, even under --skip-existing."""
    index_session = MagicMock()
    index_session.execute.return_value.all.return_value = [
        ("player_basic", "1", chunk_content_hash("", "same"), "rag-v1", "ACTIVE", True, {})
    ]
    chunks = iter([{"source_table": "player_basic", "source_row_id": "1", "content": "same"}])

    result = list(
        build_rag_index._skip_existing_index_rows(
            chunks,
            index_session,
            build_rag_index.IncrementalSelectionOptions(
                include_healthy=False,
                desired_embedding_fingerprint="model-a:1536:rag-v1",
                repair_legacy_fingerprint=True,
            ),
        )
    )

    assert result == [{"source_table": "player_basic", "source_row_id": "1", "content": "same"}]


def test_repair_gate_leaves_a_current_fingerprint_row_alone() -> None:
    """Keep the gate narrow: a row already labelled for this model is not re-embedded."""
    index_session = MagicMock()
    index_session.execute.return_value.all.return_value = [
        (
            "player_basic",
            "1",
            chunk_content_hash("", "same"),
            "rag-v1",
            "ACTIVE",
            True,
            {"embedding_fingerprint": "model-a:1536:rag-v1"},
        )
    ]
    chunks = iter([{"source_table": "player_basic", "source_row_id": "1", "content": "same"}])

    result = list(
        build_rag_index._skip_existing_index_rows(
            chunks,
            index_session,
            build_rag_index.IncrementalSelectionOptions(
                include_healthy=False,
                desired_embedding_fingerprint="model-a:1536:rag-v1",
                repair_legacy_fingerprint=True,
            ),
        )
    )

    assert result == []


def test_repair_gate_does_not_resurrect_a_terminal_identity() -> None:
    """A blocked row stays blocked: the gate repairs metadata, not lifecycle state."""
    index_session = MagicMock()
    index_session.execute.return_value.all.return_value = [
        ("player_basic", "1", chunk_content_hash("", "gone"), "rag-v1", "DELETED", False, {})
    ]
    chunks = iter([{"source_table": "player_basic", "source_row_id": "1", "content": "gone"}])

    result = list(
        build_rag_index._skip_existing_index_rows(
            chunks,
            index_session,
            build_rag_index.IncrementalSelectionOptions(
                include_healthy=False,
                desired_embedding_fingerprint="model-a:1536:rag-v1",
                repair_legacy_fingerprint=True,
            ),
        )
    )

    assert result == []


def test_iter_candidate_chunks_passes_the_repair_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Carry the repair gate from BuildSourceOptions into the selector."""
    captured: dict[str, object] = {}

    def _capture(chunk_iter: object, index_session: object, options: object = None, **kwargs: object) -> object:
        captured["options"] = options
        return iter([])

    monkeypatch.setattr(build_rag_index, "_prepare_source_chunks", lambda *args, **kwargs: iter([]))
    monkeypatch.setattr(build_rag_index, "_skip_existing_index_rows", _capture)

    source_name = next(iter(build_rag_index._SOURCE_MAP))
    list(
        build_rag_index._iter_candidate_chunks(
            source_name,
            MagicMock(),
            MagicMock(),
            None,
            build_rag_index.BuildSourceOptions(skip_existing=True, repair_legacy_fingerprint=True),
        )
    )

    options = captured["options"]
    assert options is not None
    assert options.repair_legacy_fingerprint is True


def test_skip_existing_index_rows_reads_real_sqlalchemy_projection() -> None:
    """Use the production-shaped projection against an ephemeral SQLite table."""
    engine = create_engine("sqlite://")
    RagChunk.__table__.create(engine)
    with Session(engine) as session:
        session.add(
            RagChunk(
                source_table="player_basic",
                source_row_id="1",
                title="",
                content="same",
                content_hash=chunk_content_hash("", "same"),
                index_version="rag-v1",
                index_status="ACTIVE",
                embedding=[0.1],
                meta={},
            )
        )
        session.commit()

        result = list(
            build_rag_index._skip_existing_index_rows(
                iter([{"source_table": "player_basic", "source_row_id": "1", "content": "same"}]),
                session,
            )
        )

    assert result == []


def test_skip_existing_index_rows_selects_changed_content() -> None:
    """Re-embed a populated identity when its canonical content changes."""
    index_session = MagicMock()
    index_session.execute.return_value.all.return_value = [
        ("player_basic", "1", chunk_content_hash("", "old"), "rag-v1", "ACTIVE", True, {})
    ]
    chunks = iter([{"source_table": "player_basic", "source_row_id": "1", "content": "new"}])

    result = list(build_rag_index._skip_existing_index_rows(chunks, index_session))

    assert result == [{"source_table": "player_basic", "source_row_id": "1", "content": "new"}]


def test_skip_existing_index_rows_selects_missing_vector_state() -> None:
    """Re-embed when a split vector store has no row for a sparse identity."""
    index_session = MagicMock()
    index_session.execute.return_value.all.return_value = [
        ("player_basic", "1", chunk_content_hash("", "same"), "rag-v1", "ACTIVE", True, {})
    ]
    vector_session = MagicMock()
    vector_session.execute.return_value.all.return_value = []
    chunks = iter([{"source_table": "player_basic", "source_row_id": "1", "content": "same"}])

    result = list(
        build_rag_index._skip_existing_index_rows(
            chunks,
            index_session,
            build_rag_index.IncrementalSelectionOptions(vector_session=vector_session),
        )
    )

    assert result == [{"source_table": "player_basic", "source_row_id": "1", "content": "same"}]


def test_skip_existing_index_rows_uses_vector_state_when_sparse_embedding_is_empty() -> None:
    """Treat a split vector row as authoritative when sparse JSON is empty."""
    index_session = MagicMock()
    index_session.execute.return_value.all.return_value = [
        ("player_basic", "1", chunk_content_hash("", "same"), "rag-v1", "ACTIVE", False, {})
    ]
    vector_session = MagicMock()
    vector_session.execute.return_value.all.return_value = [
        ("player_basic", "1", chunk_content_hash("", "same"), "rag-v1", "ACTIVE", True, {})
    ]
    chunks = iter([{"source_table": "player_basic", "source_row_id": "1", "content": "same"}])

    result = list(
        build_rag_index._skip_existing_index_rows(
            chunks,
            index_session,
            build_rag_index.IncrementalSelectionOptions(vector_session=vector_session),
        )
    )

    assert result == []


def test_skip_existing_index_rows_scopes_lookup_to_source_table() -> None:
    """Limit the populated-vector lookup to the source currently being built."""
    index_session = MagicMock()
    index_session.execute.return_value.all.return_value = []

    list(
        build_rag_index._skip_existing_index_rows(
            iter(()),
            index_session,
            build_rag_index.IncrementalSelectionOptions(source_table="player_basic"),
        ),
    )

    statement = index_session.execute.call_args.args[0]
    compiled = str(statement.compile(compile_kwargs={"literal_binds": True}))
    assert "player_basic" in compiled


def test_prepare_long_database_sources_releases_session() -> None:
    session = MagicMock(name="source_session")
    chunks = [{"source_table": "game_play_by_play", "source_row_id": "1"}]

    prepared = build_rag_index._prepare_source_chunks(
        "pbp",
        lambda _session, _season, _limit: iter(chunks),
        session,
        None,
        None,
    )

    assert list(prepared) == chunks
    session.close.assert_called_once_with()


def test_validate_embeddings_rejects_zero_vectors() -> None:
    with pytest.raises(RuntimeError, match="zero vector"):
        build_rag_index._validate_embeddings([[0.0, 0.0]], 1)


def test_validate_embeddings_rejects_incomplete_batches() -> None:
    with pytest.raises(RuntimeError, match="returned 1 vectors for 2 chunks"):
        build_rag_index._validate_embeddings([[0.1, 0.2]], 2)


def test_build_targets_redact_credentials_and_allow_dry_run(monkeypatch) -> None:
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://vector:secret@127.0.0.1:5432/rag_vector")
    monkeypatch.setenv("RAG_TARGET_ENV", "staging")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")

    targets = build_rag_index._resolve_build_targets(
        "oracle+oracledb://app:secret@kbo_medium",
        embedding_mode="configured",
        dry_run=True,
    )

    assert targets.display() == {
        "source_db": "oracle+oracledb://kbo_medium",
        "sparse_index_db": "oracle+oracledb://kbo_medium",
        "vector_db": "oracle+oracledb://kbo_medium",
        "target_environment": "staging",
        "write_enabled": False,
    }


def test_build_targets_reject_shared_write_target(monkeypatch) -> None:
    monkeypatch.setenv("RAG_INDEX_DB_URL", "oracle+oracledb://app:secret@kbo_medium")
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://127.0.0.1:5432/rag_vector")
    monkeypatch.setenv("RAG_TARGET_ENV", "staging")
    monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")

    with pytest.raises(ValueError, match="source and sparse index targets must be different"):
        build_rag_index._resolve_build_targets(
            "oracle+oracledb://other:password@kbo_medium",
            embedding_mode="configured",
            dry_run=False,
        )


def test_oracle_staging_build_allows_postgresql_index_targets(monkeypatch) -> None:
    """Allow Oracle source reads to publish into isolated PostgreSQL staging indexes."""
    monkeypatch.setenv("RAG_INDEX_DB_URL", "postgresql://127.0.0.1:5432/rag_sparse")
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://127.0.0.1:5432/rag_vector")
    monkeypatch.setenv("RAG_TARGET_ENV", "staging")
    monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")

    targets = build_rag_index._resolve_build_targets(
        "oracle+oracledb://app:secret@kbo_medium",
        embedding_mode="configured",
        dry_run=False,
    )

    assert targets.sparse_index_db == "postgresql://127.0.0.1:5432/rag_sparse"
    assert targets.vector_db == "postgresql://127.0.0.1:5432/rag_vector"


def test_oracle_source_rejects_non_postgres_split_vector_target(monkeypatch) -> None:
    """Prevent a PostgreSQL sparse target from being paired with an Oracle vector URL."""
    monkeypatch.setenv("RAG_INDEX_DB_URL", "postgresql://127.0.0.1:5432/rag_sparse")
    monkeypatch.setenv("PGVECTOR_URL", "oracle+oracledb://app:secret@kbo_medium")
    monkeypatch.setenv("RAG_TARGET_ENV", "staging")
    monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")

    with pytest.raises(ValueError, match="PostgreSQL vector URL"):
        build_rag_index._resolve_build_targets(
            "oracle+oracledb://source:secret@kbo_source",
            embedding_mode="configured",
            dry_run=False,
        )


def test_postgresql_production_build_requires_the_production_opt_in(monkeypatch) -> None:
    """Keep a non-Oracle production write closed until its own guard is set.

    The engine is no longer what closes this door -- the vector store's move to
    pgvector made "production" and "Oracle" separate questions. What closes it
    is the opt-in, which the pgvector path now needs exactly as the Oracle path
    always did.
    """
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://vector:secret@127.0.0.1:5432/rag_vector")
    monkeypatch.setenv("RAG_TARGET_ENV", "production")
    monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
    monkeypatch.delenv("RAG_INDEX_ALLOW_PRODUCTION_WRITE", raising=False)
    monkeypatch.delenv("RAG_INDEX_DB_URL", raising=False)
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")

    with pytest.raises(ValueError, match="RAG_INDEX_ALLOW_PRODUCTION_WRITE"):
        build_rag_index._resolve_build_targets(
            "postgresql://source:secret@127.0.0.1:5432/bega_prod",
            embedding_mode="configured",
            dry_run=False,
        )


def test_postgresql_production_build_is_allowed_with_both_guards(monkeypatch) -> None:
    """Accept the deployment the vector store now lives in.

    The sparse index stays in the operational database beside its source rows,
    which is where it has always been, so a production build may leave
    ``RAG_INDEX_DB_URL`` unset rather than naming a third database.
    """
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://vector:secret@127.0.0.1:5432/rag_vector")
    monkeypatch.setenv("RAG_TARGET_ENV", "production")
    monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
    monkeypatch.setenv("RAG_INDEX_ALLOW_PRODUCTION_WRITE", "1")
    monkeypatch.delenv("RAG_INDEX_DB_URL", raising=False)
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")

    targets = build_rag_index._resolve_build_targets(
        "postgresql://source:secret@127.0.0.1:5432/bega_prod",
        embedding_mode="configured",
        dry_run=False,
    )
    assert targets.vector_db == "postgresql://vector:secret@127.0.0.1:5432/rag_vector"


def test_separate_oracle_sparse_target_reports_itself_as_vector_store(monkeypatch) -> None:
    """Keep an explicitly configured Oracle index and vector target aligned."""
    for key in ("PGVECTOR_URL", "PGVECTOR_TEST_URL"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("RAG_INDEX_DB_URL", "oracle+oracledb://app:secret@separate_sparse")
    monkeypatch.setenv("RAG_TARGET_ENV", "staging")
    monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")

    targets = build_rag_index._resolve_build_targets(
        "oracle+oracledb://source:secret@source_db",
        target_db_url="oracle+oracledb://app:secret@primary_db",
        embedding_mode="configured",
        dry_run=False,
    )

    assert targets.sparse_index_db == "oracle+oracledb://app:secret@separate_sparse"
    assert targets.vector_db == "oracle+oracledb://app:secret@separate_sparse"


def test_oracle_build_uses_one_database_for_sparse_and_vector_targets(monkeypatch) -> None:
    """Allow Oracle AI Vector Search without PostgreSQL target variables."""
    for key in ("PGVECTOR_URL", "PGVECTOR_TEST_URL", "RAG_INDEX_DB_URL"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("RAG_TARGET_ENV", "staging")
    monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")

    targets = build_rag_index._resolve_build_targets(
        "oracle+oracledb://app:secret@kbo_medium",
        embedding_mode="configured",
        dry_run=False,
    )

    assert targets.sparse_index_db == "oracle+oracledb://app:secret@kbo_medium"
    assert targets.vector_db == "oracle+oracledb://app:secret@kbo_medium"
    assert targets.write_enabled is True


def test_postgresql_source_can_publish_to_oracle_target(monkeypatch) -> None:
    """Keep source reads separate from the Oracle single-store write target."""
    for key in ("PGVECTOR_URL", "PGVECTOR_TEST_URL", "RAG_INDEX_DB_URL"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("RAG_TARGET_ENV", "production")
    monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
    monkeypatch.setenv("RAG_INDEX_ALLOW_PRODUCTION_WRITE", "1")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")

    targets = build_rag_index._resolve_build_targets(
        "postgresql://source:secret@127.0.0.1:5432/staging",
        target_db_url="oracle+oracledb://app:secret@kbo_medium",
        embedding_mode="configured",
        dry_run=False,
    )

    assert targets.source_db == "postgresql://source:secret@127.0.0.1:5432/staging"
    assert targets.sparse_index_db == "oracle+oracledb://app:secret@kbo_medium"
    assert targets.vector_db == "oracle+oracledb://app:secret@kbo_medium"


def test_oracle_production_build_requires_explicit_write_gate(monkeypatch) -> None:
    """Keep production Oracle writes behind a separate explicit safety flag."""
    for key in ("PGVECTOR_URL", "PGVECTOR_TEST_URL", "RAG_INDEX_DB_URL", "RAG_INDEX_ALLOW_PRODUCTION_WRITE"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("RAG_TARGET_ENV", "production")
    monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")

    with pytest.raises(ValueError, match="RAG_INDEX_ALLOW_PRODUCTION_WRITE"):
        build_rag_index._resolve_build_targets(
            "oracle+oracledb://app:secret@kbo_medium",
            embedding_mode="configured",
            dry_run=False,
        )


def test_deterministic_build_requires_staging_environment(monkeypatch) -> None:
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://127.0.0.1:5432/rag_vector")
    monkeypatch.delenv("RAG_TARGET_ENV", raising=False)

    with pytest.raises(ValueError, match="deterministic embedding requires RAG_TARGET_ENV=staging"):
        build_rag_index._resolve_build_targets(
            "oracle+oracledb://kbo_medium",
            embedding_mode="deterministic",
            dry_run=True,
        )


def test_vector_presence_is_refused_without_skip_existing() -> None:
    """Without the skip filter every chunk is yielded, so key presence would be ignored silently."""
    with pytest.raises(SystemExit) as raised:
        build_rag_index.main(["--vector-presence"])

    assert raised.value.code == 2


def test_content_presence_is_refused_without_skip_existing() -> None:
    """Content presence only means something on the skip filter path."""
    with pytest.raises(SystemExit) as raised:
        build_rag_index.main(["--content-presence"])

    assert raised.value.code == 2


def test_count_candidates_writes_one_json_line_per_chunk_without_embedding(tmp_path) -> None:
    """Listing candidates is for sampling: it writes the rows and never calls a provider."""
    import json

    chunks = iter(
        [
            {
                "source_table": "game_play_by_play",
                "source_row_id": "20260816OBHT0_7",
                "title": "t1",
                "content": "본문1",
            },
            {"source_table": "game_play_by_play", "source_row_id": "1680134", "title": "t2", "content": "본문2"},
        ]
    )
    target = tmp_path / "candidates.jsonl"

    count = build_rag_index._count_candidates(chunks, target)

    assert count == 2
    lines = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines()]
    assert [row["source_row_id"] for row in lines] == ["20260816OBHT0_7", "1680134"]
    assert lines[0]["content"] == "본문1"


def test_count_candidates_without_a_path_only_counts(tmp_path) -> None:
    """The default path counts and writes nothing."""
    count = build_rag_index._count_candidates(iter([{"content": "a"}, {"content": "b"}]), None)

    assert count == 2
    assert list(tmp_path.iterdir()) == []


def test_list_candidates_is_refused_without_count_only() -> None:
    """The listing belongs to the read-only path only."""
    with pytest.raises(SystemExit) as raised:
        build_rag_index.main(["--list-candidates", "x.jsonl"])

    assert raised.value.code == 2


def test_drop_excluded_removes_only_listed_identities() -> None:
    """The exclusion is applied to candidates, so count, list, and apply all see the same set."""
    chunks = [{"source_row_id": "A"}, {"source_row_id": "B"}, {"source_row_id": "C"}]

    kept = list(build_rag_index._drop_excluded(iter(chunks), frozenset({"B"})))

    assert [chunk["source_row_id"] for chunk in kept] == ["A", "C"]


def test_read_exclude_ids_skips_blank_lines_and_comments(tmp_path) -> None:
    """One identity per line; blank lines and comments are not identities."""
    source = tmp_path / "exclude.txt"
    source.write_text("# 322 matched plays\n20260816DBKIA0_7\n\n20260816DBKIA0_9\n", encoding="utf-8")

    assert build_rag_index._read_exclude_ids(str(source)) == frozenset({"20260816DBKIA0_7", "20260816DBKIA0_9"})
    assert build_rag_index._read_exclude_ids(None) == frozenset()


def test_exclude_file_is_refused_for_other_sources() -> None:
    """Only the PBP path is covered by the verified exclusion list."""
    with pytest.raises(SystemExit) as raised:
        build_rag_index.main(["--source", "lineups", "--count-only", "--skip-existing", "--exclude-file", "x.txt"])

    assert raised.value.code == 2


def test_play_presence_is_refused_without_skip_existing_or_for_other_sources() -> None:
    """The play filter only means something on the PBP skip path."""
    with pytest.raises(SystemExit) as no_skip:
        build_rag_index.main(["--source", "pbp", "--play-presence"])
    with pytest.raises(SystemExit) as other_source:
        build_rag_index.main(["--source", "lineups", "--skip-existing", "--play-presence"])

    assert no_skip.value.code == 2
    assert other_source.value.code == 2


def _lineup_chunks(rows: list[tuple[SimpleNamespace, SimpleNamespace]]) -> list[dict]:
    """Run the lineup generator over in-memory rows instead of a live session."""

    class _Query:
        def join(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def yield_per(self, size):
            return iter(rows)

    class _Session:
        def query(self, *models):
            return _Query()

    return list(build_rag_index._iter_lineup_chunks(_Session(), None, None))


def test_lineup_chunk_omits_a_missing_batting_order_instead_of_printing_none() -> None:
    """2001 has 2,201 lineup rows with no slot; the chunk must not assert one."""
    game = SimpleNamespace(game_date=date(2001, 4, 5), season_id=200100, home_team="OB", away_team="LG")
    rows = [
        (
            SimpleNamespace(
                game_id="20010405OBLG0",
                team_side="home",
                team_code="OB",
                batting_order=None,
                player_name="김동주",
                standard_position="3루수",
                position=None,
                is_starter=False,
                appearance_seq=1,
            ),
            game,
        )
    ]

    chunk = _lineup_chunks(rows)[0]

    assert "None번" not in chunk["content"]
    assert "[교체]" not in chunk["content"]
    assert "김동주 (3루수)" in chunk["content"]


def test_lineup_chunk_keeps_slot_and_substitution_marker_when_the_row_supports_both() -> None:
    """A row that really carries a slot and a bench flag keeps both facts."""
    game = SimpleNamespace(game_date=date(2026, 5, 1), season_id=202600, home_team="LG", away_team="HH")
    rows = [
        (
            SimpleNamespace(
                game_id="20260501LGHH0",
                team_side="home",
                team_code="LG",
                batting_order=3,
                player_name="홍창기",
                standard_position="우익수",
                position=None,
                is_starter=True,
                appearance_seq=1,
            ),
            game,
        ),
        (
            SimpleNamespace(
                game_id="20260501LGHH0",
                team_side="home",
                team_code="LG",
                batting_order=5,
                player_name="문보경",
                standard_position="1루수",
                position=None,
                is_starter=False,
                appearance_seq=2,
            ),
            game,
        ),
    ]

    chunk = _lineup_chunks(rows)[0]

    assert "3번 홍창기 (우익수)" in chunk["content"]
    assert "5번 문보경 (1루수) [교체]" in chunk["content"]
    assert chunk["source_row_id"] == "20260501LGHH0_home"


def test_lineup_chunk_drops_an_unknown_position_sentinel() -> None:
    """Every 2026 lineup row stores the literal string UNKNOWN, not NULL.

    `standard_position or position` therefore never fell through, and the index
    recorded "(UNKNOWN)" as if it were a fielding position (1,462 chunks).
    """
    game = SimpleNamespace(game_date=date(2026, 3, 19), season_id=202600, home_team="SS", away_team="NC")
    rows = [
        (
            SimpleNamespace(
                game_id="20260319SSNC0",
                team_side="away",
                team_code="SS",
                batting_order=1,
                player_name="김영웅",
                standard_position="UNKNOWN",
                position=None,
                is_starter=True,
                appearance_seq=1,
            ),
            game,
        )
    ]

    chunk = _lineup_chunks(rows)[0]

    assert "UNKNOWN" not in chunk["content"]
    assert "1번 김영웅" in chunk["content"]


def test_lineup_chunk_falls_through_the_sentinel_to_the_next_position_column() -> None:
    """A sentinel in one column must not hide a real value in the other."""
    game = SimpleNamespace(game_date=date(2026, 3, 19), season_id=202600, home_team="SS", away_team="NC")
    rows = [
        (
            SimpleNamespace(
                game_id="20260319SSNC0",
                team_side="away",
                team_code="SS",
                batting_order=2,
                player_name="이재현",
                standard_position="UNKNOWN",
                position="유격수",
                is_starter=True,
                appearance_seq=1,
            ),
            game,
        )
    ]

    chunk = _lineup_chunks(rows)[0]

    assert "2번 이재현 (유격수)" in chunk["content"]


@pytest.mark.parametrize("placeholder", [None, "", "   ", "Unknown", "UNKNOWN", "none", "null", "N/A", "Unknown 74164"])
def test_player_label_falls_back_to_the_id_when_the_name_is_a_placeholder(placeholder: str | None) -> None:
    """The source rows resolve today, but a blank or sentinel name must not become "None"."""
    player = SimpleNamespace(name=placeholder)

    assert build_rag_index._player_label(player, 50150) == "ID:50150"


def test_player_label_keeps_a_real_name() -> None:
    """A usable name is passed through with surrounding whitespace removed."""
    player = SimpleNamespace(name="  김성현 ")

    assert build_rag_index._player_label(player, 50150) == "김성현"


def test_player_label_handles_a_missing_player_row() -> None:
    """An outer join that finds nothing still has to produce a stable label."""
    assert build_rag_index._player_label(None, 50150) == "ID:50150"


def _batting_chunks(rows: list[tuple[SimpleNamespace, SimpleNamespace | None]]) -> list[dict]:
    """Run the season-batting generator over in-memory rows instead of a live session."""

    class _Query:
        def join(self, *args, **kwargs):
            return self

        def filter(self, *args, **kwargs):
            return self

        def limit(self, size):
            return self

        def yield_per(self, size):
            return iter(rows)

    class _Session:
        def query(self, *models):
            return _Query()

    return list(build_rag_index._iter_batting_chunks(_Session(), None, None))


def test_season_batting_chunk_uses_the_id_instead_of_a_sentinel_player_name() -> None:
    """The 74 "Unknown는" chunks came from a name the source no longer serves."""
    batting = SimpleNamespace(
        player_id=91523,
        season=2002,
        team_code="SS",
        league="regular",
        level="KBO1",
        games=131,
        avg=0.258,
        obp=None,
        slg=None,
        ops=None,
        hits=0,
        home_runs=12,
        rbi=50,
        runs=0,
        stolen_bases=0,
        strikeouts=0,
        walks=0,
    )

    chunk = _batting_chunks([(batting, SimpleNamespace(name="Unknown"))])[0]

    assert "Unknown" not in chunk["content"]
    assert "Unknown" not in chunk["title"]
    assert "ID:91523" in chunk["content"]


def _player_chunks(monkeypatch, players, *, team_labels=None, duplicated=frozenset()):
    """Run the player generator over in-memory rows instead of a live session."""

    class _Query:
        def filter(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def limit(self, size):
            return self

        def yield_per(self, size):
            return iter(players)

    class _Session:
        def query(self, *models):
            return _Query()

    monkeypatch.setattr(build_rag_index, "_team_label_map", lambda session: dict(team_labels or {}))
    monkeypatch.setattr(build_rag_index, "_colliding_player_labels", lambda session, labels: set(duplicated))
    return list(build_rag_index._iter_player_chunks(_Session(), None, None))


def _player(**overrides):
    """A complete player row so a missing attribute is never the reason a field is absent."""
    base = {
        "player_id": 50150,
        "name": "김성현",
        "team": None,
        "position": None,
        "birth_date": None,
        "height_cm": None,
        "weight_kg": None,
        "bats": None,
        "throws": None,
        "debut_year": None,
        "career": None,
        "status": None,
        "draft_info": None,
        "salary_original": None,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_player_chunk_renders_the_korean_club_name(monkeypatch) -> None:
    """``팀: SS`` is unreachable for someone searching ``삼성``."""
    chunk = _player_chunks(monkeypatch, [_player(team="SS")], team_labels={"SS": "삼성"})[0]

    assert chunk["title"] == "김성현 (삼성)"
    assert "팀: 삼성" in chunk["content"]
    assert chunk["team_id"] == "SS"


def test_player_chunk_omits_the_team_when_the_club_is_unknown(monkeypatch) -> None:
    """``무소속`` claimed a fact for the 2,119 rows that simply have no team."""
    chunk = _player_chunks(monkeypatch, [_player(position="포수")])[0]

    assert chunk["title"] == "김성현"
    assert "무소속" not in chunk["title"]
    assert "팀:" not in chunk["content"]


@pytest.mark.parametrize("status", ["NOT_FOUND", "PSEUDO", "Unknown/Runtime"])
def test_player_chunk_drops_a_crawler_sentinel_status(monkeypatch, status: str) -> None:
    """The status records what the crawl did, not what the player is."""
    chunk = _player_chunks(monkeypatch, [_player(status=status, position="포수")])[0]

    assert "상태:" not in chunk["content"]
    assert status not in chunk["content"]


def test_player_chunk_keeps_a_real_status(monkeypatch) -> None:
    chunk = _player_chunks(monkeypatch, [_player(status="retired")])[0]

    assert "상태: retired" in chunk["content"]


def test_player_chunk_disambiguates_a_shared_name(monkeypatch) -> None:
    """Two players with the same name *and* club would otherwise become one title."""
    chunk = _player_chunks(monkeypatch, [_player(position="포수")], duplicated={("김성현", "")})[0]

    assert chunk["title"] == "김성현 #50150"
    assert "선수: 김성현 #50150" in chunk["content"]


def test_player_chunk_keeps_the_title_clean_when_the_club_differs(monkeypatch) -> None:
    """A shared name is not ambiguity once the club separates the two rows."""
    chunk = _player_chunks(
        monkeypatch,
        [_player(team="SS", position="포수")],
        team_labels={"SS": "삼성"},
        duplicated={("김성현", "두산")},
    )[0]

    assert chunk["title"] == "김성현 (삼성)"


def test_player_chunk_renders_bats_and_throws_in_korean(monkeypatch) -> None:
    chunk = _player_chunks(monkeypatch, [_player(bats="R", throws="L")])[0]

    assert "타석: 우타" in chunk["content"]
    assert "투구: 좌투" in chunk["content"]


def test_player_chunk_uses_the_id_when_the_name_is_a_placeholder(monkeypatch) -> None:
    chunk = _player_chunks(monkeypatch, [_player(name="Unknown 74164", position="포수")])[0]

    assert chunk["title"] == "ID:50150"
    assert "Unknown" not in chunk["content"]


def test_player_chunk_is_skipped_when_only_the_name_is_known(monkeypatch) -> None:
    """1,062 players have no attribute at all; a name-only vector answers nothing."""
    assert _player_chunks(monkeypatch, [_player()]) == []


def test_player_chunk_survives_a_status_alone(monkeypatch) -> None:
    """A crawl outcome is still something the row asserts, so the chunk is kept."""
    chunks = _player_chunks(monkeypatch, [_player(status="active")])

    assert len(chunks) == 1
    assert "상태: active" in chunks[0]["content"]


def _inning_game(
    game_id: str = "20180313LGLT0",
    *,
    game_date: date = date(2018, 3, 13),
    away: str = "LG",
    home: str = "LT",
    away_score: int | None = 4,
    home_score: int | None = 3,
    status: str = "COMPLETED",
) -> SimpleNamespace:
    return SimpleNamespace(
        game_id=game_id,
        game_date=game_date,
        away_team=away,
        home_team=home,
        away_score=away_score,
        home_score=home_score,
        game_status=status,
        season_id=201800,
    )


def _inning_rows(scores: dict[int, tuple[int, int]], game_id: str = "20180313LGLT0") -> list:
    rows = []
    for inning, (away_runs, home_runs) in scores.items():
        rows.append(SimpleNamespace(game_id=game_id, inning=inning, team_side="away", runs=away_runs))
        rows.append(SimpleNamespace(game_id=game_id, inning=inning, team_side="home", runs=home_runs))
    return rows


def _inning_chunks(
    game: SimpleNamespace,
    inning_rows: list,
    *,
    pbp_max: list[tuple[str, int]] | None = None,
    curtailed: list[tuple[str]] | None = None,
    minutes: list[tuple[str, int]] | None = None,
    teams: list | None = None,
) -> list[dict]:
    """Run the line-score generator over in-memory rows instead of a live session."""
    from src.models.game import Game, GameInningScore, GameMetadata, GamePlayByPlay
    from src.models.team import Team

    class _Query:
        def __init__(self, rows):
            self._rows = rows

        def join(self, *args, **kwargs):
            return self

        def filter(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def group_by(self, *args, **kwargs):
            return self

        def distinct(self):
            return self

        def yield_per(self, size):
            return iter(self._rows)

        def __iter__(self):
            return iter(self._rows)

        def all(self):
            return list(self._rows)

    class _Session:
        def query(self, *models):
            owner = models[0] if isinstance(models[0], type) else models[0].class_
            if owner is GameInningScore:
                return _Query([(row, game) for row in inning_rows])
            if owner is GamePlayByPlay:
                return _Query((pbp_max or []) if len(models) == 2 else (curtailed or []))
            if owner is GameMetadata:
                return _Query(minutes or [])
            if owner is Team:
                return _Query(teams or [])
            message = f"unexpected query for {owner}"
            raise AssertionError(message)

    return list(build_rag_index._iter_inning_score_chunks(_Session(), None, None))


def _team(team_id: str, name: str) -> SimpleNamespace:
    return SimpleNamespace(team_id=team_id, team_name=name, team_short_name=name)


_PADDED_NINE = {
    1: (0, 0),
    2: (0, 0),
    3: (0, 0),
    4: (0, 1),
    5: (0, 1),
    6: (0, 0),
    7: (1, 0),
    8: (0, 0),
    9: (1, 0),
    10: (0, 0),
    11: (0, 0),
    12: (0, 0),
}


def test_decided_inning_returns_none_while_the_score_is_level() -> None:
    assert build_rag_index._decided_inning({1: {"away": 3, "home": 3}}) is None


def test_decided_inning_stops_when_the_home_team_never_batted() -> None:
    """3-2 with only the top of the ninth stored: the game ended at nine, not later."""
    by_inning = {1: {"away": 3, "home": 2}, 9: {"away": 0}}

    assert build_rag_index._decided_inning(by_inning) == 9


def test_decided_inning_reaches_the_extra_inning_that_settled_it() -> None:
    by_inning = {
        **{n: {"away": 0, "home": 0} for n in range(1, 9)},
        9: {"away": 4, "home": 4},
        10: {"away": 0, "home": 1},
    }

    assert build_rag_index._decided_inning(by_inning) == 10


def test_played_innings_ignores_padding_beyond_the_score() -> None:
    """2024 stores twelve innings for every game; this one was level, not twelve."""
    innings, evidence = build_rag_index._played_innings(
        {n: {"away": 0, "home": 0} for n in range(11, 13)},
        domain_max=12,
        pbp_max=9,
        game_minutes=None,
        curtailed=False,
    )

    assert (innings, evidence) == (9, "pbp")


def test_played_innings_accepts_a_short_game_from_the_curtailment_caption() -> None:
    innings, evidence = build_rag_index._played_innings(
        {n: {"away": 0, "home": 0} for n in range(1, 6)},
        domain_max=5,
        pbp_max=6,
        game_minutes=None,
        curtailed=True,
    )

    assert (innings, evidence) == (5, "curtailed")


def test_played_innings_stops_at_the_last_run_when_the_length_is_unrecorded() -> None:
    """Eight curtailed games (90~130 minutes) have no play-by-play to count innings from.

    Their domain rows are filled out to 12, so the count has to come from the runs.
    """
    scores = {n: {"away": 0, "home": 0} for n in range(1, 13)}
    scores.update({1: {"away": 0, "home": 2}, 2: {"away": 4, "home": 0}, 5: {"away": 0, "home": 4}})
    innings, evidence = build_rag_index._played_innings(
        scores,
        domain_max=12,
        pbp_max=0,
        game_minutes=127,
        curtailed=False,
    )

    assert (innings, evidence) == (5, "curtailed_unknown")


def test_played_innings_does_not_trim_a_game_whose_stub_play_by_play_stops_early() -> None:
    """2011 play-by-play holds one or two rows per inning; a normal clock outranks it."""
    scores = {**{n: {"away": 0, "home": 0} for n in range(1, 13)}, 3: {"away": 0, "home": 2}}
    innings, evidence = build_rag_index._played_innings(
        scores,
        domain_max=12,
        pbp_max=1,
        game_minutes=208,
        curtailed=False,
    )

    assert (innings, evidence) == (9, "decided")


def test_played_innings_reads_a_running_time_as_a_curtailment() -> None:
    innings, evidence = build_rag_index._played_innings(
        {n: {"away": 0, "home": 0} for n in range(1, 6)},
        domain_max=5,
        pbp_max=5,
        game_minutes=96,
        curtailed=False,
    )

    assert (innings, evidence) == (5, "curtailed")


def test_played_innings_keeps_the_filled_innings_when_a_tie_has_no_play_by_play() -> None:
    """Early draws carry no play-by-play at all, so nothing contradicts the filled rows."""
    innings, evidence = build_rag_index._played_innings(
        {1: {"away": 1, "home": 1}, 2: {"away": 2, "home": 1}},
        domain_max=12,
        pbp_max=0,
        game_minutes=None,
        curtailed=False,
    )

    assert (innings, evidence) == (12, "domain")


def test_line_score_notes_read_a_tie_from_the_score_not_the_status() -> None:
    """``20210529NCLT1`` is stored COMPLETED at 10-10 and is still a draw."""
    assert build_rag_index._line_score_notes(9, 10, 10, "decided") == " (무승부)"
    assert build_rag_index._line_score_notes(9, 4, 3, "decided") == ""


def test_line_score_notes_mark_an_extension_and_a_short_game() -> None:
    assert build_rag_index._line_score_notes(11, 5, 4, "decided") == " (연장 2회)"
    assert build_rag_index._line_score_notes(6, 5, 4, "curtailed") == " (6회 종료)"


def test_line_score_notes_do_not_invent_a_count_for_an_unrecorded_short_game() -> None:
    assert build_rag_index._line_score_notes(5, 4, 6, "curtailed_unknown") == " (조기 종료)"


def test_inning_score_chunk_replaces_the_false_extension_with_the_played_innings() -> None:
    """The index held ``(무승부) (연장 3회)`` for a game that stopped after nine."""
    game = _inning_game(
        game_id="20240630SSKT1",
        game_date=date(2024, 6, 30),
        away="SS",
        home="KT",
        away_score=2,
        home_score=2,
        status="DRAW",
    )
    chunk = _inning_chunks(
        game,
        _inning_rows(_PADDED_NINE, game.game_id),
        pbp_max=[(game.game_id, 9)],
        teams=[_team("SS", "삼성 라이온즈"), _team("KT", "kt wiz")],
    )[0]

    assert chunk["content"].splitlines()[3] == "삼성 라이온즈 2 - 2 kt wiz (무승부)"
    assert "연장" not in chunk["content"]
    assert chunk["content"].splitlines()[4].endswith("9회 1-0")
    assert chunk["meta"]["innings_played"] == 9


def test_inning_score_chunk_renders_the_extension_a_game_reached() -> None:
    """``20180327LGWO0`` was level at 3-3 after nine and settled in the tenth."""
    scores = dict.fromkeys(range(1, 13), (0, 0))
    scores.update({3: (1, 0), 4: (0, 1), 5: (1, 0), 6: (0, 1), 9: (1, 1), 10: (0, 1)})
    game = _inning_game(game_id="20180327LGWO0", home="WO", away_score=3, home_score=4)
    chunk = _inning_chunks(
        game,
        _inning_rows(scores, game.game_id),
        pbp_max=[(game.game_id, 10)],
        teams=[_team("LG", "LG 트윈스"), _team("WO", "키움 히어로즈")],
    )[0]

    assert chunk["content"].splitlines()[3] == "LG 트윈스 3 - 4 키움 히어로즈 (연장 1회)"
    assert chunk["meta"]["innings_evidence"] == "decided"


def test_inning_score_chunk_keeps_the_existing_identity_and_shape() -> None:
    """The store already holds ``{game_id}_line`` rows; regenerating must not duplicate them."""
    game = _inning_game()
    scores = {1: (0, 1), 2: (0, 0), 3: (0, 0), 4: (0, 0), 5: (2, 0), 6: (2, 0), 7: (0, 2), 8: (0, 0), 9: (0, 0)}
    chunk = _inning_chunks(
        game,
        _inning_rows(scores, game.game_id),
        teams=[_team("LG", "LG 트윈스"), _team("LT", "롯데 자이언츠")],
    )[0]

    assert chunk["source_table"] == "game_inning_scores"
    assert chunk["source_row_id"] == "20180313LGLT0_line"
    assert chunk["title"] == "[2018-03-13] LG 트윈스 vs 롯데 자이언츠 라인스코어"
    assert chunk["document_type"] == "game_line_score"
    assert chunk["meta"]["kind"] == "game_line_score"
    assert chunk["content"].startswith("경기 라인스코어\n경기: 20180313LGLT0\n경기일: 2018-03-13\n")
