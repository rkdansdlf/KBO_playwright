from __future__ import annotations

from contextlib import nullcontext
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
