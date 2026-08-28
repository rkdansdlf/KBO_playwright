"""Tests for safe RAG season-year metadata repair planning."""

from __future__ import annotations

from contextlib import nullcontext
from unittest.mock import MagicMock

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.cli.rag import repair_season_year_metadata as repair
from src.models.base import Base
from src.models.rag_chunk import RagChunk


def test_expected_year_extracts_game_and_child_source_ids() -> None:
    assert repair._expected_year("game", "20250606HHHT0", {}) == 2025
    assert repair._expected_year("game_lineups", "20250606HHHT0_home", {}) == 2025
    assert repair._expected_year("game_play_by_play", "123", {"game_id": "20010405LTHU0"}) == 2001
    assert repair._expected_year("game_highlights", "123", {"game_id": "19880402OBLT0"}) == 1988


def test_expected_year_rejects_invalid_game_identity() -> None:
    assert repair._expected_year("game", "not-a-game", {}) is None
    assert repair._expected_year("game_play_by_play", "123", {}) is None
    assert repair._expected_year("game", "19790405LTHU0", {}) is None


def test_build_repair_plan_only_changes_season_id_aliases() -> None:
    plan = repair.build_repair_plan(
        [
            (1, "game", "20250606HHHT0", 259, {}),
            (2, "game", "20250607HHHT0", 2025, {}),
            (3, "game_lineups", "20220614SKKT0_home", 241, {"game_id": "20220614SKKT0"}),
            (4, "game_play_by_play", "123", None, {"game_id": "20010405LTHU0"}),
            (5, "game_highlights", "456", 2023, {"game_id": "20230823OBWO0"}),
            (6, "game", "invalid", 123, {}),
        ],
    )

    assert [(item.chunk_id, item.new_season_year) for item in plan.repairs] == [(1, 2025), (3, 2022), (4, 2001)]
    assert plan.by_source == {"game": 1, "game_lineups": 1, "game_play_by_play": 1}
    assert plan.by_year == {2025: 1, 2022: 1, 2001: 1}
    assert plan.skipped_by_reason == {"already_correct": 2, "invalid_game_id": 1}


def test_apply_repair_plan_batches_column_only_updates() -> None:
    session = MagicMock()
    plan = repair.build_repair_plan(
        [
            (1, "game", "20250606HHHT0", 259, {}),
            (2, "game", "20250607HHHT0", 259, {}),
        ],
    )

    assert repair.apply_repair_plan(session, plan, batch_size=1) == 2
    assert session.execute.call_count == 2
    assert all(call.args[1][0]["repair_season_year"] == 2025 for call in session.execute.call_args_list)


def test_apply_repair_plan_updates_sqlite_rows_without_touching_payload() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[RagChunk.__table__])
    with Session(engine) as session:
        chunk = RagChunk(
            source_table="game",
            source_row_id="20250606HHHT0",
            season_year=259,
            content="unchanged",
            content_hash="hash",
            index_version="rag-v1",
            index_status="ACTIVE",
            meta={"game_date": "2025-06-06"},
        )
        session.add(chunk)
        session.commit()
        plan = repair.build_repair_plan(
            [(chunk.id, "game", "20250606HHHT0", 259, {"game_date": "2025-06-06"})],
        )

        assert repair.apply_repair_plan(session, plan, batch_size=10) == 1
        session.commit()
        refreshed = session.get(RagChunk, chunk.id)

    assert refreshed is not None
    assert refreshed.season_year == 2025
    assert refreshed.content == "unchanged"
    assert refreshed.content_hash == "hash"
    assert refreshed.embedding is None
    assert refreshed.meta == {"game_date": "2025-06-06"}


def test_apply_requires_write_gate(monkeypatch) -> None:
    monkeypatch.delenv("RAG_INDEX_ALLOW_WRITE", raising=False)
    args = repair._parse_args(["--apply"])

    assert repair._validate_args(args) == "--apply requires RAG_INDEX_ALLOW_WRITE=1"


def test_main_dry_run_renders_selected_sources(monkeypatch, capsys) -> None:
    session = MagicMock()
    session.execute.return_value.all.return_value = [(1, "game", "20250606HHHT0", 259, {})]
    monkeypatch.setattr(repair, "get_rag_index_session", lambda: nullcontext(session))

    assert repair.main(["--source", "game", "--json"]) == 0
    output = capsys.readouterr().out

    assert '"mode": "dry-run"' in output
    assert '"candidate_count": 1' in output
    assert '"source_tables": ["game"]' in output
