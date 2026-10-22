"""Tests for scripts/maintenance/cleanup_corrupted_milestones.py."""

from __future__ import annotations

import io
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import create_engine, func, inspect, select, text
from sqlalchemy.orm import Session

from scripts.maintenance.cleanup_corrupted_milestones import (
    BACKUP_TABLE_NAME,
    SELECTION_REASON,
    backup_table,
    main,
    run_cleanup,
)
from src.models.base import Base
from src.models.player_milestone import PlayerMilestone
from src.models.rag_chunk import RagChunk
from src.services.rag_index_identity import stable_milestone_source_row_id

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


class MilestoneTestHarness:
    """In-memory SQLite test harness with source, sparse, and vector stores."""

    def __init__(self) -> None:
        """Initialize in-memory databases and sessions."""
        self.source_engine: Engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.source_engine)
        self.source_session: Session = Session(self.source_engine)
        self.index_session: Session = Session(self.source_engine)

        self.vector_engine: Engine = create_engine("sqlite:///:memory:")
        with self.vector_engine.begin() as conn:
            conn.execute(text("CREATE TABLE rag_chunks (source_table TEXT, source_row_id TEXT, index_status TEXT)"))
        self.vector_session: Session = Session(self.vector_engine)

    def close(self) -> None:
        """Close sessions and dispose engines."""
        self.source_session.close()
        self.index_session.close()
        self.vector_session.close()
        self.source_engine.dispose()
        self.vector_engine.dispose()


def _seed_corrupted(harness: MilestoneTestHarness) -> None:
    """Seed test corrupted milestone rows and matching ACTIVE RAG chunks."""
    m1 = PlayerMilestone(
        id=1,
        season=2026,
        player_id="101",
        player_name="최정",
        team_code="SSG",
        milestone_category="500홈런",
        current_val=505,
        target_val=500,
        remaining_val=0,
        is_achieved=1,
        achieved_date=date(2026, 4, 15),
        created_at=datetime(2026, 3, 1, 10, 0, 0, tzinfo=UTC),
        updated_at=datetime(2026, 4, 15, 22, 0, 0, tzinfo=UTC),
    )
    m2 = PlayerMilestone(
        id=2,
        season=2026,
        player_id="102",
        player_name="손아섭",
        team_code="NC",
        milestone_category="2500안타",
        current_val=3100,  # >3x target: 3100 > 3000
        target_val=1000,
        remaining_val=0,
        is_achieved=1,
        achieved_date=date(2026, 5, 20),
        created_at=datetime(2026, 3, 1, 10, 0, 0, tzinfo=UTC),
        updated_at=datetime(2026, 5, 20, 22, 0, 0, tzinfo=UTC),
    )
    m3 = PlayerMilestone(
        id=3,
        season=2025,
        player_id="103",
        player_name="양현종",
        team_code="HT",
        milestone_category="2000탈삼진",
        current_val=1990,  # target_val - current_val == 10 (crawler signature)
        target_val=2000,
        remaining_val=10,
        is_achieved=0,
        achieved_date=None,
        created_at=datetime(2025, 3, 1, 10, 0, 0, tzinfo=UTC),
        updated_at=datetime(2025, 9, 30, 22, 0, 0, tzinfo=UTC),
    )
    harness.source_session.add_all([m1, m2, m3])
    harness.source_session.commit()

    # Sparse store chunks
    c1 = RagChunk(
        source_table="milestone",
        source_row_id=stable_milestone_source_row_id(m1.season, m1.player_id, m1.milestone_category),
        content="최정 500홈런 달성",
        index_status="ACTIVE",
    )
    c2 = RagChunk(
        source_table="milestone",
        source_row_id=stable_milestone_source_row_id(m2.season, m2.player_id, m2.milestone_category),
        content="손아섭 2500안타 달성",
        index_status="ACTIVE",
    )
    c3 = RagChunk(
        source_table="milestone",
        source_row_id=stable_milestone_source_row_id(m3.season, m3.player_id, m3.milestone_category),
        content="양현종 2000탈삼진 현황",
        index_status="ACTIVE",
    )
    harness.index_session.add_all([c1, c2, c3])
    harness.index_session.commit()

    # Vector store chunks
    with harness.vector_engine.begin() as conn:
        for m in (m1, m2, m3):
            row_id = stable_milestone_source_row_id(m.season, m.player_id, m.milestone_category)
            conn.execute(
                text(
                    "INSERT INTO rag_chunks (source_table, source_row_id, index_status) "
                    "VALUES ('milestone', :r, 'ACTIVE')"
                ),
                {"r": row_id},
            )


def test_dry_run_writes_nothing_reports_counts() -> None:
    """Verify dry-run does not mutate databases and reports accurate candidate counts."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)
    out = io.StringIO()
    report = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=False,
        out=out,
    )
    assert report.status_code == 0
    assert report.mode == "dry-run"
    assert report.selected == 3
    assert report.candidates == 3
    assert report.already_backed_up == 0
    assert report.would_back_up == 3
    assert report.backed_up_new == 0
    assert report.would_delete == 3
    assert report.deleted == 0
    assert report.sparse_active == 3
    assert report.sparse_tombstoned == 0
    assert report.vector_active == 3
    assert report.vector_tombstoned == 0

    # Ensure source table unchanged
    live_count = harness.source_session.scalar(select(func.count(PlayerMilestone.id)))
    assert live_count == 3

    # Ensure backup table not created
    bind = harness.source_session.connection()
    assert not inspect(bind).has_table(BACKUP_TABLE_NAME)

    # Ensure chunks remain ACTIVE
    sparse_statuses = harness.index_session.scalars(select(RagChunk.index_status)).all()
    assert all(s == "ACTIVE" for s in sparse_statuses)

    vec_statuses = harness.vector_session.scalars(text("SELECT index_status FROM rag_chunks")).all()
    assert all(s == "ACTIVE" for s in vec_statuses)

    output = out.getvalue()
    assert "Breakdown: total=3" in output
    assert "mode=dry-run selected=3 backed_up_new=0 deleted=0 sparse_tombstoned=0" in output
    harness.close()


def test_apply_backs_up_deletes_and_tombstones() -> None:
    """Verify apply backs up all rows, deletes them, and tombstones chunks across both stores."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)
    out = io.StringIO()
    report = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        out=out,
    )
    assert report.status_code == 0
    assert report.mode == "apply"
    assert report.selected == 3
    assert report.backed_up_new == 3
    assert report.deleted == 3
    assert report.sparse_tombstoned == 3
    assert report.vector_tombstoned == 3

    # Verify player_milestones table is now empty
    live_count = harness.source_session.scalar(select(func.count(PlayerMilestone.id)))
    assert live_count == 0

    # Verify backup table exists and exact values are preserved
    bind = harness.source_session.connection()
    assert inspect(bind).has_table(BACKUP_TABLE_NAME)
    backup_rows = list(
        harness.source_session.execute(select(backup_table).order_by(backup_table.c.id)).mappings().all()
    )
    assert len(backup_rows) == 3

    row1 = backup_rows[0]
    assert row1["id"] == 1
    assert row1["season"] == 2026
    assert row1["player_id"] == "101"
    assert row1["player_name"] == "최정"
    assert row1["team_code"] == "SSG"
    assert row1["milestone_category"] == "500홈런"
    assert row1["current_val"] == 505
    assert row1["target_val"] == 500
    assert row1["remaining_val"] == 0
    assert row1["is_achieved"] == 1
    assert row1["achieved_date"] == date(2026, 4, 15)
    assert row1["rag_source_key"] == "milestone:2026_101_500홈런"
    assert row1["selection_reason"] == SELECTION_REASON
    assert row1["backed_up_at"] is not None

    # Verify chunks are marked DELETED
    sparse_statuses = harness.index_session.scalars(select(RagChunk.index_status)).all()
    assert all(s == "DELETED" for s in sparse_statuses)

    vec_statuses = harness.vector_session.scalars(text("SELECT index_status FROM rag_chunks")).all()
    assert all(s == "DELETED" for s in vec_statuses)

    output = out.getvalue()
    assert "mode=apply selected=3 backed_up_new=3 deleted=3 sparse_tombstoned=3" in output
    harness.close()


def test_second_apply_is_noop_with_zero_counts() -> None:
    """Verify second apply run is idempotent, using the frozen set and reporting zero mutations."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)

    # First run executes cleanup
    run_cleanup(harness.source_session, harness.index_session, harness.vector_session, apply=True)

    # Second run
    out = io.StringIO()
    second_report = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        out=out,
    )
    assert second_report.status_code == 0
    assert second_report.selected == 3
    assert second_report.already_backed_up == 3
    assert second_report.backed_up_new == 0
    assert second_report.deleted == 0
    assert second_report.sparse_tombstoned == 0
    assert second_report.vector_tombstoned == 0
    assert second_report.sparse_already == 3
    assert second_report.vector_already == 3

    output = out.getvalue()
    assert "frozen backup set" in output
    assert "backed_up_new=0 deleted=0 sparse_tombstoned=0" in output
    assert "vector_tombstoned=0" in output
    harness.close()


def test_rerun_after_recrawl_preserves_new_row_and_chunk() -> None:
    """Verify rerun after fresh re-crawl does not delete the new row or retire its active chunk."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)

    # First run cleans up rows 1, 2, 3
    run_cleanup(harness.source_session, harness.index_session, harness.vector_session, apply=True)

    # Re-crawl re-inserts a valid milestone with the same natural key as row 1 (2026, 101, 500홈런)
    new_milestone = PlayerMilestone(
        id=999,
        season=2026,
        player_id="101",
        player_name="최정",
        team_code="SSG",
        milestone_category="500홈런",
        current_val=495,
        target_val=500,
        remaining_val=5,
        is_achieved=0,
        achieved_date=None,
        created_at=datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC),
        updated_at=datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC),
    )
    harness.source_session.add(new_milestone)
    harness.source_session.commit()

    chunk_row_id = stable_milestone_source_row_id(2026, "101", "500홈런")
    sparse_chunk = harness.index_session.scalar(
        select(RagChunk).where(RagChunk.source_table == "milestone", RagChunk.source_row_id == chunk_row_id)
    )
    assert sparse_chunk is not None
    sparse_chunk.index_status = "ACTIVE"
    harness.index_session.commit()

    with harness.vector_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE rag_chunks SET index_status = 'ACTIVE' WHERE source_table = 'milestone' AND source_row_id = :r"
            ),
            {"r": chunk_row_id},
        )

    # Re-run cleanup
    out = io.StringIO()
    rerun_report = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        out=out,
    )
    assert rerun_report.status_code == 0
    assert rerun_report.deleted == 0
    assert rerun_report.sparse_tombstoned == 0
    assert rerun_report.vector_tombstoned == 0

    # Ensure new milestone row is preserved
    surviving = harness.source_session.scalar(select(PlayerMilestone).where(PlayerMilestone.id == 999))
    assert surviving is not None
    assert surviving.current_val == 495

    # Ensure chunk remains ACTIVE
    assert sparse_chunk.index_status == "ACTIVE"
    vec_status = harness.vector_session.scalar(
        text("SELECT index_status FROM rag_chunks WHERE source_table = 'milestone' AND source_row_id = :r"),
        {"r": chunk_row_id},
    )
    assert vec_status == "ACTIVE"
    harness.close()


def test_expect_count_mismatch_refuses_status_3_no_writes() -> None:
    """Verify --expect-count mismatch refuses early with status 3 and performs no writes."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)
    out = io.StringIO()
    report = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=999,
        out=out,
    )
    assert report.status_code == 3
    assert report.exit_code == 3
    assert "count mismatch: expected 999, found 3" in out.getvalue()

    # Verify no writes occurred
    live_count = harness.source_session.scalar(select(func.count(PlayerMilestone.id)))
    assert live_count == 3
    bind = harness.source_session.connection()
    assert not inspect(bind).has_table(BACKUP_TABLE_NAME)
    sparse_statuses = harness.index_session.scalars(select(RagChunk.index_status)).all()
    assert all(s == "ACTIVE" for s in sparse_statuses)
    harness.close()


def test_season_filter_limits_selection() -> None:
    """Verify --season restricts processing only to the designated season."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)
    out = io.StringIO()
    report = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        season=2026,
        out=out,
    )
    assert report.status_code == 0
    assert report.selected == 2
    assert report.backed_up_new == 2
    assert report.deleted == 2
    assert report.sparse_tombstoned == 2
    assert report.vector_tombstoned == 2

    # Verify season 2025 row survives untouched
    remaining = harness.source_session.scalars(select(PlayerMilestone)).all()
    assert len(remaining) == 1
    assert remaining[0].season == 2025

    # Verify season 2025 chunk remains ACTIVE
    s2025_chunk_id = stable_milestone_source_row_id(2025, "103", "2000탈삼진")
    sparse_2025 = harness.index_session.scalar(
        select(RagChunk.index_status).where(RagChunk.source_row_id == s2025_chunk_id)
    )
    assert sparse_2025 == "ACTIVE"
    harness.close()


def test_vector_session_none_prints_skip_completes_rest() -> None:
    """Verify cleanup succeeds with vector_session=None and logs vector skip line."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)
    out = io.StringIO()
    report = run_cleanup(
        harness.source_session,
        harness.index_session,
        vector_session=None,
        apply=True,
        out=out,
    )
    assert report.status_code == 0
    assert report.vector_skipped is True
    assert report.backed_up_new == 3
    assert report.deleted == 3
    assert report.sparse_tombstoned == 3
    assert report.vector_tombstoned == 0
    assert "vector store not configured (PGVECTOR_URL unset): skipped" in out.getvalue()

    sparse_statuses = harness.index_session.scalars(select(RagChunk.index_status)).all()
    assert all(s == "DELETED" for s in sparse_statuses)
    harness.close()


def test_production_guard_refuses_before_write(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify production guard refuses write when RAG_INDEX_ALLOW_PRODUCTION_WRITE!=1."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)

    monkeypatch.setenv("RAG_TARGET_ENV", "production")
    monkeypatch.delenv("RAG_INDEX_ALLOW_PRODUCTION_WRITE", raising=False)

    out = io.StringIO()
    report = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        out=out,
    )
    assert report.status_code == 3
    assert report.exit_code == 3
    assert "production --apply requires RAG_INDEX_ALLOW_PRODUCTION_WRITE=1" in out.getvalue()

    # Verify no writes occurred
    live_count = harness.source_session.scalar(select(func.count(PlayerMilestone.id)))
    assert live_count == 3
    bind = harness.source_session.connection()
    assert not inspect(bind).has_table(BACKUP_TABLE_NAME)

    # Permit production write and verify completion
    monkeypatch.setenv("RAG_INDEX_ALLOW_PRODUCTION_WRITE", "1")
    out2 = io.StringIO()
    report2 = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        out=out2,
    )
    assert report2.status_code == 0
    assert report2.deleted == 3
    harness.close()


def test_main_cli_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify main() CLI dispatches arguments and opens configured sessions."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)

    # Mock DB session context managers
    from contextlib import contextmanager

    @contextmanager
    def mock_db():
        yield harness.source_session

    @contextmanager
    def mock_index():
        yield harness.index_session

    @contextmanager
    def mock_vector():
        yield harness.vector_session

    monkeypatch.setattr("src.db.engine.get_db_session", mock_db)
    monkeypatch.setattr("src.db.engine.get_rag_index_session", mock_index)
    monkeypatch.setattr("src.db.vector_engine.get_vector_session", mock_vector)
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://user:pass@localhost:5432/test")

    exit_code = main(["--season", "2026"])
    assert exit_code == 0
    harness.close()


def test_any_earlier_apply_freezes_selection_for_every_season() -> None:
    """After one --apply, no later run re-selects live rows, even for a season that was not cleaned."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)
    run_cleanup(harness.source_session, harness.index_session, harness.vector_session, apply=True, season=2026)

    out = io.StringIO()
    report = run_cleanup(
        harness.source_session, harness.index_session, harness.vector_session, apply=True, season=2025, out=out
    )

    assert report.status_code == 0
    assert report.selected == 0
    assert report.deleted == 0
    assert "frozen backup set" in out.getvalue()
    remaining = harness.source_session.scalars(select(PlayerMilestone)).all()
    assert [m.season for m in remaining] == [2025]
    harness.close()
