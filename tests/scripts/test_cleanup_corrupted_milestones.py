"""Tests for scripts/maintenance/cleanup_corrupted_milestones.py."""

from __future__ import annotations

import io
import json
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import pytest
from sqlalchemy import create_engine, func, inspect, select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from scripts.maintenance.cleanup_corrupted_milestones import (
    BACKUP_TABLE_NAME,
    CHUNK_STATE_TABLE_NAME,
    RESTORE_ROWS_SQL,
    SELECTION_REASON,
    backup_table,
    check_confirm_target,
    chunk_state_table,
    main,
    reactivate_chunks,
    restore_chunk_targets,
    run_cleanup,
    url_identity,
)
from src.models.base import Base
from src.models.player_milestone import PlayerMilestone
from src.models.rag_chunk import RagChunk
from src.services.rag_index_identity import stable_milestone_source_row_id
from src.services.rag_vector_reactivation import CLEANUP_TOMBSTONE_META_KEY

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


class MilestoneTestHarness:
    """In-memory SQLite test harness with source, sparse, and vector stores."""

    def __init__(self, *, with_optional_vector_cols: bool = True) -> None:
        """Initialize in-memory databases and sessions."""
        self.source_engine: Engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.source_engine)
        self.source_session: Session = Session(self.source_engine)
        self.index_session: Session = Session(self.source_engine)

        self.vector_engine: Engine = create_engine("sqlite:///:memory:")
        with self.vector_engine.begin() as conn:
            if with_optional_vector_cols:
                conn.execute(
                    text(
                        "CREATE TABLE rag_chunks ("
                        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                        "meta TEXT NOT NULL DEFAULT '{}', "
                        "source_table TEXT, source_row_id TEXT, index_status TEXT, "
                        "is_active INTEGER, valid_to TIMESTAMP"
                        ")"
                    )
                )
            else:
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
        cols = {col["name"] for col in inspect(conn).get_columns("rag_chunks")}
        has_is_active = "is_active" in cols
        for m in (m1, m2, m3):
            row_id = stable_milestone_source_row_id(m.season, m.player_id, m.milestone_category)
            if has_is_active:
                conn.execute(
                    text(
                        "INSERT INTO rag_chunks (source_table, source_row_id, index_status, is_active, valid_to) "
                        "VALUES ('milestone', :r, 'ACTIVE', 1, NULL)"
                    ),
                    {"r": row_id},
                )
            else:
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
        expect_count=3,
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

    vec_rows = harness.vector_session.execute(text("SELECT index_status, is_active, meta FROM rag_chunks")).fetchall()
    assert all(s[0] == "DELETED" and s[1] == 0 for s in vec_rows)
    for r in vec_rows:
        m = json.loads(r[2]) if isinstance(r[2], str) else r[2]
        assert m.get(CLEANUP_TOMBSTONE_META_KEY) is True

    output = out.getvalue()
    assert "mode=apply selected=3 backed_up_new=3 deleted=3 sparse_tombstoned=3" in output
    harness.close()


def test_second_apply_is_noop_with_zero_counts() -> None:
    """Verify second apply run is idempotent, using the frozen set and reporting zero mutations."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)

    # First run executes cleanup
    run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
    )

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
    assert "season=2025 mode=frozen rows=1" in output
    assert "season=2026 mode=frozen rows=2" in output
    assert "backed_up_new=0 deleted=0 sparse_tombstoned=0" in output
    assert "vector_tombstoned=0" in output
    harness.close()


def test_rerun_after_recrawl_preserves_new_row_and_chunk() -> None:
    """Verify rerun after fresh re-crawl does not delete the new row or retire its active chunk."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)

    # First run cleans up rows 1, 2, 3
    run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
    )

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
        expect_count=2,
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
        expect_count=3,
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
        expect_count=3,
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
        expect_count=3,
        out=out2,
    )
    assert report2.status_code == 0
    assert report2.deleted == 3
    harness.close()


def test_main_cli_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify main() CLI dispatches arguments and opens configured sessions."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)

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


def test_r1_per_season_frozen_sets() -> None:
    """R1: apply --season 2026 (expect_count=2) then full run backs up only 2025 rows (expect_count=1)

    and 2026 stays frozen; then --season 2025 frozen rerun is no-op; re-crawled 2026 row is untouched.
    """
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)

    # Step 1: apply --season 2026 (expect_count=2)
    out1 = io.StringIO()
    rep1 = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        season=2026,
        expect_count=2,
        out=out1,
    )
    assert rep1.status_code == 0
    assert rep1.selected == 2
    assert rep1.backed_up_new == 2
    assert rep1.deleted == 2
    assert "season=2026 mode=new rows=2" in out1.getvalue()

    # Re-crawl inserts a new 2026 milestone
    recrawled_2026 = PlayerMilestone(
        id=999,
        season=2026,
        player_id="101",
        player_name="최정",
        team_code="SSG",
        milestone_category="500홈런",
        current_val=500,
        target_val=500,
        remaining_val=0,
        is_achieved=1,
        achieved_date=date(2026, 4, 15),
        created_at=datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC),
        updated_at=datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC),
    )
    harness.source_session.add(recrawled_2026)
    harness.source_session.commit()

    # Running a plain run without --allow-new-season skips 2025 because backup exists for 2026
    out_skip = io.StringIO()
    rep_skip = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=False,
        out=out_skip,
    )
    assert rep_skip.status_code == 0
    assert "season=2025 mode=skipped-new-season rows=1" in out_skip.getvalue()
    assert "season=2026 mode=frozen rows=2" in out_skip.getvalue()

    # Step 2: run with --season 2025 --allow-new-season (expect_count=1): backs up 2025 rows
    out2 = io.StringIO()
    rep2 = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        season=2025,
        allow_new_season=True,
        expect_count=1,
        out=out2,
    )
    assert rep2.status_code == 0
    assert rep2.backed_up_new == 1
    assert rep2.would_back_up == 1
    out2_val = out2.getvalue()
    assert "season=2025 mode=new rows=1" in out2_val

    # Re-crawled 2026 row is untouched
    live_999 = harness.source_session.scalar(select(PlayerMilestone).where(PlayerMilestone.id == 999))
    assert live_999 is not None

    # Step 3: later --season 2025 run is frozen too and no-op
    out3 = io.StringIO()
    rep3 = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        season=2025,
        out=out3,
    )
    assert rep3.status_code == 0
    assert rep3.backed_up_new == 0
    assert rep3.already_backed_up == 1
    assert "season=2025 mode=frozen rows=1" in out3.getvalue()

    # Step 4: plain run now treats both seasons as frozen and is a no-op
    out4 = io.StringIO()
    rep4 = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        out=out4,
    )
    assert rep4.status_code == 0
    assert rep4.backed_up_new == 0
    assert rep4.already_backed_up == 3
    assert "season=2025 mode=frozen rows=1" in out4.getvalue()
    assert "season=2026 mode=frozen rows=2" in out4.getvalue()
    harness.close()


def test_r2_expect_count_requirements() -> None:
    """R2: first apply without expect_count -> 3 and nothing written; frozen rerun without expect_count ok; mismatch -> 3."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)

    # 1. First apply without expect_count -> 3, nothing written
    out1 = io.StringIO()
    rep1 = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=None,
        out=out1,
    )
    assert rep1.status_code == 3
    assert "--apply with new rows to back up requires --expect-count 3" in out1.getvalue()
    assert harness.source_session.scalar(select(func.count(PlayerMilestone.id))) == 3

    # 2. Mismatch -> 3
    out2 = io.StringIO()
    rep2 = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=999,
        out=out2,
    )
    assert rep2.status_code == 3
    assert "count mismatch: expected 999, found 3" in out2.getvalue()

    # Apply with correct count succeeds
    run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
    )

    # 3. Frozen rerun without expect_count ok; nonzero match ok; mismatch -> 3
    out3 = io.StringIO()
    rep3 = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=None,
        out=out3,
    )
    assert rep3.status_code == 0

    # 0 is ok
    rep3_zero = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=0,
    )
    assert rep3_zero.status_code == 0

    # Nonzero mismatch -> 3
    out4 = io.StringIO()
    rep4 = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=999,
        out=out4,
    )
    assert rep4.status_code == 3
    assert "count mismatch: expected 999, found 3" in out4.getvalue()
    harness.close()


def test_r3_check_confirm_target_helper() -> None:
    """R3: check_confirm_target unit tests for target identity."""
    urls_single = {"source": "postgresql://user:pass@db.prod.internal:5432/kbo"}

    # Missing -> error
    err_missing = check_confirm_target(urls_single, None)
    assert err_missing is not None
    assert "db.prod.internal:5432/kbo" in err_missing

    err_empty = check_confirm_target(urls_single, [])
    assert err_empty is not None
    assert "db.prod.internal:5432/kbo" in err_empty

    # Host-only is no longer enough -> error
    err_host_only = check_confirm_target(urls_single, ["db.prod.internal"])
    assert err_host_only is not None
    assert "db.prod.internal:5432/kbo" in err_host_only

    # Wrong target identity -> error
    err_wrong = check_confirm_target(urls_single, ["other.host:5432/kbo"])
    assert err_wrong is not None
    assert "db.prod.internal:5432/kbo" in err_wrong

    # Exact identity confirmed -> None
    assert check_confirm_target(urls_single, ["db.prod.internal:5432/kbo"]) is None

    # Port default 5432 for postgresql*
    urls_no_port = {"source": "postgresql://user:pass@db.prod.internal/kbo"}
    assert check_confirm_target(urls_no_port, ["db.prod.internal:5432/kbo"]) is None
    urls_psycopg = {"source": "postgresql+psycopg://user:pass@db.prod.internal/kbo"}
    assert check_confirm_target(urls_psycopg, ["db.prod.internal:5432/kbo"]) is None

    # SQLite identity <drivername>:<database>
    urls_sqlite = {"source": "sqlite:///./data/kbo_dev.db"}
    assert check_confirm_target(urls_sqlite, ["sqlite:./data/kbo_dev.db"]) is None
    urls_memory = {"source": "sqlite:///:memory:"}
    assert check_confirm_target(urls_memory, ["sqlite::memory:"]) is None

    # Multiple distinct target identities
    urls_multi = {
        "source": "postgresql://user:pass@host1.internal:5432/db",
        "sparse": "postgresql://user:pass@host2.internal:5432/db",
        "vector": "postgresql://user:pass@host3.internal:5432/vec",
    }
    # Partial confirmation -> error
    err_partial = check_confirm_target(urls_multi, ["host1.internal:5432/db", "host2.internal:5432/db"])
    assert err_partial is not None
    assert "host3.internal:5432/vec" in err_partial

    # All distinct identities confirmed -> None
    assert (
        check_confirm_target(
            urls_multi,
            ["host1.internal:5432/db", "host2.internal:5432/db", "host3.internal:5432/vec"],
        )
        is None
    )


def test_r3_main_pgvector_test_url_mismatch_refuses_before_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    """R3: main() with PGVECTOR_TEST_URL differing on --apply -> 3 without opening sessions; redacted URL printed without password."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://app_user:secret_pw123@db.prod.internal:5432/bega_prod")
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://vec_user:vec_pw456@vec.prod.internal:5432/kbo_rag")
    monkeypatch.setenv("PGVECTOR_TEST_URL", "postgresql://vec_user:vec_pw456@test.vec.internal:5432/kbo_rag")

    def explode() -> None:
        message = "Session factory should not have been called!"
        raise AssertionError(message)

    monkeypatch.setattr("src.db.engine.get_db_session", explode)
    monkeypatch.setattr("src.db.engine.get_rag_index_session", explode)
    monkeypatch.setattr("src.db.vector_engine.get_vector_session", explode)

    out = io.StringIO()
    exit_code = main(
        ["--apply", "--expect-count", "353", "--confirm-target", "db.prod.internal:5432/bega_prod"], out=out
    )
    assert exit_code == 3
    output = out.getvalue()
    assert "PGVECTOR_TEST_URL is set and would override PGVECTOR_URL" in output
    assert "secret_pw123" not in output
    assert "vec_pw456" not in output
    assert "***" in output


def test_r4_vector_tombstone_honest_counting() -> None:
    """R4: vector tombstone sets is_active=0 and valid_to non-null; already-DELETED-but-active rows get deactivated on rerun

    and counted as deactivated_only; simulated failure on one key -> committed keys counted, failed key not counted, status 1,
    and DB state matches counts.
    """
    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)

    # First apply
    rep1 = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
    )
    assert rep1.status_code == 0
    assert rep1.vector_tombstoned == 3

    # Check that is_active=0 and valid_to is not null
    with harness.vector_engine.begin() as conn:
        rows = conn.execute(text("SELECT is_active, valid_to FROM rag_chunks")).fetchall()
        for is_act, val_to in rows:
            assert is_act == 0
            assert val_to is not None

        # Simulate an older script run that set DELETED but left is_active=1
        conn.execute(
            text("UPDATE rag_chunks SET is_active = 1, valid_to = NULL WHERE source_row_id = '2026_101_500홈런'")
        )

    # Rerun: already-DELETED-but-active row gets deactivated on rerun and counted as deactivated_only
    rep2 = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
    )
    assert rep2.status_code == 0
    assert rep2.vector_tombstoned == 0
    assert rep2.vector_deactivated_only == 1
    assert rep2.vector_already == 2

    # Simulated failure on one key: wrapper around vector_session.execute that raises on key 2026_102_2500안타
    with harness.vector_engine.begin() as conn:
        conn.execute(text("UPDATE rag_chunks SET index_status = 'ACTIVE', is_active = 1"))

    orig_execute = harness.vector_session.execute

    def flaky_execute(stmt: object, params: dict[str, object] | None = None, *args: object, **kwargs: object) -> object:
        if params and params.get("row_id") == "2026_102_2500안타" and "UPDATE" in str(stmt):
            message = "Simulated failure for 2026_102_2500안타"
            raise OperationalError(message, params, Exception(message))
        return orig_execute(stmt, params, *args, **kwargs)

    harness.vector_session.execute = flaky_execute  # type: ignore[assignment]

    out_fail = io.StringIO()
    rep_fail = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        out=out_fail,
    )
    assert rep_fail.status_code == 1
    assert rep_fail.vector_tombstoned == 2

    # Restore execute and check DB state matches counts
    harness.vector_session.execute = orig_execute
    with harness.vector_engine.begin() as conn:
        failed_row = conn.execute(
            text("SELECT index_status, is_active FROM rag_chunks WHERE source_row_id = '2026_102_2500안타'")
        ).first()
        assert failed_row[0] == "ACTIVE"
        assert failed_row[1] == 1

        success_row = conn.execute(
            text("SELECT index_status, is_active FROM rag_chunks WHERE source_row_id = '2026_101_500홈런'")
        ).first()
        assert success_row[0] == "DELETED"
        assert success_row[1] == 0

    harness.close()


def test_r5_sparse_without_is_active_prints_line() -> None:
    """R5: sparse without is_active columns prints the 'index_status only' line."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)

    out = io.StringIO()
    run_cleanup(
        harness.source_session,
        harness.index_session,
        vector_session=None,
        apply=False,
        out=out,
    )
    assert "sparse store has no is_active/valid_to columns: index_status only" in out.getvalue()
    harness.close()


def test_r6_delete_only_true_matches_skips_mismatch() -> None:
    """R6: a live row reusing a backed-up id with a different natural key is not deleted (delete_skipped_mismatch=1)."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)

    # First apply to back up and delete
    run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
    )

    # Insert a new live row reusing id=1 but with a different milestone_category
    mismatched = PlayerMilestone(
        id=1,
        season=2026,
        player_id="101",
        player_name="최정",
        team_code="SSG",
        milestone_category="4000루타",  # Different from backup "500홈런"
        current_val=4005,
        target_val=4000,
        remaining_val=0,
        is_achieved=1,
        achieved_date=date(2026, 4, 15),
        created_at=datetime(2026, 3, 1, 10, 0, 0, tzinfo=UTC),
        updated_at=datetime(2026, 4, 15, 22, 0, 0, tzinfo=UTC),
    )
    harness.source_session.add(mismatched)
    harness.source_session.commit()

    # Rerun cleanup in frozen mode
    out = io.StringIO()
    rep = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        out=out,
    )
    assert rep.status_code == 0
    assert rep.delete_skipped_mismatch == 1
    assert rep.deleted == 0

    # Ensure mismatched row was left alone
    surviving = harness.source_session.get(PlayerMilestone, 1)
    assert surviving is not None
    assert surviving.milestone_category == "4000루타"
    harness.close()


def test_r7_incompatible_backup_schema_refuses_status_3() -> None:
    """R7: pre-existing incompatible backup table -> 3 with clear message, no traceback, nothing written."""
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)

    # Pre-create incompatible backup table (missing several required columns)
    with harness.source_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {BACKUP_TABLE_NAME} (id INTEGER PRIMARY KEY, season INTEGER)"))

    out = io.StringIO()
    rep = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=False,
        out=out,
    )
    assert rep.status_code == 3
    output = out.getvalue()
    assert f"existing {BACKUP_TABLE_NAME} has an incompatible schema; missing columns:" in output
    assert "player_id" in output
    assert "rag_source_key" in output

    # In apply mode also refuses
    rep_apply = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
        out=io.StringIO(),
    )
    assert rep_apply.status_code == 3
    # Nothing was deleted from live
    assert harness.source_session.scalar(select(func.count(PlayerMilestone.id))) == 3
    harness.close()


def test_r8_legacy_empty_key_chunks() -> None:
    """R8: matched legacy chunk reported in dry-run, retired in both stores on apply, second apply 0; unmatched legacy chunk stays ACTIVE."""
    # --- Case 1: Matched legacy chunk ---
    harness1 = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness1)

    legacy_matched = RagChunk(
        source_table="milestone",
        source_row_id="",
        content="최정 레거시 청크",
        index_status="ACTIVE",
        meta={"player_name": "최정", "milestone_category": "500홈런"},
    )
    harness1.index_session.add(legacy_matched)
    harness1.index_session.commit()

    with harness1.vector_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO rag_chunks (source_table, source_row_id, index_status, is_active, valid_to, meta) "
                "VALUES ('milestone', '', 'ACTIVE', 1, NULL, :meta)"
            ),
            {"meta": '{"player_name": "최정", "milestone_category": "500홈런"}'},
        )

    # Dry-run
    out_dry = io.StringIO()
    rep_dry = run_cleanup(
        harness1.source_session,
        harness1.index_session,
        harness1.vector_session,
        apply=False,
        out=out_dry,
    )
    assert rep_dry.status_code == 0
    dry_val = out_dry.getvalue()
    assert (
        "legacy_empty_key_chunks: sparse_found=1 sparse_matched=1 sparse_active=1 "
        "vector_found=1 vector_matched=1 vector_active=1"
    ) in dry_val

    # Apply 1: retires matched legacy chunk in both stores
    rep_app1 = run_cleanup(
        harness1.source_session,
        harness1.index_session,
        harness1.vector_session,
        apply=True,
        expect_count=3,
    )
    assert rep_app1.status_code == 0
    assert rep_app1.legacy_sparse_tombstoned == 1
    assert rep_app1.legacy_vector_tombstoned == 1

    # Matched legacy sparse chunk is DELETED
    assert legacy_matched.index_status == "DELETED"

    # Vector store legacy row is DELETED
    with harness1.vector_engine.begin() as conn:
        vec_legacy = conn.execute(
            text(
                "SELECT index_status, is_active FROM rag_chunks WHERE source_table = 'milestone' AND source_row_id = ''"
            )
        ).first()
        assert vec_legacy[0] == "DELETED"
        assert vec_legacy[1] == 0

    # Apply 2: second apply gives 0 retired legacy chunks
    rep_app2 = run_cleanup(
        harness1.source_session,
        harness1.index_session,
        harness1.vector_session,
        apply=True,
    )
    assert rep_app2.status_code == 0
    assert rep_app2.legacy_sparse_tombstoned == 0
    assert rep_app2.legacy_vector_tombstoned == 0
    harness1.close()

    # --- Case 2: Unmatched legacy chunk stays ACTIVE ---
    harness2 = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness2)

    legacy_unmatched = RagChunk(
        source_table="milestone",
        source_row_id="",
        content="이정후 레거시 청크",
        index_status="ACTIVE",
        meta={"player_name": "이정후", "milestone_category": "1000안타"},
    )
    harness2.index_session.add(legacy_unmatched)
    harness2.index_session.commit()

    out_unmatched = io.StringIO()
    rep_unmatched = run_cleanup(
        harness2.source_session,
        harness2.index_session,
        harness2.vector_session,
        apply=True,
        expect_count=3,
        out=out_unmatched,
    )
    assert rep_unmatched.status_code == 0
    assert rep_unmatched.legacy_sparse_tombstoned == 0
    assert "legacy empty-key chunk unmatched" in out_unmatched.getvalue()
    assert legacy_unmatched.index_status == "ACTIVE"
    harness2.close()


def test_r9_restore_and_reactivate_chunks() -> None:
    """R9: after cleanup + re-crawl that re-creates one natural key, executing RESTORE_ROWS_SQL for the season

    does not raise and restores only non-conflicting rows; reactivate_chunks reactivates exactly those chunks.
    """
    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)

    # 1. Cleanup runs (backs up and deletes 3 rows)
    run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
    )
    assert harness.source_session.scalar(select(func.count(PlayerMilestone.id))) == 0

    # 2. Re-crawl re-creates one natural key for 2026 (최정 500홈런) with a new ID
    recrawled = PlayerMilestone(
        id=999,
        season=2026,
        player_id="101",
        player_name="최정",
        team_code="SSG",
        milestone_category="500홈런",
        current_val=500,
        target_val=500,
        remaining_val=0,
        is_achieved=1,
        achieved_date=date(2026, 4, 15),
        created_at=datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC),
        updated_at=datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC),
    )
    harness.source_session.add(recrawled)
    harness.source_session.commit()

    # 3. Execute RESTORE_ROWS_SQL for 2026 on SQLite
    harness.source_session.execute(RESTORE_ROWS_SQL, {"season": 2026})
    harness.source_session.commit()

    # Verify only non-conflicting row (id=2, 손아섭 2500안타) was restored; 최정 was not restored with id=1
    live_rows = list(harness.source_session.scalars(select(PlayerMilestone).order_by(PlayerMilestone.id)).all())
    assert len(live_rows) == 2
    assert live_rows[0].id == 2
    assert live_rows[0].player_name == "손아섭"
    assert live_rows[1].id == 999
    assert live_rows[1].player_name == "최정"

    # 4. restore_chunk_targets returns target tuples only for restored non-conflicting row (id=2)
    expected_row_id = stable_milestone_source_row_id(2026, "102", "2500안타")
    sparse_targets = restore_chunk_targets(harness.source_session, 2026, "sparse")
    assert len(sparse_targets) == 1
    assert sparse_targets[0][0] == expected_row_id
    assert sparse_targets[0][1] == "ACTIVE"

    vec_targets = restore_chunk_targets(harness.source_session, 2026, "vector")
    assert len(vec_targets) == 1
    assert vec_targets[0][0] == expected_row_id
    assert vec_targets[0][1] == "ACTIVE"

    # 5. Execute reactivate_chunks for sparse and vector stores
    reactivated_sparse = reactivate_chunks(harness.source_session, harness.index_session, 2026, "sparse")
    assert reactivated_sparse == 1

    reactivated_vector = reactivate_chunks(harness.source_session, harness.vector_session, 2026, "vector")
    assert reactivated_vector == 1

    # Verify 손아섭 chunk is ACTIVE, 최정 chunk is still DELETED
    sparse_son = harness.index_session.scalar(
        select(RagChunk.index_status).where(RagChunk.source_row_id == expected_row_id)
    )
    assert sparse_son == "ACTIVE"

    choi_row_id = stable_milestone_source_row_id(2026, "101", "500홈런")
    sparse_choi = harness.index_session.scalar(
        select(RagChunk.index_status).where(RagChunk.source_row_id == choi_row_id)
    )
    assert sparse_choi == "DELETED"

    # In vector store:
    with harness.vector_engine.begin() as conn:
        vec_son = conn.execute(
            text("SELECT index_status, is_active, valid_to FROM rag_chunks WHERE source_row_id = :r"),
            {"r": expected_row_id},
        ).first()
        assert vec_son[0] == "ACTIVE"
        assert vec_son[1] == 1
        assert vec_son[2] is None

        vec_choi = conn.execute(
            text("SELECT index_status, is_active FROM rag_chunks WHERE source_row_id = :r"),
            {"r": choi_row_id},
        ).first()
        assert vec_choi[0] == "DELETED"
        assert vec_choi[1] == 0

    harness.close()


def test_minimal_3_column_vector_table() -> None:
    """Verify vector tombstone operates correctly when rag_chunks has only 3 columns (no is_active/valid_to)."""
    harness = MilestoneTestHarness(with_optional_vector_cols=False)
    _seed_corrupted(harness)

    rep = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
    )
    assert rep.status_code == 0
    assert rep.vector_tombstoned == 3

    statuses = harness.vector_session.scalars(text("SELECT index_status FROM rag_chunks")).all()
    assert all(s == "DELETED" for s in statuses)
    harness.close()


def test_production_guard_does_not_block_when_target_env_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Documented behaviour: without RAG_TARGET_ENV the guard defaults to "local" and does not block.

    That is why --confirm-target and --expect-count are required on every --apply instead.
    """
    from scripts.maintenance.cleanup_corrupted_milestones import check_production_guard

    monkeypatch.delenv("RAG_TARGET_ENV", raising=False)
    monkeypatch.delenv("RAG_INDEX_ALLOW_PRODUCTION_WRITE", raising=False)
    assert check_production_guard(apply=True) is None
    monkeypatch.setenv("RAG_TARGET_ENV", "staging")
    assert check_production_guard(apply=True) is None
    monkeypatch.setenv("RAG_TARGET_ENV", "production")
    assert check_production_guard(apply=True) is not None


def test_apply_without_confirm_target_refuses_before_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    """--apply without --confirm-target refuses (3) before any session is opened."""
    monkeypatch.delenv("PGVECTOR_TEST_URL", raising=False)
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://vec_user:vec_pw@vec.example.internal:5432/kbo_rag")

    def explode() -> None:
        message = "Session factory should not have been called!"
        raise AssertionError(message)

    monkeypatch.setattr("src.db.engine.get_db_session", explode)
    monkeypatch.setattr("src.db.engine.get_rag_index_session", explode)
    monkeypatch.setattr("src.db.vector_engine.get_vector_session", explode)

    out = io.StringIO()
    assert main(["--apply", "--expect-count", "3"], out=out) == 3
    assert "unconfirmed target(s)" in out.getvalue()
    assert "vec.example.internal:5432/kbo_rag" in out.getvalue()
    assert "vec_pw" not in out.getvalue()


def test_vector_legacy_regression_independent_filtering() -> None:
    """Regression: vector table holds 1 matched legacy row plus several unrelated NULL/'' rows.

    After apply, only the matched row is DELETED/is_active=0, unrelated rows are byte-for-byte
    unchanged, and legacy_vector_tombstoned == 1.
    """
    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)

    # 1. Matched legacy row
    # 2. Unrelated row with different player/category
    # 3. Unrelated row with empty meta '{}'
    # 4. Unrelated row with invalid JSON
    with harness.vector_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO rag_chunks (id, source_table, source_row_id, index_status, is_active, valid_to, meta) "
                "VALUES (10, 'milestone', '', 'ACTIVE', 1, NULL, :meta)"
            ),
            {"meta": '{"player_name": "최정", "milestone_category": "500홈런"}'},
        )
        conn.execute(
            text(
                "INSERT INTO rag_chunks (id, source_table, source_row_id, index_status, is_active, valid_to, meta) "
                "VALUES (11, 'milestone', '', 'ACTIVE', 1, NULL, :meta)"
            ),
            {"meta": '{"player_name": "박용택", "milestone_category": "2500안타"}'},
        )
        conn.execute(
            text(
                "INSERT INTO rag_chunks (id, source_table, source_row_id, index_status, is_active, valid_to, meta) "
                "VALUES (12, 'milestone', NULL, 'ACTIVE', 1, NULL, :meta)"
            ),
            {"meta": "{}"},
        )
        conn.execute(
            text(
                "INSERT INTO rag_chunks (id, source_table, source_row_id, index_status, is_active, valid_to, meta) "
                "VALUES (13, 'milestone', NULL, 'ACTIVE', 1, NULL, :meta)"
            ),
            {"meta": "not-valid-json{}"},
        )

    out = io.StringIO()
    report = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
        out=out,
    )
    assert report.status_code == 0
    assert report.legacy_vector_tombstoned == 1

    # Verify rows in vector store
    with harness.vector_engine.begin() as conn:
        row10 = (
            conn.execute(text("SELECT index_status, is_active, valid_to, meta FROM rag_chunks WHERE id = 10"))
            .mappings()
            .first()
        )
        assert row10 is not None
        assert row10["index_status"] == "DELETED"
        assert row10["is_active"] == 0
        assert row10["valid_to"] is not None

        row11 = (
            conn.execute(text("SELECT index_status, is_active, valid_to, meta FROM rag_chunks WHERE id = 11"))
            .mappings()
            .first()
        )
        assert row11 is not None
        assert row11["index_status"] == "ACTIVE"
        assert row11["is_active"] == 1
        assert row11["valid_to"] is None
        assert row11["meta"] == '{"player_name": "박용택", "milestone_category": "2500안타"}'

        row12 = (
            conn.execute(text("SELECT index_status, is_active, valid_to, meta FROM rag_chunks WHERE id = 12"))
            .mappings()
            .first()
        )
        assert row12 is not None
        assert row12["index_status"] == "ACTIVE"
        assert row12["is_active"] == 1
        assert row12["valid_to"] is None
        assert row12["meta"] == "{}"

        row13 = (
            conn.execute(text("SELECT index_status, is_active, valid_to, meta FROM rag_chunks WHERE id = 13"))
            .mappings()
            .first()
        )
        assert row13 is not None
        assert row13["index_status"] == "ACTIVE"
        assert row13["is_active"] == 1
        assert row13["valid_to"] is None
        assert row13["meta"] == "not-valid-json{}"

    output = out.getvalue()
    assert "legacy empty-key vector chunk id=11 unmatched:" in output
    assert "legacy empty-key vector chunk id=12 unmatched:" in output
    assert "legacy empty-key vector chunk id=13 unmatched: no usable meta" in output
    harness.close()


def test_zero_sparse_matches_with_matching_vector_legacy_row() -> None:
    """Verify vector legacy chunk is tombstoned even when zero sparse legacy chunks match."""
    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)

    # Note: no sparse legacy chunks added! Sparse legacy found=0, matched=0.
    with harness.vector_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO rag_chunks (source_table, source_row_id, index_status, is_active, valid_to, meta) "
                "VALUES ('milestone', '', 'ACTIVE', 1, NULL, :meta)"
            ),
            {"meta": '{"player_name": "최정", "milestone_category": "500홈런"}'},
        )

    out = io.StringIO()
    report = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
        out=out,
    )
    assert report.status_code == 0
    assert report.legacy_sparse_tombstoned == 0
    assert report.legacy_vector_tombstoned == 1

    with harness.vector_engine.begin() as conn:
        vec_row = conn.execute(
            text("SELECT index_status, is_active, meta FROM rag_chunks WHERE source_row_id = ''")
        ).first()
        assert vec_row is not None
        assert vec_row[0] == "DELETED"
        assert vec_row[1] == 0
        meta_dict = json.loads(vec_row[2]) if isinstance(vec_row[2], str) else vec_row[2]
        assert meta_dict.get(CLEANUP_TOMBSTONE_META_KEY) is True
    harness.close()


def test_vector_table_without_meta_column_skips() -> None:
    """Verify vector table without meta column writes nothing and prints skip line."""
    harness = MilestoneTestHarness(with_optional_vector_cols=False)
    _seed_corrupted(harness)

    with harness.vector_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO rag_chunks (source_table, source_row_id, index_status) VALUES ('milestone', '', 'ACTIVE')"
            )
        )

    out = io.StringIO()
    report = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
        out=out,
    )
    assert report.status_code == 0
    assert report.legacy_vector_tombstoned == 0
    output = out.getvalue()
    assert (
        "legacy vector chunks: found=1, skipped (vector rag_chunks has no meta/id column; cannot identify precisely)"
    ) in output

    with harness.vector_engine.begin() as conn:
        vec_row = conn.execute(text("SELECT index_status FROM rag_chunks WHERE source_row_id = ''")).first()
        assert vec_row is not None
        assert vec_row[0] == "ACTIVE"
    harness.close()


def test_dry_run_prints_separate_sparse_vector_counts_and_writes_nothing() -> None:
    """Verify dry-run prints separate sparse/vector matched counts and writes nothing."""
    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)

    c_sparse = RagChunk(
        source_table="milestone",
        source_row_id="",
        content="최정 sparse",
        index_status="ACTIVE",
        meta={"player_name": "최정", "milestone_category": "500홈런"},
    )
    harness.index_session.add(c_sparse)
    harness.index_session.commit()

    with harness.vector_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO rag_chunks (source_table, source_row_id, index_status, is_active, valid_to, meta) "
                "VALUES ('milestone', '', 'ACTIVE', 1, NULL, :meta)"
            ),
            {"meta": '{"player_name": "최정", "milestone_category": "500홈런"}'},
        )

    out = io.StringIO()
    report = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=False,
        out=out,
    )
    assert report.status_code == 0
    assert report.legacy_sparse_tombstoned == 0
    assert report.legacy_vector_tombstoned == 0
    assert (
        "legacy_empty_key_chunks: sparse_found=1 sparse_matched=1 sparse_active=1 "
        "vector_found=1 vector_matched=1 vector_active=1"
    ) in out.getvalue()

    # Verify both remain ACTIVE
    assert c_sparse.index_status == "ACTIVE"
    with harness.vector_engine.begin() as conn:
        vec_row = conn.execute(text("SELECT index_status FROM rag_chunks WHERE source_row_id = ''")).first()
        assert vec_row is not None
        assert vec_row[0] == "ACTIVE"
    harness.close()


def test_surviving_pair_left_alone_in_vector_store() -> None:
    """Verify vector chunk matching a live row that survives is left alone."""
    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)

    # First run cleans up initial corrupted rows
    run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
    )

    # Re-crawl inserts a new valid live row for 최정 500홈런
    recrawled = PlayerMilestone(
        id=999,
        season=2026,
        player_id="101",
        player_name="최정",
        team_code="SSG",
        milestone_category="500홈런",
        current_val=500,
        target_val=500,
        remaining_val=0,
        is_achieved=1,
        achieved_date=date(2026, 4, 15),
        created_at=datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC),
        updated_at=datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC),
    )
    harness.source_session.add(recrawled)
    harness.source_session.commit()

    # Insert legacy vector row for 최정 500홈런
    with harness.vector_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO rag_chunks (source_table, source_row_id, index_status, is_active, valid_to, meta) "
                "VALUES ('milestone', '', 'ACTIVE', 1, NULL, :meta)"
            ),
            {"meta": '{"player_name": "최정", "milestone_category": "500홈런"}'},
        )

    # Second cleanup run (frozen mode): target_items includes 최정 from backup,
    # but a live row now exists in player_milestones (surviving pair)!
    out = io.StringIO()
    report = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        out=out,
    )
    assert report.status_code == 0
    assert report.legacy_vector_tombstoned == 0
    output = out.getvalue()
    assert "live player_milestones row exists for player_name='최정', milestone_category='500홈런'" in output

    with harness.vector_engine.begin() as conn:
        vec_row = conn.execute(
            text("SELECT index_status, is_active, meta FROM rag_chunks WHERE source_row_id = ''")
        ).first()
        assert vec_row is not None
        assert vec_row[0] == "ACTIVE"
        assert vec_row[1] == 1
        meta_dict = json.loads(vec_row[2]) if isinstance(vec_row[2], str) else vec_row[2]
        assert CLEANUP_TOMBSTONE_META_KEY not in meta_dict
    harness.close()


def test_main_refusal_for_unparseable_url_without_printing_it(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify main() on --apply refuses with exit 3 when a configured target URL is unparseable without printing it."""
    invalid_url = "postgresql://user:pass@invalid ://: 5432/bad_db"
    monkeypatch.setenv("PGVECTOR_URL", invalid_url)
    monkeypatch.delenv("PGVECTOR_TEST_URL", raising=False)

    out = io.StringIO()
    exit_code = main(["--apply", "--expect-count", "3", "--confirm-target", "sqlite::memory:"], out=out)
    assert exit_code == 3
    output = out.getvalue()
    assert "cannot parse vector URL; refusing to write" in output
    assert "(unparseable URL hidden)" in output
    assert "invalid ://:" not in output
    assert "bad_db" not in output


def test_q3_new_season_skipped_unless_allowed() -> None:
    """Q3: 2027 rows added after cleanup -> dry-run shows skipped-new-season, apply leaves them;

    --season 2027 --allow-new-season --expect-count 2 cleans them; --allow-new-season without --season -> 3.
    """
    harness = MilestoneTestHarness()
    _seed_corrupted(harness)

    # Initial cleanup creates backup rows for 2025 and 2026
    rep_init = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
    )
    assert rep_init.status_code == 0
    assert rep_init.backed_up_new == 3

    # Add 2 new corrupted rows for 2027 created after the cleanup
    m27_1 = PlayerMilestone(
        id=201,
        season=2027,
        player_id="201",
        player_name="김도영",
        team_code="KIA",
        milestone_category="300도루",
        current_val=310,
        target_val=100,
        remaining_val=0,
        is_achieved=1,
        achieved_date=date(2027, 5, 1),
        created_at=datetime(2027, 4, 1, 10, 0, 0, tzinfo=UTC),
        updated_at=datetime(2027, 5, 1, 10, 0, 0, tzinfo=UTC),
    )
    m27_2 = PlayerMilestone(
        id=202,
        season=2027,
        player_id="202",
        player_name="문동주",
        team_code="한화",
        milestone_category="500탈삼진",
        current_val=490,
        target_val=500,
        remaining_val=10,
        is_achieved=0,
        achieved_date=None,
        created_at=datetime(2027, 4, 1, 10, 0, 0, tzinfo=UTC),
        updated_at=datetime(2027, 5, 1, 10, 0, 0, tzinfo=UTC),
    )
    harness.source_session.add_all([m27_1, m27_2])
    harness.source_session.commit()

    c27_1_id = stable_milestone_source_row_id(2027, "201", "300도루")
    c27_2_id = stable_milestone_source_row_id(2027, "202", "500탈삼진")
    harness.index_session.add_all(
        [
            RagChunk(source_table="milestone", source_row_id=c27_1_id, content="김도영 300도루", index_status="ACTIVE"),
            RagChunk(
                source_table="milestone", source_row_id=c27_2_id, content="문동주 500탈삼진", index_status="ACTIVE"
            ),
        ]
    )
    harness.index_session.commit()

    with harness.vector_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO rag_chunks (source_table, source_row_id, index_status, is_active, valid_to) "
                "VALUES ('milestone', :r1, 'ACTIVE', 1, NULL), ('milestone', :r2, 'ACTIVE', 1, NULL)"
            ),
            {"r1": c27_1_id, "r2": c27_2_id},
        )

    # 1. Dry-run: shows skipped-new-season
    out_dry = io.StringIO()
    rep_dry = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=False,
        out=out_dry,
    )
    assert rep_dry.status_code == 0
    dry_val = out_dry.getvalue()
    assert "season=2027 mode=skipped-new-season rows=2" in dry_val
    assert (
        "(rows created after the cleanup are not treated as corrupted; "
        "pass --allow-new-season with --season 2027 to clean it explicitly)"
    ) in dry_val

    # 2. Apply without --allow-new-season: leaves 2027 rows untouched
    out_app = io.StringIO()
    rep_app = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        out=out_app,
    )
    assert rep_app.status_code == 0
    assert rep_app.deleted == 0
    assert "season=2027 mode=skipped-new-season rows=2" in out_app.getvalue()
    live_2027_count = harness.source_session.scalar(
        select(func.count(PlayerMilestone.id)).where(PlayerMilestone.season == 2027)
    )
    assert live_2027_count == 2

    # 3. --allow-new-season without --season -> status 3
    out_err = io.StringIO()
    rep_err = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        allow_new_season=True,
        out=out_err,
    )
    assert rep_err.status_code == 3
    assert "--allow-new-season requires --season" in out_err.getvalue()

    out_cli = io.StringIO()
    exit_cli = main(["--apply", "--allow-new-season"], out=out_cli)
    assert exit_cli == 3
    assert "--allow-new-season requires --season" in out_cli.getvalue()

    # 4. --season 2027 --allow-new-season --expect-count 2 cleans them
    out_clean = io.StringIO()
    rep_clean = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        season=2027,
        allow_new_season=True,
        expect_count=2,
        out=out_clean,
    )
    assert rep_clean.status_code == 0
    assert rep_clean.backed_up_new == 2
    assert rep_clean.deleted == 2
    assert rep_clean.sparse_tombstoned == 2
    assert rep_clean.vector_tombstoned == 2
    assert "season=2027 mode=new rows=2" in out_clean.getvalue()

    live_after = harness.source_session.scalar(
        select(func.count(PlayerMilestone.id)).where(PlayerMilestone.season == 2027)
    )
    assert live_after == 0
    harness.close()


def test_q4_sparse_step_failure_continues_to_vector_and_legacy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Q4: first apply with an index session whose tombstone fails (OperationalError) -> rows deleted,

    vector chunks tombstoned, status 1, no traceback; second apply with real function -> sparse chunks DELETED, status 0.
    """
    import scripts.maintenance.cleanup_corrupted_milestones as cleanup_module

    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)

    def failing_tombstone(session: Session, keys: tuple[str, ...]) -> list[object]:
        message = "simulated sparse OperationalError"
        raise OperationalError(message, {}, Exception(message))

    monkeypatch.setattr(cleanup_module, "tombstone_primary_rows", failing_tombstone)

    out1 = io.StringIO()
    rep1 = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
        out=out1,
    )
    output1 = out1.getvalue()
    assert rep1.status_code == 1
    assert rep1.deleted == 3
    assert rep1.vector_tombstoned == 3
    assert rep1.sparse_tombstoned == 0
    assert "ERROR: sparse store step failed: " in output1
    assert "simulated sparse OperationalError" in output1
    assert "; vector step still attempted; rerun to finish" in output1

    # Live rows deleted
    live_count = harness.source_session.scalar(select(func.count(PlayerMilestone.id)))
    assert live_count == 0

    # Vector store chunks tombstoned
    with harness.vector_engine.begin() as conn:
        vec_statuses = conn.execute(text("SELECT index_status, is_active FROM rag_chunks")).fetchall()
        assert len(vec_statuses) == 3
        assert all(st == "DELETED" and act == 0 for st, act in vec_statuses)

    # Sparse chunks still ACTIVE
    sparse_statuses = harness.index_session.scalars(select(RagChunk.index_status)).all()
    assert all(st == "ACTIVE" for st in sparse_statuses)

    # Second apply with real function succeeds
    monkeypatch.undo()

    out2 = io.StringIO()
    rep2 = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        out=out2,
    )
    assert rep2.status_code == 0
    assert rep2.sparse_tombstoned == 3
    sparse_after = harness.index_session.scalars(select(RagChunk.index_status)).all()
    assert all(st == "DELETED" for st in sparse_after)
    harness.close()


def test_q5a_summary_fields_present() -> None:
    """Q5a: verify sparse_deactivated_only=, vector_deactivated_only=, and legacy_vector_deactivated_only= in summary."""
    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)

    out_dry = io.StringIO()
    run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=False,
        out=out_dry,
    )
    val_dry = out_dry.getvalue()
    assert "sparse_deactivated_only=0" in val_dry
    assert "vector_deactivated_only=0" in val_dry
    assert "legacy_vector_deactivated_only=0" in val_dry

    out_app = io.StringIO()
    run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
        out=out_app,
    )
    val_app = out_app.getvalue()
    assert "sparse_deactivated_only=0" in val_app
    assert "vector_deactivated_only=0" in val_app
    assert "legacy_vector_deactivated_only=0" in val_app
    harness.close()


def test_q5b_chunk_prior_state_recording_and_restore() -> None:
    """Q5b: chunk already DELETED before cleanup is recorded with prior DELETED and not reactivated after restore,

    chunk retired by cleanup is restored to ACTIVE with prior is_active/valid_to; rerun of apply does not overwrite
    prior state; incompatible chunk_state table -> 3.
    """
    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)

    choi_row_id = stable_milestone_source_row_id(2026, "101", "500홈런")
    sparse_choi = harness.index_session.scalar(select(RagChunk).where(RagChunk.source_row_id == choi_row_id))
    assert sparse_choi is not None
    sparse_choi.index_status = "DELETED"
    harness.index_session.commit()

    with harness.vector_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE rag_chunks SET index_status = 'DELETED', is_active = 0, valid_to = CURRENT_TIMESTAMP "
                "WHERE source_row_id = :r"
            ),
            {"r": choi_row_id},
        )

    # First apply
    rep1 = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
    )
    assert rep1.status_code == 0

    # Verify chunk_state table exists
    bind = harness.source_session.connection()
    assert inspect(bind).has_table(CHUNK_STATE_TABLE_NAME)

    recorded_states = list(
        harness.source_session.execute(
            select(
                chunk_state_table.c.store,
                chunk_state_table.c.chunk_ref,
                chunk_state_table.c.prior_index_status,
                chunk_state_table.c.prior_is_active,
                chunk_state_table.c.recorded_at,
            ).order_by(chunk_state_table.c.store, chunk_state_table.c.chunk_ref)
        )
        .mappings()
        .all()
    )
    assert len(recorded_states) > 0

    son_row_id = stable_milestone_source_row_id(2026, "102", "2500안타")
    choi_vec_state = next(r for r in recorded_states if r["store"] == "vector" and r["chunk_ref"] == choi_row_id)
    assert choi_vec_state["prior_index_status"] == "DELETED"
    assert choi_vec_state["prior_is_active"] == 0
    choi_recorded_at = choi_vec_state["recorded_at"]

    son_vec_state = next(r for r in recorded_states if r["store"] == "vector" and r["chunk_ref"] == son_row_id)
    assert son_vec_state["prior_index_status"] == "ACTIVE"
    assert son_vec_state["prior_is_active"] == 1

    # Rerun of apply does not overwrite prior state
    rep2 = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
    )
    assert rep2.status_code == 0

    choi_vec_state_after = (
        harness.source_session.execute(
            select(chunk_state_table.c.prior_index_status, chunk_state_table.c.recorded_at).where(
                chunk_state_table.c.store == "vector",
                chunk_state_table.c.chunk_ref == choi_row_id,
            )
        )
        .mappings()
        .first()
    )
    assert choi_vec_state_after is not None
    assert choi_vec_state_after["prior_index_status"] == "DELETED"
    assert choi_vec_state_after["recorded_at"] == choi_recorded_at

    # Restore rows for season 2026
    harness.source_session.execute(RESTORE_ROWS_SQL, {"season": 2026})
    harness.source_session.commit()

    # Check restore_chunk_targets
    out_targets = io.StringIO()
    sparse_targets = restore_chunk_targets(harness.source_session, 2026, "sparse", out=out_targets)
    assert len(sparse_targets) == 2
    sparse_by_id = {t[0]: t for t in sparse_targets}
    assert sparse_by_id[son_row_id][1] == "ACTIVE"
    assert sparse_by_id[choi_row_id][1] == "DELETED"

    vec_targets = restore_chunk_targets(harness.source_session, 2026, "vector")
    assert len(vec_targets) == 2
    vec_by_id = {t[0]: t for t in vec_targets}
    assert vec_by_id[son_row_id][1] == "ACTIVE"
    assert vec_by_id[son_row_id][2] == 1
    assert vec_by_id[choi_row_id][1] == "DELETED"
    assert vec_by_id[choi_row_id][2] == 0

    # Reactivate chunks
    reactivated_sparse = reactivate_chunks(harness.source_session, harness.index_session, 2026, "sparse")
    assert reactivated_sparse == 2

    reactivated_vec = reactivate_chunks(harness.source_session, harness.vector_session, 2026, "vector")
    assert reactivated_vec == 2

    with harness.vector_engine.begin() as conn:
        son_chunk = conn.execute(
            text("SELECT index_status, is_active, valid_to, meta FROM rag_chunks WHERE source_row_id = :r"),
            {"r": son_row_id},
        ).first()
        assert son_chunk[0] == "ACTIVE"
        assert son_chunk[1] == 1
        assert son_chunk[2] is None
        son_meta = json.loads(son_chunk[3]) if isinstance(son_chunk[3], str) else son_chunk[3]
        assert CLEANUP_TOMBSTONE_META_KEY not in son_meta

        choi_chunk = conn.execute(
            text("SELECT index_status, is_active, meta FROM rag_chunks WHERE source_row_id = :r"),
            {"r": choi_row_id},
        ).first()
        assert choi_chunk[0] == "DELETED"
        assert choi_chunk[1] == 0
        choi_meta = json.loads(choi_chunk[2]) if isinstance(choi_chunk[2], str) else choi_chunk[2]
        assert CLEANUP_TOMBSTONE_META_KEY not in choi_meta

    # Verify chunk_state rows removed after restore
    remaining_states = harness.source_session.scalars(
        select(chunk_state_table.c.chunk_ref).where(chunk_state_table.c.chunk_ref.in_([choi_row_id, son_row_id]))
    ).all()
    assert len(remaining_states) == 0

    # Second cleanup after restore records fresh state (new prior values)
    with harness.vector_engine.begin() as conn:
        conn.execute(
            text("UPDATE rag_chunks SET is_active = 1, valid_to = '2026-08-01 12:00:00' WHERE source_row_id = :r"),
            {"r": son_row_id},
        )

    rep_fresh = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=0,
    )
    assert rep_fresh.status_code == 0

    fresh_state = harness.source_session.execute(
        select(chunk_state_table.c.prior_valid_to).where(
            chunk_state_table.c.store == "vector",
            chunk_state_table.c.chunk_ref == son_row_id,
        )
    ).scalar()
    assert fresh_state is not None

    # Second restore uses fresh state
    harness.source_session.execute(RESTORE_ROWS_SQL, {"season": 2026})
    harness.source_session.commit()
    reactivate_chunks(harness.source_session, harness.vector_session, 2026, "vector")
    with harness.vector_engine.begin() as conn:
        son_restored2 = conn.execute(
            text("SELECT index_status, is_active, valid_to FROM rag_chunks WHERE source_row_id = :r"),
            {"r": son_row_id},
        ).first()
        assert son_restored2[0] == "ACTIVE"
        assert son_restored2[1] == 1
        assert str(son_restored2[2]).startswith("2026-08-01")

    harness.close()

    # Incompatible chunk_state table -> 3
    harness_incompat = MilestoneTestHarness()
    _seed_corrupted(harness_incompat)
    with harness_incompat.source_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {CHUNK_STATE_TABLE_NAME} (store TEXT PRIMARY KEY, chunk_ref TEXT)"))

    out_incompat = io.StringIO()
    rep_incompat = run_cleanup(
        harness_incompat.source_session,
        harness_incompat.index_session,
        harness_incompat.vector_session,
        apply=False,
        out=out_incompat,
    )
    assert rep_incompat.status_code == 3
    assert f"existing {CHUNK_STATE_TABLE_NAME} has an incompatible schema; missing columns:" in out_incompat.getvalue()
    assert "prior_index_status" in out_incompat.getvalue()
    harness_incompat.close()


def test_cleanup_apply_delete_failure_skips_rag_returns_1_and_rerun_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """If DELETE in _backup_and_delete raises OperationalError, rollback, return status 1 without traceback, RAG untouched; rerun succeeds."""
    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)

    orig_execute = harness.source_session.execute

    def mock_execute(statement, *args, **kwargs):
        if hasattr(statement, "is_delete") and statement.is_delete:
            raise OperationalError("simulated delete failure", {}, Exception("simulated delete failure"))
        return orig_execute(statement, *args, **kwargs)

    monkeypatch.setattr(harness.source_session, "execute", mock_execute)

    out = io.StringIO()
    report = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
        out=out,
    )
    output = out.getvalue()
    assert report.status_code == 1
    assert "ERROR: delete step failed: " in output
    assert "no RAG chunks were changed; rerun to finish" in output
    assert "Traceback" not in output
    assert "mode=apply selected=3 backed_up_new=3 deleted=0 sparse_tombstoned=0" in output

    # Rows still present in PlayerMilestone
    live_count = harness.source_session.scalar(select(func.count(PlayerMilestone.id)))
    assert live_count == 3

    # RAG chunks untouched (still ACTIVE)
    sparse_statuses = harness.index_session.scalars(select(RagChunk.index_status)).all()
    assert all(st == "ACTIVE" for st in sparse_statuses)
    vec_statuses = harness.vector_session.scalars(text("SELECT index_status FROM rag_chunks")).all()
    assert all(st == "ACTIVE" for st in vec_statuses)

    # Undo monkeypatch and rerun with a working session finishes status 0
    monkeypatch.undo()
    out2 = io.StringIO()
    report2 = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=0,
        out=out2,
    )
    assert report2.status_code == 0
    assert report2.deleted == 3
    assert report2.sparse_tombstoned == 3
    assert report2.vector_tombstoned == 3

    assert harness.source_session.scalar(select(func.count(PlayerMilestone.id))) == 0
    assert all(st == "DELETED" for st in harness.index_session.scalars(select(RagChunk.index_status)).all())
    assert all(
        st == "DELETED" for st in harness.vector_session.scalars(text("SELECT index_status FROM rag_chunks")).all()
    )

    harness.close()


def test_cleanup_apply_backup_failure_skips_rag_returns_1(monkeypatch: pytest.MonkeyPatch) -> None:
    """If backup insert in _backup_and_delete raises OperationalError, rollback, return status 1 without traceback, RAG untouched."""
    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)

    orig_execute = harness.source_session.execute

    def mock_execute(statement, *args, **kwargs):
        if hasattr(statement, "is_insert") and statement.is_insert:
            raise OperationalError("simulated backup failure", {}, Exception("simulated backup failure"))
        return orig_execute(statement, *args, **kwargs)

    monkeypatch.setattr(harness.source_session, "execute", mock_execute)

    out = io.StringIO()
    report = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
        out=out,
    )
    output = out.getvalue()
    assert report.status_code == 1
    assert "ERROR: backup step failed: " in output
    assert "no RAG chunks were changed; rerun to finish" in output
    assert "Traceback" not in output
    assert "mode=apply selected=3 backed_up_new=0 deleted=0 sparse_tombstoned=0" in output

    # Rows still present in PlayerMilestone
    live_count = harness.source_session.scalar(select(func.count(PlayerMilestone.id)))
    assert live_count == 3

    # RAG chunks untouched
    sparse_statuses = harness.index_session.scalars(select(RagChunk.index_status)).all()
    assert all(st == "ACTIVE" for st in sparse_statuses)

    harness.close()


# --- QA follow-up (#60): curated inactive rows, in-flight PENDING publish, legacy restore ---


def _vector_row(harness: MilestoneTestHarness, where: str, params: dict[str, object]) -> tuple:
    with harness.vector_engine.begin() as conn:
        return conn.execute(
            text(f"SELECT index_status, is_active, valid_to, meta FROM rag_chunks WHERE {where}"),
            params,
        ).first()


def _has_marker(meta: object) -> bool:
    if isinstance(meta, str):
        meta = json.loads(meta or "{}")
    return isinstance(meta, dict) and bool(meta.get(CLEANUP_TOMBSTONE_META_KEY))


def test_qa2_curated_inactive_vector_row_gets_no_marker_and_publish_keeps_it_inactive() -> None:
    """QA #60 item 2: ACTIVE|is_active=false|valid_to 2099 rows must not be re-published as ACTIVE|true|NULL."""
    from src.services.rag_vector_reactivation import (
        clear_reactivation_columns_cache,
        reactivate_cleanup_tombstoned_keys,
    )

    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)
    curated_id = stable_milestone_source_row_id(2026, "101", "500홈런")
    normal_id = stable_milestone_source_row_id(2026, "102", "2500안타")
    with harness.vector_engine.begin() as conn:
        conn.execute(
            text("UPDATE rag_chunks SET is_active = 0, valid_to = '2099-12-31 00:00:00' WHERE source_row_id = :r"),
            {"r": curated_id},
        )

    rep = run_cleanup(harness.source_session, harness.index_session, harness.vector_session, apply=True, expect_count=3)
    assert rep.status_code == 0

    curated = _vector_row(harness, "source_row_id = :r", {"r": curated_id})
    assert curated[0] == "DELETED"
    assert curated[1] == 0
    assert str(curated[2]).startswith("2099-12-31")
    assert not _has_marker(curated[3])
    normal = _vector_row(harness, "source_row_id = :r", {"r": normal_id})
    assert normal[0] == "DELETED"
    assert _has_marker(normal[3])

    # A rerun reuses the recorded prior is_active (0), so the curated row still gets no marker.
    rep2 = run_cleanup(harness.source_session, harness.index_session, harness.vector_session, apply=True)
    assert rep2.status_code == 0
    assert not _has_marker(_vector_row(harness, "source_row_id = :r", {"r": curated_id})[3])

    # Simulate the publisher's ACTIVE upsert + marker reactivation for both keys.
    with harness.vector_engine.begin() as conn:
        conn.execute(
            text("UPDATE rag_chunks SET index_status = 'ACTIVE' WHERE source_row_id IN (:a, :b)"),
            {"a": curated_id, "b": normal_id},
        )
    clear_reactivation_columns_cache()
    reactivate_cleanup_tombstoned_keys(harness.vector_session, [("milestone", curated_id), ("milestone", normal_id)])
    harness.vector_session.commit()
    clear_reactivation_columns_cache()

    curated = _vector_row(harness, "source_row_id = :r", {"r": curated_id})
    assert curated[0] == "ACTIVE"
    assert curated[1] == 0
    assert str(curated[2]).startswith("2099-12-31")
    normal = _vector_row(harness, "source_row_id = :r", {"r": normal_id})
    assert normal[0] == "ACTIVE"
    assert normal[1] == 1
    assert normal[2] is None
    assert not _has_marker(normal[3])
    harness.close()


def test_qa2_curated_inactive_legacy_vector_row_gets_no_marker() -> None:
    """QA #60 item 2 (legacy path): an inactive legacy empty-key row is retired without the marker."""
    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)
    with harness.vector_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO rag_chunks (source_table, source_row_id, index_status, is_active, valid_to, meta) "
                "VALUES ('milestone', '', 'ACTIVE', 0, '2099-12-31 00:00:00', :meta)"
            ),
            {"meta": '{"player_name": "최정", "milestone_category": "500홈런"}'},
        )
    rep = run_cleanup(harness.source_session, harness.index_session, harness.vector_session, apply=True, expect_count=3)
    assert rep.status_code == 0
    legacy = _vector_row(harness, "source_row_id = ''", {})
    assert legacy[0] == "DELETED"
    assert legacy[1] == 0
    assert str(legacy[2]).startswith("2099-12-31")
    assert not _has_marker(legacy[3])
    harness.close()


@pytest.mark.parametrize("store", ["sparse", "vector"])
def test_qa3_pending_target_chunk_refuses_apply_without_writes(store: str) -> None:
    """QA #60 item 3: a PENDING target chunk (in-flight publish) makes --apply refuse with status 3 before writes."""
    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)
    pending_id = stable_milestone_source_row_id(2026, "102", "2500안타")
    if store == "sparse":
        chunk = harness.index_session.scalar(select(RagChunk).where(RagChunk.source_row_id == pending_id))
        chunk.index_status = "PENDING"
        harness.index_session.commit()
    else:
        with harness.vector_engine.begin() as conn:
            conn.execute(
                text("UPDATE rag_chunks SET index_status = 'PENDING' WHERE source_row_id = :r"), {"r": pending_id}
            )

    out_dry = io.StringIO()
    rep_dry = run_cleanup(harness.source_session, harness.index_session, harness.vector_session, out=out_dry)
    assert rep_dry.status_code == 0
    assert f"WARNING: {store}_pending=1" in out_dry.getvalue() or f"{store}_pending=1" in out_dry.getvalue()

    out = io.StringIO()
    rep = run_cleanup(
        harness.source_session, harness.index_session, harness.vector_session, apply=True, expect_count=3, out=out
    )
    assert rep.status_code == 3
    assert "in-flight publish" in (rep.error_message or "")
    assert "Refusing write" in out.getvalue()
    assert harness.source_session.scalar(select(func.count(PlayerMilestone.id))) == 3
    assert not inspect(harness.source_session.connection()).has_table(CHUNK_STATE_TABLE_NAME) or (
        harness.source_session.execute(select(func.count()).select_from(chunk_state_table)).scalar() == 0
    )
    statuses = harness.index_session.scalars(select(RagChunk.index_status)).all()
    assert "DELETED" not in statuses
    with harness.vector_engine.begin() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM rag_chunks WHERE index_status = 'DELETED'")).scalar() == 0

    # Once the publish finished (ACTIVE), the same apply goes through.
    if store == "sparse":
        harness.index_session.expire_all()
        chunk = harness.index_session.scalar(select(RagChunk).where(RagChunk.source_row_id == pending_id))
        chunk.index_status = "ACTIVE"
        harness.index_session.commit()
    else:
        with harness.vector_engine.begin() as conn:
            conn.execute(
                text("UPDATE rag_chunks SET index_status = 'ACTIVE' WHERE source_row_id = :r"), {"r": pending_id}
            )
    rep_ok = run_cleanup(
        harness.source_session, harness.index_session, harness.vector_session, apply=True, expect_count=3
    )
    assert rep_ok.status_code == 0
    harness.close()


def test_qa3_vector_key_turning_pending_mid_run_is_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    """QA #60 item 3: a vector row that becomes PENDING after the pre-flight is skipped (exit 1), not tombstoned."""
    import scripts.maintenance.cleanup_corrupted_milestones as mod

    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)
    monkeypatch.setattr(mod, "_check_pending_publish", lambda *_a, **_k: (None, None))
    pending_id = stable_milestone_source_row_id(2026, "102", "2500안타")
    with harness.vector_engine.begin() as conn:
        conn.execute(text("UPDATE rag_chunks SET index_status = 'PENDING' WHERE source_row_id = :r"), {"r": pending_id})

    out = io.StringIO()
    rep = run_cleanup(
        harness.source_session, harness.index_session, harness.vector_session, apply=True, expect_count=3, out=out
    )
    assert rep.status_code == 1
    assert "PENDING (in-flight publish); skipped" in out.getvalue()
    row = _vector_row(harness, "source_row_id = :r", {"r": pending_id})
    assert row[0] == "PENDING"
    assert row[1] == 1
    assert not _has_marker(row[3])
    ledger = harness.source_session.execute(
        select(chunk_state_table.c.chunk_ref).where(
            chunk_state_table.c.store == "vector", chunk_state_table.c.chunk_ref == pending_id
        )
    ).first()
    assert ledger is None
    harness.close()


def test_qa4_legacy_empty_key_chunks_restored_by_reactivate_chunks() -> None:
    """QA #60 item 4: reactivate_chunks restores legacy:id=<id> ledger rows (sparse and vector) of restored pairs."""
    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)

    legacy_choi = RagChunk(
        source_table="milestone",
        source_row_id="",
        content="최정 레거시",
        index_status="ACTIVE",
        meta={"player_name": "최정", "milestone_category": "500홈런"},
    )
    # The sparse store allows a single empty key per source_table (unique constraint).
    harness.index_session.add(legacy_choi)
    harness.index_session.commit()
    with harness.vector_engine.begin() as conn:
        for meta in (
            '{"player_name": "최정", "milestone_category": "500홈런"}',
            '{"player_name": "양현종", "milestone_category": "2000탈삼진"}',
        ):
            conn.execute(
                text(
                    "INSERT INTO rag_chunks (source_table, source_row_id, index_status, is_active, valid_to, meta) "
                    "VALUES ('milestone', '', 'ACTIVE', 1, NULL, :meta)"
                ),
                {"meta": meta},
            )

    rep = run_cleanup(harness.source_session, harness.index_session, harness.vector_session, apply=True, expect_count=3)
    assert rep.status_code == 0
    assert rep.legacy_sparse_tombstoned == 1
    assert rep.legacy_vector_tombstoned == 2

    # Restore 2026 only (최정 500홈런, 손아섭 2500안타); 양현종 is a 2025 row.
    harness.source_session.execute(RESTORE_ROWS_SQL, {"season": 2026})
    harness.source_session.commit()

    out = io.StringIO()
    assert (
        reactivate_chunks(harness.source_session, harness.index_session, 2026, "sparse", out=out) == 3
    )  # 2 keyed + legacy
    assert reactivate_chunks(harness.source_session, harness.vector_session, 2026, "vector", out=out) == 3

    harness.index_session.expire_all()
    assert legacy_choi.index_status == "ACTIVE"

    vec_choi = _vector_row(harness, "source_row_id = '' AND meta LIKE :m", {"m": "%최정%"})
    assert vec_choi[0] == "ACTIVE"
    assert vec_choi[1] == 1
    assert vec_choi[2] is None
    assert not _has_marker(vec_choi[3])
    vec_yang = _vector_row(harness, "source_row_id = '' AND meta LIKE :m", {"m": "%양현종%"})
    assert vec_yang[0] == "DELETED"
    assert vec_yang[1] == 0

    refs = set(
        harness.source_session.scalars(
            select(chunk_state_table.c.chunk_ref).where(chunk_state_table.c.chunk_ref.like("legacy:id=%"))
        ).all()
    )
    assert refs == {f"legacy:id={_vector_legacy_id(harness, '양현종')}"}

    # 2025 restore later picks up the remaining legacy rows; a second 2026 call is a no-op.
    assert reactivate_chunks(harness.source_session, harness.index_session, 2026, "sparse", out=out) == 0
    harness.source_session.execute(RESTORE_ROWS_SQL, {"season": 2025})
    harness.source_session.commit()
    assert reactivate_chunks(harness.source_session, harness.index_session, 2025, "sparse", out=out) == 1
    assert reactivate_chunks(harness.source_session, harness.vector_session, 2025, "vector", out=out) == 2
    assert _vector_row(harness, "source_row_id = '' AND meta LIKE :m", {"m": "%양현종%"})[:2] == ("ACTIVE", 1)
    assert harness.source_session.execute(select(func.count()).select_from(chunk_state_table)).scalar() == 0
    harness.close()


def _vector_legacy_id(harness: MilestoneTestHarness, player_name: str) -> int:
    with harness.vector_engine.begin() as conn:
        return int(
            conn.execute(
                text("SELECT id FROM rag_chunks WHERE source_row_id = '' AND meta LIKE :m"), {"m": f"%{player_name}%"}
            ).scalar()
        )


# ---------------------------------------------------------------------------
# PR #62 regression tests: SELECT/UPDATE race on the vector store
# ---------------------------------------------------------------------------


def test_build_vector_update_sql_contains_pending_guard() -> None:
    """_build_vector_update_sql must include _VECTOR_NOT_PENDING_GUARD in both has_is_active variants."""
    from scripts.maintenance.cleanup_corrupted_milestones import (
        _VECTOR_NOT_PENDING_GUARD,
        _build_vector_update_sql,
    )

    sql_with = _build_vector_update_sql(has_is_active=True, has_valid_to=True)
    assert _VECTOR_NOT_PENDING_GUARD in sql_with
    assert "'PENDING'" in sql_with

    sql_without = _build_vector_update_sql(has_is_active=False, has_valid_to=False)
    assert _VECTOR_NOT_PENDING_GUARD in sql_without
    assert "'PENDING'" in sql_without


def test_tombstone_vector_key_pending_race_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A row flipped to PENDING between SELECT and UPDATE must NOT be tombstoned; error is reported."""
    import scripts.maintenance.cleanup_corrupted_milestones as mod

    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)

    target_key = stable_milestone_source_row_id(2026, "102", "2500안타")

    original_fetch = mod._fetch_vector_chunk_row

    def racing_fetch(
        session: Session,
        source_table: str,
        source_row_id: str,
        **kw: Any,
    ) -> tuple[dict[str, Any] | None, bool]:
        result = original_fetch(session, source_table, source_row_id, **kw)
        if source_row_id == target_key:
            # Simulate in-flight publish writing PENDING through a separate connection
            with harness.vector_engine.begin() as conn:
                conn.execute(
                    text("UPDATE rag_chunks SET index_status = 'PENDING' WHERE source_row_id = :r"),
                    {"r": target_key},
                )
        return result

    monkeypatch.setattr(mod, "_fetch_vector_chunk_row", racing_fetch)

    out = io.StringIO()
    report = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
        out=out,
    )

    # The key must remain PENDING (not overwritten to DELETED)
    vec_row = harness.vector_session.execute(
        text("SELECT index_status, is_active FROM rag_chunks WHERE source_row_id = :r"),
        {"r": target_key},
    ).one()
    assert vec_row[0] == "PENDING", f"Expected PENDING but got {vec_row[0]}"

    # Report must signal error
    assert report.status_code == 1
    output = out.getvalue()
    assert "PENDING (in-flight publish); skipped" in output
    assert "row changed after read" in output

    # The tombstoned count must NOT include the racing key
    assert report.vector_tombstoned == 2
    harness.close()


def test_retire_legacy_vector_pending_race_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A legacy vector row flipped to PENDING between inspection and UPDATE is not tombstoned; error reported."""
    import scripts.maintenance.cleanup_corrupted_milestones as mod

    harness = MilestoneTestHarness(with_optional_vector_cols=True)
    _seed_corrupted(harness)

    # Seed a legacy empty-key vector chunk matching a target pair
    with harness.vector_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO rag_chunks (source_table, source_row_id, index_status, is_active, valid_to, meta) "
                "VALUES ('milestone', '', 'ACTIVE', 1, NULL, :meta)"
            ),
            {"meta": '{"player_name": "최정", "milestone_category": "500홈런"}'},
        )

    # Get the legacy chunk id
    with harness.vector_engine.begin() as conn:
        legacy_id = conn.execute(
            text("SELECT id FROM rag_chunks WHERE source_row_id = '' AND meta LIKE '%최정%'")
        ).scalar()

    original_record = mod._record_chunk_prior_state

    def racing_record(
        source_session: Session,
        *,
        store: str,
        chunk_ref: str,
        **kw: Any,
    ) -> int | None:
        result = original_record(source_session, store=store, chunk_ref=chunk_ref, **kw)
        # After recording prior state for the legacy chunk, flip it to PENDING
        if store == "vector" and chunk_ref == f"legacy:id={legacy_id}":
            with harness.vector_engine.begin() as conn:
                conn.execute(
                    text("UPDATE rag_chunks SET index_status = 'PENDING' WHERE id = :id"),
                    {"id": legacy_id},
                )
        return result

    monkeypatch.setattr(mod, "_record_chunk_prior_state", racing_record)

    out = io.StringIO()
    report = run_cleanup(
        harness.source_session,
        harness.index_session,
        harness.vector_session,
        apply=True,
        expect_count=3,
        out=out,
    )

    # The legacy chunk must remain PENDING
    with harness.vector_engine.begin() as conn:
        row = conn.execute(text("SELECT index_status FROM rag_chunks WHERE id = :id"), {"id": legacy_id}).one()
    assert row[0] == "PENDING", f"Expected PENDING but got {row[0]}"

    # Report must signal error
    assert report.status_code == 1
    assert report.legacy_vector_tombstoned == 0
    output = out.getvalue()
    assert "PENDING (in-flight publish); skipped" in output
    assert "row changed after read" in output
    harness.close()
