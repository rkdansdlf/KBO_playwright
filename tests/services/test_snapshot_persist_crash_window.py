"""What survives a crash between the last domain commit and the status update.

`persist_parsed_records` commits **one transaction per record** because the
domain repositories only call `session.add`, so a single flush-time constraint
violation would otherwise roll back every record before it. `persist_snapshot`
then writes `parse_status` in a *separate* transaction afterwards.

That leaves a window with no test coverage anywhere in the repository
(`tests/crawlers/` and `tests/services/` had no commit-time crash case at all):

    [record 1 committed] [record 2 committed] ... [record N committed]
                                                    <- crash here
                                                (parse_status never written)

The result is a snapshot that is `pending` while its domain rows already exist.
Nothing is lost -- re-running is idempotent -- but the state is worth pinning
because two subsystems read `parse_status`, and neither filters on it when
picking work.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import src.services.snapshot_persist as sp
from src.models.source_registry import DataSource, RawSourceSnapshot
from src.models.team_event import TeamEvent
from src.repositories.source_registry_repository import (
    DataSourceRepository,
    RawSourceSnapshotRepository,
)
from src.services.snapshot_replay import _recent_snapshot_ids, validate_recent_snapshots

SOURCE_KEY = "lg_twins_events"
TITLES = [f"행사 {i}" for i in range(4)]


@pytest.fixture
def session_factory() -> sessionmaker:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    DataSource.__table__.create(engine)
    RawSourceSnapshot.__table__.create(engine)
    TeamEvent.__table__.create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture(autouse=True)
def _evidence_root(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CRAWL_EVIDENCE_DIR", str(tmp_path))


@pytest.fixture
def artifact(tmp_path):
    path = tmp_path / "events.json"
    path.write_text(json.dumps([{"title": t} for t in TITLES], ensure_ascii=False), encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def _probe_parser(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.parsers import registry as parser_registry

    monkeypatch.setitem(
        parser_registry.PARSER_REGISTRY,
        SOURCE_KEY,
        lambda raw, source_key, metadata: [{"title": t} for t in TITLES],
    )


def _seed(session_factory: sessionmaker, *, raw_path: str) -> int:
    with session_factory() as session:
        data_source = DataSourceRepository(session).save(
            {"source_key": SOURCE_KEY, "source_type": "official_team", "target_domain": "event"},
        )
        session.flush()
        snapshot = RawSourceSnapshotRepository(session).save(
            {
                "data_source_id": data_source.id,
                "fetched_at": datetime(2026, 9, 26, 6, 0, 0),
                "raw_html_or_json_path": raw_path,
                "source_url": "https://example.com/events",
                "content_hash": "abc123",
                "parser_version": "team-event-v1",
            },
        )
        session.commit()
        return snapshot.id


def _event_rows(session_factory: sessionmaker) -> int:
    with session_factory() as session:
        return session.execute(select(func.count()).select_from(TeamEvent)).scalar_one()


def _parse_status(session_factory: sessionmaker, snapshot_id: int) -> str:
    with session_factory() as session:
        return session.get(RawSourceSnapshot, snapshot_id).parse_status


class TestTheCrashWindowIsReachable:
    def test_every_record_commits_before_any_status_is_written(self, session_factory, artifact) -> None:
        """One transaction per record, so N rows land before `parse_status` moves.

        Called directly rather than through ``persist_snapshot`` -- that call is
        what would have written the status, and simulating its absence by
        omitting it *is* the crash.
        """
        snapshot_id = _seed(session_factory, raw_path=str(artifact))

        commits = {"n": 0}
        real_save = sp.save_parsed

        def _counting(session, target_domain, records):
            commits["n"] += 1
            return real_save(session, target_domain, records)

        sp.save_parsed = _counting
        try:
            outcome = sp.persist_parsed_records(
                session_factory,
                "event",
                [{"title": t} for t in TITLES],
            )
        finally:
            sp.save_parsed = real_save

        assert (outcome.saved, outcome.failed) == (4, 0)
        assert commits["n"] == 4, "each record got its own commit"
        assert _event_rows(session_factory) == 4

        # The crash: rows are durable, the status was never touched.
        assert _parse_status(session_factory, snapshot_id) == "pending"


class TestRecoveringFromTheWindow:
    def test_re_running_the_batch_does_not_duplicate_rows(self, session_factory, artifact) -> None:
        """Idempotence is why this window is harmless rather than a data bug."""
        _seed(session_factory, raw_path=str(artifact))
        batch = [{"title": t} for t in TITLES]

        first = sp.persist_parsed_records(session_factory, "event", batch)
        second = sp.persist_parsed_records(session_factory, "event", batch)

        assert (first.saved, second.saved) == (4, 4)
        assert _event_rows(session_factory) == 4, "the second pass upserted rather than appended"

    def test_a_later_full_run_repairs_the_status(self, session_factory, artifact) -> None:
        snapshot_id = _seed(session_factory, raw_path=str(artifact))
        batch = [{"title": t} for t in TITLES]
        sp.persist_parsed_records(session_factory, "event", batch)

        assert _parse_status(session_factory, snapshot_id) == "pending"

        result = sp.persist_snapshot(snapshot_id, session_factory=session_factory)

        assert result.success is True
        assert _parse_status(session_factory, snapshot_id) == "done"


class TestTheStuckPendingSnapshotIsInvisibleToTheDriftGate:
    """The part that is a real gap rather than a data defect.

    ``_recent_snapshot_ids`` takes the newest N snapshots with no status filter,
    so a `pending` snapshot whose rows already exist is picked up and then judged
    with no baseline -- which resolves to ``drifted=False``. It is not lost, but
    it can never be drifted either, and the daily summary counts it under
    ``with_baseline=0`` rather than flagging it.
    """

    def test_pending_snapshots_are_still_picked_up_for_drift_checking(
        self,
        session_factory,
        artifact,
    ) -> None:
        snapshot_id = _seed(session_factory, raw_path=str(artifact))
        sp.persist_parsed_records(session_factory, "event", [{"title": t} for t in TITLES])

        assert _parse_status(session_factory, snapshot_id) == "pending"
        assert _recent_snapshot_ids(limit=10, factory=session_factory) == [snapshot_id]

    def test_and_it_passes_without_a_baseline(self, session_factory, artifact) -> None:
        """`drifted=False` here means "nothing to compare", not "compared, equal"."""
        snapshot_id = _seed(session_factory, raw_path=str(artifact))
        sp.persist_parsed_records(session_factory, "event", [{"title": t} for t in TITLES])

        results = validate_recent_snapshots(limit=10, session_factory=session_factory)

        assert [r.snapshot_id for r in results] == [snapshot_id]
        assert results[0].baseline_count is None
        assert results[0].delta is None
        assert results[0].drifted is False
