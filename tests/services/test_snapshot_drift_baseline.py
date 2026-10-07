"""A snapshot without a recorded baseline cannot be drift-checked, by design.

BH2-c: `validate_snapshot` compares a re-parse against the record count captured
*at crawl time*, which lives in ``capture_metadata["parsed_records"]``. That
baseline is written by the crawler when it fetches, and by nothing else.
``_mark_status`` -- the path taken by ``kbo snapshot replay --persist`` -- writes
``parser_version`` and ``error_message`` and leaves ``capture_metadata`` alone.

The consequence is easy to misread, so it is pinned here:

* **no baseline** -> ``delta is None`` -> ``drifted is False``, unconditionally
* baseline present and equal -> ``delta == 0``
* baseline present and stale -> ``drifted is True``

This is correct rather than broken. A re-parse that records *today's* parser
output as the baseline would be comparing the parser against itself, which can
never detect drift -- so refusing to judge is the honest answer. The cost is
that every snapshot produced through the replay path is permanently
un-driftable, and an operator can mistake "no baseline" for "verified".

The alternative reading -- that a snapshot with no baseline should fail closed
rather than pass -- would flood the daily gate with records that have nothing to
compare against, which is why it is not done.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.models.source_registry import DataSource, RawSourceSnapshot
from src.models.team_event import TeamEvent
from src.repositories.source_registry_repository import DataSourceRepository, RawSourceSnapshotRepository
from src.services import snapshot_persist as sp
from src.services.snapshot_replay import _baseline_count, validate_snapshot

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
    """Register a parser that re-reads exactly the records under test."""
    from src.parsers import registry as parser_registry

    monkeypatch.setitem(
        parser_registry.PARSER_REGISTRY,
        SOURCE_KEY,
        lambda raw, source_key, metadata: [{"title": t} for t in TITLES],
    )


def _seed(
    session_factory: session_factory,
    *,
    raw_path: str,
    capture_metadata: dict | None = None,
) -> int:
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
                "capture_metadata": capture_metadata,
            },
        )
        session.commit()
        return snapshot.id


def _event_rows(session_factory: sessionmaker) -> int:
    with session_factory() as session:
        return session.execute(select(func.count()).select_from(TeamEvent)).scalar_one()


class TestTheBaselineDecidesWhetherDriftIsCheckable:
    def test_a_matching_baseline_reports_no_drift(self, session_factory, artifact) -> None:
        snapshot_id = _seed(session_factory, raw_path=str(artifact), capture_metadata={"parsed_records": 4})

        result = validate_snapshot(snapshot_id, session_factory=session_factory)

        assert (result.baseline_count, result.replayed_count, result.delta) == (4, 4, 0)
        assert result.drifted is False
        assert result.success is True

    def test_a_stale_baseline_reports_drift(self, session_factory, artifact) -> None:
        """The gate works when it has a baseline -- so its silence below is meaningful."""
        snapshot_id = _seed(session_factory, raw_path=str(artifact), capture_metadata={"parsed_records": 7})

        result = validate_snapshot(snapshot_id, session_factory=session_factory)

        assert result.delta == -3
        assert result.drifted is True

    def test_without_a_baseline_the_gate_refuses_to_judge(self, session_factory, artifact) -> None:
        """`delta is None` and `drifted is False` -- the combination that reads as healthy.

        This is the state every ``kbo snapshot replay --persist`` snapshot lands
        in. Asserted explicitly so the distinction between "nothing to compare"
        and "compared and equal" cannot be lost in a refactor.
        """
        snapshot_id = _seed(session_factory, raw_path=str(artifact), capture_metadata=None)

        result = validate_snapshot(snapshot_id, session_factory=session_factory)

        assert result.baseline_count is None
        assert result.replayed_count == 4, "the re-parse itself still ran"
        assert result.delta is None
        assert result.drifted is False
        assert result.success is True

    def test_baseline_extraction_ignores_a_non_integer(self, session_factory, artifact) -> None:
        """A malformed metadata value must not become a baseline that always drifts."""
        snapshot_id = _seed(session_factory, raw_path=str(artifact), capture_metadata={"parsed_records": "many"})

        result = validate_snapshot(snapshot_id, session_factory=session_factory)

        assert _baseline_count({"parsed_records": "many"}) is None
        assert result.baseline_count is None
        assert result.drifted is False


class TestPersistCreatesUndriftableSnapshots:
    def test_the_persist_path_records_a_baseline(self, session_factory, artifact, monkeypatch) -> None:
        """BUG-CANDIDATE-007: it does not, and that is the gap.

        ``persist_snapshot`` writes ``parse_status='done'`` and the domain rows
        are correct, but ``capture_metadata`` keeps whatever the crawler left
        there -- which for a replay-created snapshot is nothing.
        """
        snapshot_id = _seed(session_factory, raw_path=str(artifact), capture_metadata=None)

        result = sp.persist_snapshot(snapshot_id, session_factory=session_factory)

        assert result.success is True
        assert result.saved == 4
        assert _event_rows(session_factory) == 4

        with session_factory() as check:
            snapshot = check.get(RawSourceSnapshot, snapshot_id)
            assert snapshot.parse_status == "done"
            assert _baseline_count(snapshot.capture_metadata) is None

        verdict = validate_snapshot(snapshot_id, session_factory=session_factory)
        assert verdict.baseline_count is None
        assert verdict.drifted is False

    def test_mark_status_does_not_touch_capture_metadata(self, session_factory, artifact) -> None:
        """The mechanism: only ``parser_version`` and ``error_message`` are written."""
        snapshot_id = _seed(session_factory, raw_path=str(artifact), capture_metadata={"other": "kept"})

        sp.persist_snapshot(snapshot_id, session_factory=session_factory)

        with session_factory() as check:
            snapshot = check.get(RawSourceSnapshot, snapshot_id)

            assert snapshot.parse_status == "done"
            assert snapshot.capture_metadata == {"other": "kept"}, "existing metadata survives untouched"
            assert "parsed_records" not in snapshot.capture_metadata
