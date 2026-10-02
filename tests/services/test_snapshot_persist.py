"""Tests for guarded snapshot domain persistence (E6)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.models.source_registry import DataSource, RawSourceSnapshot
from src.models.team_event import TeamEvent
from src.repositories.source_registry_repository import (
    DataSourceRepository,
    RawSourceSnapshotRepository,
)
from src.services.snapshot_persist import (
    SaveOutcome,
    persist_recent_snapshots,
    persist_snapshot,
    save_parsed,
    supported_domains,
)


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
def _evidence_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the evidence root at the per-test tmp dir so artifacts are in-scope."""
    monkeypatch.setenv("CRAWL_EVIDENCE_DIR", str(tmp_path))


def _seed(
    session_factory: sessionmaker,
    *,
    raw_path: str | None,
    target_domain: str = "event",
    source_key: str = "lg_twins_events",
) -> int:
    with session_factory() as session:
        data_source = DataSourceRepository(session).save(
            {"source_key": source_key, "source_type": "official_team", "target_domain": target_domain},
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


def _event_parser(title: str = "t"):
    def _parse(_text: str, source_key: str, _metadata: dict | None = None) -> list[dict]:
        return [{"team_id": "LG", "title": title, "source_url": f"https://example.com/{title}"}]

    return _parse


def _parse_status(session_factory: sessionmaker, snapshot_id: int) -> str:
    with session_factory() as session:
        snapshot = RawSourceSnapshotRepository(session).get_by_id(snapshot_id)
        return snapshot.parse_status


def test_persist_success_and_marks_done(session_factory, tmp_path: Path, monkeypatch) -> None:
    artifact = tmp_path / "snap.bin"
    artifact.write_text("<html/>", encoding="utf-8")
    snapshot_id = _seed(session_factory, raw_path=str(artifact))
    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _event_parser())

    result = persist_snapshot(snapshot_id, session_factory=session_factory)

    assert result.success is True
    assert result.saved == 1
    assert result.target_domain == "event"
    assert _parse_status(session_factory, snapshot_id) == "done"
    with session_factory() as session:
        assert session.query(TeamEvent).count() == 1


def test_persist_is_idempotent(session_factory, tmp_path: Path, monkeypatch) -> None:
    artifact = tmp_path / "snap.bin"
    artifact.write_text("<html/>", encoding="utf-8")
    snapshot_id = _seed(session_factory, raw_path=str(artifact))
    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _event_parser())

    persist_snapshot(snapshot_id, session_factory=session_factory)
    persist_snapshot(snapshot_id, session_factory=session_factory)

    with session_factory() as session:
        assert session.query(TeamEvent).count() == 1


def test_persist_unsupported_domain_is_skipped(session_factory, tmp_path: Path, monkeypatch) -> None:
    artifact = tmp_path / "snap.bin"
    artifact.write_text("<html/>", encoding="utf-8")
    snapshot_id = _seed(session_factory, raw_path=str(artifact), target_domain="schedule")
    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _event_parser())

    result = persist_snapshot(snapshot_id, session_factory=session_factory)

    assert result.skipped is True
    assert result.success is False
    assert _parse_status(session_factory, snapshot_id) == "pending"


def test_persist_parse_failure_marks_failed(session_factory, tmp_path: Path, monkeypatch) -> None:
    artifact = tmp_path / "snap.bin"
    artifact.write_text("<html/>", encoding="utf-8")
    snapshot_id = _seed(session_factory, raw_path=str(artifact))

    def _boom(*_a: object, **_k: object) -> list[dict]:
        raise ValueError("bad html")

    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _boom)

    result = persist_snapshot(snapshot_id, session_factory=session_factory)

    assert result.success is False
    assert _parse_status(session_factory, snapshot_id) == "failed"


def test_persist_recent_isolates_failures(session_factory, tmp_path: Path, monkeypatch) -> None:
    good = tmp_path / "good.bin"
    good.write_text("<html/>", encoding="utf-8")
    _seed(session_factory, raw_path=str(good))
    _seed(session_factory, raw_path="https://example.com/raw.html", source_key="other_source")
    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _event_parser())

    results = persist_recent_snapshots(limit=10, session_factory=session_factory)

    assert len(results) == 2
    assert any(result.success for result in results)


def test_persist_result_outcome_status() -> None:
    from src.services.snapshot_persist import SnapshotPersistResult

    assert SnapshotPersistResult(1, "k", "event", 3, True).outcome_status == "saved"
    partial = SnapshotPersistResult(1, "k", "event", 1, False, error="x", failed_count=1)
    assert partial.outcome_status == "partial"
    failed = SnapshotPersistResult(1, "k", "event", 0, False, error="x", failed_count=2)
    assert failed.outcome_status == "failed"
    skipped = SnapshotPersistResult(1, "k", None, 0, False, error="x", skipped=True)
    assert skipped.outcome_status == "skipped"


def test_supported_domains_and_save_parsed_unknown(session_factory) -> None:
    assert {"event", "ticket", "seat", "roster", "parking", "food"} <= supported_domains()
    with session_factory() as session:
        outcome = save_parsed(session, "unknown-domain", [{"x": 1}])
    assert outcome.saved == 0
    assert outcome.failed == 1


def test_save_flat_counts_failures(session_factory, monkeypatch) -> None:
    class _FlakyRepo:
        def __init__(self, _session: object) -> None:
            pass

        def save(self, item: dict) -> object:
            if item.get("title") == "bad":
                raise ValueError("nope")
            return object()

    import src.services.snapshot_persist as snapshot_persist

    monkeypatch.setitem(snapshot_persist.DOMAIN_FLAT_REPOS, "event", _FlakyRepo)
    with session_factory() as session:
        outcome = save_parsed(session, "event", [{"title": "ok"}, {"title": "bad"}, {"title": "ok2"}])
    assert outcome == SaveOutcome(saved=2, failed=1)


def test_persist_partial_marks_partial(session_factory, tmp_path: Path, monkeypatch) -> None:
    artifact = tmp_path / "snap.bin"
    artifact.write_text("<html/>", encoding="utf-8")
    snapshot_id = _seed(session_factory, raw_path=str(artifact))
    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _event_parser())
    monkeypatch.setattr(
        "src.services.snapshot_persist.save_parsed",
        lambda *_a, **_k: SaveOutcome(saved=1, failed=1),
    )

    result = persist_snapshot(snapshot_id, session_factory=session_factory)

    assert result.success is False
    assert result.failed_count == 1
    assert result.saved == 1
    assert _parse_status(session_factory, snapshot_id) == "partial"
