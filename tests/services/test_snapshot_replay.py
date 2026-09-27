"""Tests for read-only snapshot-driven parser replay (Phase E1)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.models.source_registry import DataSource, RawSourceSnapshot
from src.repositories.source_registry_repository import (
    DataSourceRepository,
    RawSourceSnapshotRepository,
)
from src.services.snapshot_replay import (
    SnapshotNotFoundError,
    SnapshotReplayError,
    replay_recent_snapshots,
    replay_snapshot,
    validate_recent_snapshots,
    validate_snapshot,
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
    return sessionmaker(bind=engine, expire_on_commit=False)


def _seed(
    session_factory: sessionmaker,
    *,
    raw_path: str | None,
    source_key: str | None = "lg_twins_events",
    parser_version: str | None = "team-event-v1",
    baseline: int | None = None,
) -> int:
    with session_factory() as session:
        data_source = DataSourceRepository(session).save(
            {"source_key": source_key or "orphan_source", "source_type": "official_team", "target_domain": "event"},
        )
        session.flush()
        snapshot = RawSourceSnapshotRepository(session).save(
            {
                "data_source_id": data_source.id,
                "fetched_at": datetime(2026, 9, 26, 6, 0, 0),
                "raw_html_or_json_path": raw_path,
                "source_url": "https://example.com/events",
                "content_hash": "abc123",
                "capture_metadata": {"parsed_records": baseline} if baseline is not None else None,
                "parser_version": parser_version,
            },
        )
        session.commit()
        return snapshot.id


def _fake_parser(items: int = 2):
    def _parse(_text: str, source_key: str, _metadata: dict | None = None) -> list[dict]:
        return [{"source_key": source_key, "n": i} for i in range(items)]

    return _parse


def test_successful_replay_returns_parsed_count(session_factory, tmp_path: Path, monkeypatch) -> None:
    artifact = tmp_path / "snap.bin"
    artifact.write_text("<html>ok</html>", encoding="utf-8")
    snapshot_id = _seed(session_factory, raw_path=str(artifact))
    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _fake_parser(3))

    result = replay_snapshot(snapshot_id, session_factory=session_factory)

    assert result.success is True
    assert result.parsed_count == 3
    assert result.source_key == "lg_twins_events"
    assert result.parser_version == "team-event-v1"
    assert result.error is None


def test_missing_snapshot_raises(session_factory) -> None:
    with pytest.raises(SnapshotNotFoundError):
        replay_snapshot(999, session_factory=session_factory)


def test_no_parser_raises(session_factory, tmp_path: Path, monkeypatch) -> None:
    artifact = tmp_path / "snap.bin"
    artifact.write_text("x", encoding="utf-8")
    snapshot_id = _seed(session_factory, raw_path=str(artifact))
    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: None)
    with pytest.raises(SnapshotReplayError, match="no parser"):
        replay_snapshot(snapshot_id, session_factory=session_factory)


def test_url_artifact_is_rejected(session_factory, monkeypatch) -> None:
    snapshot_id = _seed(session_factory, raw_path="https://example.com/raw.html")
    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _fake_parser())
    result = replay_snapshot(snapshot_id, session_factory=session_factory)
    assert result.success is False
    assert "URL" in (result.error or "")


def test_missing_artifact_file_is_reported(session_factory, tmp_path: Path, monkeypatch) -> None:
    snapshot_id = _seed(session_factory, raw_path=str(tmp_path / "does-not-exist.bin"))
    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _fake_parser())
    result = replay_snapshot(snapshot_id, session_factory=session_factory)
    assert result.success is False
    assert "not found" in (result.error or "")


def test_parser_exception_is_captured(session_factory, tmp_path: Path, monkeypatch) -> None:
    artifact = tmp_path / "snap.bin"
    artifact.write_text("x", encoding="utf-8")
    snapshot_id = _seed(session_factory, raw_path=str(artifact))

    def _boom(*_a: object, **_k: object) -> list[dict]:
        raise ValueError("bad html")

    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _boom)
    result = replay_snapshot(snapshot_id, session_factory=session_factory)
    assert result.success is False
    assert result.error == "bad html"


def test_replay_is_read_only(session_factory, tmp_path: Path, monkeypatch) -> None:
    artifact = tmp_path / "snap.bin"
    artifact.write_text("x", encoding="utf-8")
    snapshot_id = _seed(session_factory, raw_path=str(artifact))
    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _fake_parser())

    with session_factory() as session:
        before = (session.query(DataSource).count(), session.query(RawSourceSnapshot).count())
    replay_snapshot(snapshot_id, session_factory=session_factory)
    with session_factory() as session:
        after = (session.query(DataSource).count(), session.query(RawSourceSnapshot).count())
    assert before == after


def test_validate_match_has_no_drift(session_factory, tmp_path: Path, monkeypatch) -> None:
    artifact = tmp_path / "snap.bin"
    artifact.write_text("x", encoding="utf-8")
    snapshot_id = _seed(session_factory, raw_path=str(artifact), baseline=3)
    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _fake_parser(3))

    result = validate_snapshot(snapshot_id, session_factory=session_factory)

    assert result.success is True
    assert result.baseline_count == 3
    assert result.replayed_count == 3
    assert result.delta == 0
    assert result.drifted is False


def test_validate_detects_drift(session_factory, tmp_path: Path, monkeypatch) -> None:
    artifact = tmp_path / "snap.bin"
    artifact.write_text("x", encoding="utf-8")
    snapshot_id = _seed(session_factory, raw_path=str(artifact), baseline=5)
    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _fake_parser(3))

    result = validate_snapshot(snapshot_id, session_factory=session_factory)

    assert result.delta == -2
    assert result.drifted is True


def test_validate_without_baseline_is_unknown(session_factory, tmp_path: Path, monkeypatch) -> None:
    artifact = tmp_path / "snap.bin"
    artifact.write_text("x", encoding="utf-8")
    snapshot_id = _seed(session_factory, raw_path=str(artifact))
    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _fake_parser(2))

    result = validate_snapshot(snapshot_id, session_factory=session_factory)

    assert result.baseline_count is None
    assert result.delta is None
    assert result.drifted is False


def test_validate_captures_parser_error(session_factory, tmp_path: Path, monkeypatch) -> None:
    artifact = tmp_path / "snap.bin"
    artifact.write_text("x", encoding="utf-8")
    snapshot_id = _seed(session_factory, raw_path=str(artifact), baseline=1)

    def _boom(*_a: object, **_k: object) -> list[dict]:
        raise ValueError("bad html")

    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _boom)
    result = validate_snapshot(snapshot_id, session_factory=session_factory)
    assert result.success is False
    assert result.error == "bad html"


def test_validate_recent_isolates_failures(session_factory, tmp_path: Path, monkeypatch) -> None:
    good = tmp_path / "good.bin"
    good.write_text("x", encoding="utf-8")
    _seed(session_factory, raw_path=str(good), baseline=1)
    _seed(session_factory, raw_path="https://example.com/raw.html", source_key="other_source")
    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _fake_parser(1))

    results = validate_recent_snapshots(limit=10, session_factory=session_factory)

    assert len(results) == 2
    assert {result.success for result in results} == {True, False}


def test_replay_recent_isolates_failures(session_factory, tmp_path: Path, monkeypatch) -> None:
    good = tmp_path / "good.bin"
    good.write_text("x", encoding="utf-8")
    _seed(session_factory, raw_path=str(good))
    _seed(session_factory, raw_path=str(tmp_path / "missing.bin"), source_key="other_source")
    monkeypatch.setattr("src.services.snapshot_replay.get_parser", lambda _key: _fake_parser(1))

    results = replay_recent_snapshots(limit=10, session_factory=session_factory)

    assert len(results) == 2
    assert {result.success for result in results} == {True, False}
