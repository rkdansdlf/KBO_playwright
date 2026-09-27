"""Read-only replay and validation of stored raw source snapshots.

Unlike ``scripts/batch_parse_snapshots`` (which re-fetches the URL), this reads
the content-addressed artifact recorded at crawl time, so parser changes can be
re-validated without any network call or database write.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from src.db.engine import SessionLocal
from src.parsers.registry import get_parser
from src.repositories.source_registry_repository import (
    DataSourceRepository,
    RawSourceSnapshotRepository,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_URL_PREFIXES = ("http://", "https://")


class SnapshotReplayError(RuntimeError):
    """Raised when a snapshot cannot be replayed from its stored artifact."""


class SnapshotNotFoundError(SnapshotReplayError):
    """Raised when the snapshot id does not exist."""

    def __init__(self, snapshot_id: int) -> None:
        """Initialize with the missing snapshot id."""
        self.snapshot_id = snapshot_id
        super().__init__(f"Snapshot not found: {snapshot_id}")


@dataclass(frozen=True)
class SnapshotParseResult:
    """Result of re-parsing one stored snapshot (records included)."""

    snapshot_id: int
    source_key: str | None
    parser_version: str | None
    records: list[dict] = field(default_factory=list)
    success: bool = True
    error: str | None = None

    @property
    def parsed_count(self) -> int:
        """Return the number of parsed records."""
        return len(self.records)


@dataclass(frozen=True)
class SnapshotReplayResult:
    """Outcome of replaying one snapshot through its parser."""

    snapshot_id: int
    source_key: str | None
    parser_version: str | None
    parsed_count: int
    success: bool
    error: str | None = None


@dataclass(frozen=True)
class SnapshotValidationResult:
    """Drift report comparing a re-parse against the recorded baseline count."""

    snapshot_id: int
    source_key: str | None
    baseline_count: int | None
    replayed_count: int
    delta: int | None
    drifted: bool
    success: bool
    error: str | None = None


@dataclass(frozen=True)
class _SnapshotView:
    """Plain view of a snapshot + its data source, safe after session close."""

    snapshot_id: int
    source_key: str | None
    target_domain: str | None
    raw_path: str | None
    source_url: str | None
    content_hash: str | None
    parser_version: str | None
    baseline_count: int | None


@dataclass(frozen=True)
class _ParserResult:
    records: list[dict]
    error: str | None


def load_snapshot_text(raw_path: str | None) -> str:
    """Load the stored snapshot artifact as text, rejecting URL-style paths."""
    if not raw_path:
        msg = "snapshot has no stored artifact path"
        raise SnapshotReplayError(msg)
    if raw_path.startswith(_URL_PREFIXES):
        msg = f"snapshot artifact is a URL, not a replayable file: {raw_path}"
        raise SnapshotReplayError(msg)
    path = Path(raw_path)
    if not path.is_file():
        msg = f"stored snapshot artifact not found: {path}"
        raise SnapshotReplayError(msg)
    return path.read_text(encoding="utf-8", errors="replace")


def _run_parser(view: _SnapshotView, parser: Callable[..., list[dict]]) -> _ParserResult:
    try:
        text = load_snapshot_text(view.raw_path)
        metadata = {
            "snapshot_id": view.snapshot_id,
            "source_url": view.source_url,
            "content_hash": view.content_hash,
            "target_domain": view.target_domain,
        }
        parsed = parser(text, view.source_key or "", metadata)
    except SnapshotReplayError as exc:
        return _ParserResult([], str(exc))
    except Exception as exc:
        logger.exception("Snapshot replay parser failed for snapshot %s", view.snapshot_id)
        return _ParserResult([], str(exc))
    return _ParserResult(list(parsed), None)


def _baseline_count(capture_metadata: dict | None) -> int | None:
    if not capture_metadata:
        return None
    raw = capture_metadata.get("parsed_records")
    return raw if isinstance(raw, int) else None


def _load_view(session: Session, snapshot_id: int) -> _SnapshotView:
    snapshot = RawSourceSnapshotRepository(session).get_by_id(snapshot_id)
    if snapshot is None:
        raise SnapshotNotFoundError(snapshot_id)
    data_source = DataSourceRepository(session).get_by_id(snapshot.data_source_id)
    return _SnapshotView(
        snapshot_id=snapshot.id,
        source_key=data_source.source_key if data_source is not None else None,
        target_domain=data_source.target_domain if data_source is not None else None,
        raw_path=snapshot.raw_html_or_json_path,
        source_url=snapshot.source_url,
        content_hash=snapshot.content_hash,
        parser_version=snapshot.parser_version,
        baseline_count=_baseline_count(snapshot.capture_metadata),
    )


def _resolve_parser(view: _SnapshotView) -> Callable[..., list[dict]]:
    if view.source_key is None:
        msg = f"snapshot {view.snapshot_id} has no linked data source"
        raise SnapshotReplayError(msg)
    parser = get_parser(view.source_key)
    if parser is None:
        msg = f"no parser registered for source_key={view.source_key}"
        raise SnapshotReplayError(msg)
    return parser


def parse_snapshot(
    snapshot_id: int,
    *,
    session_factory: Callable[[], Session] | None = None,
) -> SnapshotParseResult:
    """Re-parse one stored snapshot from its artifact without writing to the DB."""
    factory: Callable[[], Session] = session_factory or SessionLocal
    with factory() as session:
        view = _load_view(session, snapshot_id)
    parser = _resolve_parser(view)
    result = _run_parser(view, parser)
    return SnapshotParseResult(
        snapshot_id=view.snapshot_id,
        source_key=view.source_key,
        parser_version=view.parser_version,
        records=result.records,
        success=result.error is None,
        error=result.error,
    )


def replay_snapshot(
    snapshot_id: int,
    *,
    session_factory: Callable[[], Session] | None = None,
) -> SnapshotReplayResult:
    """Replay one stored snapshot, reporting only the parsed count (read-only)."""
    parsed = parse_snapshot(snapshot_id, session_factory=session_factory)
    return SnapshotReplayResult(
        snapshot_id=parsed.snapshot_id,
        source_key=parsed.source_key,
        parser_version=parsed.parser_version,
        parsed_count=parsed.parsed_count,
        success=parsed.success,
        error=parsed.error,
    )


def replay_recent_snapshots(
    *,
    limit: int = 50,
    session_factory: Callable[[], Session] | None = None,
) -> list[SnapshotReplayResult]:
    """Replay the most recent snapshots, isolating per-snapshot failures."""
    factory: Callable[[], Session] = session_factory or SessionLocal
    with factory() as session:
        snapshot_ids = [snapshot.id for snapshot in RawSourceSnapshotRepository(session).get_recent(limit=limit)]

    results: list[SnapshotReplayResult] = []
    for snapshot_id in snapshot_ids:
        try:
            results.append(replay_snapshot(snapshot_id, session_factory=factory))
        except SnapshotReplayError as exc:
            results.append(
                SnapshotReplayResult(
                    snapshot_id=snapshot_id,
                    source_key=None,
                    parser_version=None,
                    parsed_count=0,
                    success=False,
                    error=str(exc),
                ),
            )
    return results


def validate_snapshot(
    snapshot_id: int,
    *,
    session_factory: Callable[[], Session] | None = None,
) -> SnapshotValidationResult:
    """Compare a re-parse against the count recorded at crawl time (read-only)."""
    factory: Callable[[], Session] = session_factory or SessionLocal
    with factory() as session:
        view = _load_view(session, snapshot_id)
    parser = _resolve_parser(view)
    parsed = _run_parser(view, parser)
    replayed_count = len(parsed.records)
    baseline = view.baseline_count
    delta = (replayed_count - baseline) if baseline is not None else None
    return SnapshotValidationResult(
        snapshot_id=view.snapshot_id,
        source_key=view.source_key,
        baseline_count=baseline,
        replayed_count=replayed_count,
        delta=delta,
        drifted=delta is not None and delta != 0,
        success=parsed.error is None,
        error=parsed.error,
    )


def validate_recent_snapshots(
    *,
    limit: int = 50,
    session_factory: Callable[[], Session] | None = None,
) -> list[SnapshotValidationResult]:
    """Validate the most recent snapshots, isolating per-snapshot failures."""
    factory: Callable[[], Session] = session_factory or SessionLocal
    with factory() as session:
        snapshot_ids = [snapshot.id for snapshot in RawSourceSnapshotRepository(session).get_recent(limit=limit)]

    results: list[SnapshotValidationResult] = []
    for snapshot_id in snapshot_ids:
        try:
            results.append(validate_snapshot(snapshot_id, session_factory=factory))
        except SnapshotReplayError as exc:
            results.append(
                SnapshotValidationResult(
                    snapshot_id=snapshot_id,
                    source_key=None,
                    baseline_count=None,
                    replayed_count=0,
                    delta=None,
                    drifted=False,
                    success=False,
                    error=str(exc),
                ),
            )
    return results
