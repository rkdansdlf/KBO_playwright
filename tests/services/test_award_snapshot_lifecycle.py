"""Tests for obsolete award snapshot classification."""

from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

from src.services.award_snapshot_lifecycle import find_supersede_candidates


def test_pending_snapshot_with_later_done_snapshot_is_superseded() -> None:
    """Select only pending rows replaced by a later successful capture."""
    base = datetime(2026, 8, 16, 10, 0)
    sources = [SimpleNamespace(id=1, source_key="kbo_awards_wikipedia")]
    snapshots = [
        SimpleNamespace(id=25, data_source_id=1, parse_status="pending", fetched_at=base),
        SimpleNamespace(id=49, data_source_id=1, parse_status="done", fetched_at=base + timedelta(hours=2)),
        SimpleNamespace(id=50, data_source_id=1, parse_status="pending", fetched_at=base + timedelta(hours=3)),
    ]

    candidates = find_supersede_candidates(sources, snapshots)

    assert len(candidates) == 1
    assert candidates[0].snapshot_id == 25
    assert candidates[0].replacement_snapshot_id == 49
