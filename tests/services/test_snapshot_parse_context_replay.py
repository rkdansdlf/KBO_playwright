"""A time-windowed parser has to re-read the window its baseline was recorded under.

``team_event_parser`` keeps only events published no earlier than
``fetched_at - cutoff_days``. The crawler parses with ``days_back=30`` while the
parser's own default is ``CUTOFF_DAYS=60``, and the drift check's re-parse used to
pass neither — so it read *today's* window against a weeks-old artifact. Measured
2026-11-04: **6 of the 8 drifted snapshots were exactly this**. A page whose events
were all older than 30 days had crawled as 0 records and re-parsed as several; the
calendar moved and the gate called it drift.

The same fixture proves the dependency with real numbers:

| conditions | events |
| --- | --- |
| crawl (`fetched_at=2025-04-10`, `cutoff_days=30`) | 4 |
| drift re-parse (no metadata → default 60 days from *now*) | 0 |
| crawl conditions again | 4 (deterministic) |
| narrower window (`cutoff_days=7`) | 1 |

The fix has two halves, and both are pinned here and in the crawler's tests: the
crawler stores the conditions with the count, and the re-parse replays them. Without
the first half the replay has nothing to read; without the second the stored
conditions are decoration.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.services.snapshot_replay import (
    REPLAYED_PARSE_CONTEXT_KEYS,
    _run_parser,
    _SnapshotView,
)


@pytest.fixture
def artifact(tmp_path, monkeypatch):
    """An artifact inside a monkeypatched evidence root, so the load succeeds."""
    monkeypatch.setattr("src.services.snapshot_replay.evidence_root", lambda: tmp_path)
    path = tmp_path / "raw" / "abc.bin"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("<html><body>event</body></html>", encoding="utf-8")
    return path


def _view(path: Path, **overrides) -> _SnapshotView:
    fields = {
        "snapshot_id": 1,
        "source_key": "nc_dinos_events",
        "target_domain": "events",
        "raw_path": str(path),
        "source_url": "https://example.test/events",
        "content_hash": "abc",
        "parser_version": None,
        "baseline_count": 4,
        "capture_metadata": None,
    }
    fields.update(overrides)
    return _SnapshotView(**fields)


class _RecordingParser:
    def __init__(self) -> None:
        self.metadata: dict | None = None

    def __call__(self, _text: str, _source_key: str, metadata: dict) -> list[dict]:
        self.metadata = dict(metadata)
        return []


class TestTheWindowIsReplayed:
    def test_the_recorded_conditions_reach_the_parser(self, artifact) -> None:
        parser = _RecordingParser()
        view = _view(
            artifact,
            capture_metadata={
                "parsed_records": 4,
                "fetched_at": "2025-04-10T00:00:00",
                "cutoff_days": 30,
            },
        )

        _run_parser(view, parser)

        assert parser.metadata is not None
        assert parser.metadata["fetched_at"] == "2025-04-10T00:00:00"
        assert parser.metadata["cutoff_days"] == 30

    def test_the_usual_metadata_is_still_there(self, artifact) -> None:
        """The replay is additive; the snapshot's own fields keep their places."""
        parser = _RecordingParser()

        _run_parser(
            _view(artifact, capture_metadata={"fetched_at": "2025-04-10T00:00:00", "cutoff_days": 30}),
            parser,
        )

        assert parser.metadata is not None
        assert parser.metadata["snapshot_id"] == 1
        assert parser.metadata["source_url"] == "https://example.test/events"
        assert parser.metadata["content_hash"] == "abc"

    def test_every_named_key_is_the_one_the_crawler_writes(self) -> None:
        """The two halves of the contract have to name the same keys."""
        assert set(REPLAYED_PARSE_CONTEXT_KEYS) == {"fetched_at", "cutoff_days"}


class TestTheEdges:
    def test_without_a_recorded_window_nothing_is_injected(self, artifact) -> None:
        """A parser that needs no window must not receive one anyway."""
        parser = _RecordingParser()

        _run_parser(_view(artifact, capture_metadata={"parsed_records": 4}), parser)

        assert parser.metadata is not None
        for key in REPLAYED_PARSE_CONTEXT_KEYS:
            assert key not in parser.metadata

    def test_a_missing_capture_metadata_is_not_an_error(self, artifact) -> None:
        """Rows captured before this contract existed still parse."""
        parser = _RecordingParser()

        _run_parser(_view(artifact, capture_metadata=None), parser)

        assert parser.metadata is not None
        assert parser.metadata["snapshot_id"] == 1

    def test_a_none_valued_key_is_not_replayed(self, artifact) -> None:
        """``None`` is absence; injecting it would flip the parser to its fallback
        under a key that looks deliberate.
        """
        parser = _RecordingParser()

        _run_parser(_view(artifact, capture_metadata={"fetched_at": None, "cutoff_days": 30}), parser)

        assert parser.metadata is not None
        assert "fetched_at" not in parser.metadata
        assert parser.metadata["cutoff_days"] == 30
