"""A source with no parser is not a failing parser.

The daily ``snapshot_drift_check`` counts two different findings as one: it asks a
snapshot to re-parse and calls anything that did not succeed ``failed``. Two seat
sources (``lg_twins_seat``, ``seoul_stadium_seat``) have no entry in the parser
registry -- this repository has never supported them -- so every run reported two
parse failures for sources that were never parsed and were never expected to be.

That is the same shape as the mistakes already recorded in this repository: a check
that did not run reported as a check that failed, and the path errors of 2026-10-29
reported as parser defects. The gate has to separate them because the responses
differ -- an unsupported source is a coverage decision, a failing parser is a bug --
and because a threshold that absorbs the first will hide the second once enough
unsupported sources accumulate.

What is pinned here:

* an unregistered parser (or an unlinked data source) reports ``unmeasurable``;
* an unmeasurable snapshot is not counted against ``fail_max``;
* a parser that ran and raised is still ``failed`` -- the distinction has to cut
  both ways or it just relabels everything.
"""

from __future__ import annotations

import pytest

from src.services.snapshot_replay import (
    SnapshotNotReplayableError,
    SnapshotReplayError,
    SnapshotValidationResult,
    _resolve_parser,
    _SnapshotView,
    summarize_snapshot_drift,
    validate_recent_snapshots,
)


def _result(snapshot_id: int, *, success: bool = True, unmeasurable: bool = False) -> SnapshotValidationResult:
    return SnapshotValidationResult(
        snapshot_id=snapshot_id,
        source_key="some_source",
        baseline_count=None,
        replayed_count=0,
        delta=None,
        drifted=False,
        success=success,
        error=None if success else "something went wrong",
        unmeasurable=unmeasurable,
    )


class TestTheSummaryKeepsThemApart:
    def test_an_unmeasurable_snapshot_is_not_a_failed_one(self) -> None:
        summary = summarize_snapshot_drift([_result(344, success=False, unmeasurable=True)])

        assert summary.unmeasurable == 1
        assert summary.failed == 0

    def test_it_does_not_count_against_the_failure_threshold(self) -> None:
        """The gate opens on a batch that is entirely unsupported sources."""
        results = [_result(i, success=False, unmeasurable=True) for i in range(1, 6)]

        summary = summarize_snapshot_drift(results, fail_max=0)

        assert summary.unmeasurable == 5
        assert summary.ok is True

    def test_a_parser_that_ran_and_raised_is_still_failed(self) -> None:
        """Otherwise the split would just relabel every failure."""
        summary = summarize_snapshot_drift([_result(1, success=False)], fail_max=0)

        assert summary.failed == 1
        assert summary.unmeasurable == 0
        assert summary.ok is False

    def test_the_two_are_counted_separately_in_one_batch(self) -> None:
        summary = summarize_snapshot_drift(
            [
                _result(1, success=False, unmeasurable=True),
                _result(2, success=False),
                _result(3),
            ],
            fail_max=1,
        )

        assert (summary.total, summary.unmeasurable, summary.failed) == (3, 1, 1)
        assert summary.ok is True

    def test_the_unmeasurable_count_is_reported(self) -> None:
        """A number the alert reads has to be in the payload the alert reads."""
        summary = summarize_snapshot_drift([_result(1, success=False, unmeasurable=True)])

        assert summary.to_dict()["unmeasurable"] == 1


class TestTheParserLookupNamesTheReason:
    def _view(self, *, source_key: str | None) -> _SnapshotView:
        return _SnapshotView(
            snapshot_id=344,
            source_key=source_key,
            target_domain="stadium",
            raw_path="raw/abc.bin",
            source_url="https://example/",
            content_hash="abc",
            parser_version=None,
            baseline_count=None,
        )

    def test_an_unregistered_source_is_not_replayable(self) -> None:
        with pytest.raises(SnapshotNotReplayableError, match="no parser registered"):
            _resolve_parser(self._view(source_key="lg_twins_seat"))

    def test_an_unlinked_data_source_is_not_replayable(self) -> None:
        with pytest.raises(SnapshotNotReplayableError, match="no linked data source"):
            _resolve_parser(self._view(source_key=None))

    def test_it_is_still_a_replay_error_for_existing_callers(self) -> None:
        """``parse_snapshot`` and the CLI catch the parent type."""
        assert issubclass(SnapshotNotReplayableError, SnapshotReplayError)


class TestTheBatchValidationMarksIt:
    def test_an_unsupported_source_comes_back_unmeasurable(self, monkeypatch) -> None:
        def raise_not_replayable(snapshot_id, *, session_factory=None):
            raise SnapshotNotReplayableError("no parser registered for source_key=lg_twins_seat")

        monkeypatch.setattr("src.services.snapshot_replay._recent_snapshot_ids", lambda limit, factory: [344])
        monkeypatch.setattr("src.services.snapshot_replay.validate_snapshot", raise_not_replayable)

        results = validate_recent_snapshots(limit=1)

        assert [r.unmeasurable for r in results] == [True]
        assert [r.success for r in results] == [False]

    def test_a_replay_error_stays_a_failure(self, monkeypatch) -> None:
        def raise_replay_error(snapshot_id, *, session_factory=None):
            raise SnapshotReplayError("snapshot artifact not found")

        monkeypatch.setattr("src.services.snapshot_replay._recent_snapshot_ids", lambda limit, factory: [1])
        monkeypatch.setattr("src.services.snapshot_replay.validate_snapshot", raise_replay_error)

        results = validate_recent_snapshots(limit=1)

        assert [r.unmeasurable for r in results] == [False]

    def test_the_batch_summary_then_opens_the_gate(self, monkeypatch) -> None:
        """End to end over the two sources that were reporting every run."""

        def raise_not_replayable(snapshot_id, *, session_factory=None):
            raise SnapshotNotReplayableError("no parser registered")

        monkeypatch.setattr("src.services.snapshot_replay._recent_snapshot_ids", lambda limit, factory: [344, 345])
        monkeypatch.setattr("src.services.snapshot_replay.validate_snapshot", raise_not_replayable)

        summary = summarize_snapshot_drift(validate_recent_snapshots(limit=2), fail_max=0)

        assert summary.unmeasurable == 2
        assert summary.failed == 0
        assert summary.ok is True
