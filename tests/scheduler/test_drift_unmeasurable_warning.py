"""A clean drift result and an unmeasurable one must not read the same in the log.

``snapshot_drift_check`` reports ``ok`` when nothing drifted and nothing failed. A
sample where **no snapshot carries a ``parsed_records`` baseline** produces exactly
that result -- ``validate_snapshot`` cannot judge a re-parse against a count that was
never stored, so it reports ``unknown_baseline`` and the gate stays green.

Measured 2026-11-01: **0 of the most recent 100 snapshots carried one**. The check
had been reporting a clean gate for a drift it was not measuring, for as long as the
sampled sources had been capturing without recording their parse outcome.

Two things are pinned here, and the second matters as much as the first:

* the log says the result means nothing when there is nothing to compare against;
* **no incident is opened.** The condition is real but the fix is in the crawlers
  (now done), not in an operator's hands -- an alert no run can clear is the kind
  this repository has already learned to stop reading.
"""

from __future__ import annotations

import logging

import pytest

from src.scheduler.jobs.maintenance import _warn_when_drift_is_unmeasurable
from src.services.snapshot_replay import SnapshotDriftSummary


def _summary(*, total: int, with_baseline: int, unknown: int = 0) -> SnapshotDriftSummary:
    return SnapshotDriftSummary(
        total=total,
        with_baseline=with_baseline,
        unknown_baseline=unknown,
        ok=True,
    )


class TestWhenThereIsNothingToCompareAgainst:
    def test_the_log_says_the_result_means_nothing(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            _warn_when_drift_is_unmeasurable(_summary(total=100, with_baseline=0, unknown=98))

        assert any("unmeasurable" in record.message for record in caplog.records)
        assert any("98 of 100" in record.message for record in caplog.records)

    def test_it_names_the_field_the_crawlers_have_to_write(self, caplog: pytest.LogCaptureFixture) -> None:
        """A warning that does not say what to fix is noise."""
        with caplog.at_level(logging.WARNING):
            _warn_when_drift_is_unmeasurable(_summary(total=10, with_baseline=0, unknown=10))

        assert any("parsed_records" in record.message for record in caplog.records)

    def test_it_does_not_raise_or_open_anything(self, caplog: pytest.LogCaptureFixture) -> None:
        """The function's whole contract is a log line."""
        with caplog.at_level(logging.WARNING):
            _warn_when_drift_is_unmeasurable(_summary(total=100, with_baseline=0, unknown=100))

        assert len(caplog.records) == 1


class TestWhenThereIsSomethingToCompareAgainst:
    def test_a_partly_covered_sample_is_not_warned_about(self, caplog: pytest.LogCaptureFixture) -> None:
        """One comparable snapshot is enough for the check to be measuring."""
        with caplog.at_level(logging.WARNING):
            _warn_when_drift_is_unmeasurable(_summary(total=100, with_baseline=1, unknown=99))

        assert caplog.records == []

    def test_a_fully_covered_sample_is_not_warned_about(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            _warn_when_drift_is_unmeasurable(_summary(total=100, with_baseline=100))

        assert caplog.records == []


class TestWhenNothingWasSampled:
    def test_an_empty_sample_is_not_warned_about(self, caplog: pytest.LogCaptureFixture) -> None:
        """No snapshots is a different problem, and not this one."""
        with caplog.at_level(logging.WARNING):
            _warn_when_drift_is_unmeasurable(_summary(total=0, with_baseline=0))

        assert caplog.records == []
