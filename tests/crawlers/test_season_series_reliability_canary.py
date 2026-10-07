"""The all-series crawlers' reliability chain: outcome, ledger, dead letter.

The batting and pitching crawlers are twins and share one runner, so the
contract is pinned once against the shared module and once per crawler for the
wiring that is not shared.

The load-bearing distinction is what an empty list means. A season that has not
started returns no rows, a page that lost its table returns no rows, and a
fallback that answered from the database returns whatever the database held --
which can also be nothing. The crawlers used to collapse all three into the same
empty list, so the ledger could not tell a quiet off-season from a broken crawl.
"""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import src.crawlers.season_series_outcome as outcome_module
from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.season_series_outcome import (
    SeriesRead,
    SeriesReadRecorder,
    SeriesStatus,
    run_series_crawl,
)
from src.models.crawl_execution import RUN_STATUS_FAILED, RUN_STATUS_PARTIAL, RUN_STATUS_RUNNING
from src.repositories.crawl_execution_repository import CrawlRunSpec

YEAR = 2025
SERIES = "regular"


@pytest.fixture
def ledger(monkeypatch):
    """Run the real runner against a stub ledger and a stub queue."""
    run = SimpleNamespace(
        run_id="series-run",
        status=RUN_STATUS_RUNNING,
        error_code=None,
        error_message=None,
        records_read=0,
        records_failed=0,
        checkpoint=None,
    )
    specs: list[CrawlRunSpec] = []

    @contextmanager
    def track_run(spec):
        specs.append(spec)
        yield run

    monkeypatch.setattr(outcome_module, "track_crawl_run", track_run)
    enqueue = MagicMock()
    monkeypatch.setattr(outcome_module, "enqueue_failure", enqueue)
    return run, specs, enqueue


def _crawl(reporter=None, *, rows=None, raises=None):
    """Build a crawl that reports one outcome, returns rows, or raises."""

    def crawl(recorder: SeriesReadRecorder):
        if raises is not None:
            raise raises
        if reporter is not None:
            recorder(reporter)
        return list(rows or [])

    return crawl


def _execute(crawl, *, record_dead_letters: bool = True, run_spec: CrawlRunSpec | None = None):
    return run_series_crawl(
        crawl=crawl,
        crawler="unit_crawler",
        target_type="unit_series",
        year=YEAR,
        series_key=SERIES,
        source_url="https://example.test/basic1",
        exceptions=(RuntimeError,),
        run_spec=run_spec,
        record_dead_letters=record_dead_letters,
    )


class TestTheRunCoordinates:
    def test_the_season_and_series_together_are_the_unit(self, ledger):
        _, specs, _ = ledger

        _execute(_crawl(rows=[{"p": 1}]))

        assert specs[0].target_id == f"{YEAR}:{SERIES}"
        assert specs[0].season == YEAR

    def test_a_replay_spec_is_honoured(self, ledger):
        _, specs, enqueue = ledger
        spec = CrawlRunSpec(
            crawler="unit_crawler",
            target_type="unit_series",
            target_id=f"{YEAR}:{SERIES}",
            run_id="replay-run",
            replay_of_run_id="original-run",
        )

        _execute(_crawl(rows=[{"p": 1}]), run_spec=spec, record_dead_letters=False)

        assert specs[0] is spec
        enqueue.assert_not_called()


class TestTheRowsDecideWhenTheCrawlSaysNothing:
    """SUCCESS and EMPTY are inferred, so a crawl reports only what it must."""

    def test_rows_are_a_success(self, ledger):
        run, _, enqueue = ledger

        rows = _execute(_crawl(rows=[{"p": 1}, {"p": 2}]))

        assert len(rows) == 2
        assert run.status == RUN_STATUS_RUNNING
        assert run.records_read == 2
        enqueue.assert_not_called()

    def test_no_rows_are_an_empty_season(self, ledger):
        run, _, enqueue = ledger

        rows = _execute(_crawl())

        assert rows == []
        assert run.status == RUN_STATUS_RUNNING
        assert run.error_code is None
        enqueue.assert_not_called()


class TestAFallbackIsPartialRatherThanFailed:
    def test_a_fallback_run_is_recorded_partial(self, ledger):
        run, _, enqueue = ledger
        reporter = SeriesRead(status=SeriesStatus.FALLBACK, reason="page_setup_failed", rows=3)

        rows = _execute(_crawl(reporter, rows=[{"p": 1}, {"p": 2}, {"p": 3}]))

        assert len(rows) == 3
        assert run.status == RUN_STATUS_PARTIAL
        assert run.checkpoint["status"] == str(SeriesStatus.FALLBACK)

    def test_a_fallback_does_not_queue_a_letter(self, ledger):
        """The fallback monitor already raises the incident.

        Queueing as well would make one failure into two, and the fallback is an
        accepted resolution rather than work that needs reprocessing.
        """
        _, _, enqueue = ledger
        reporter = SeriesRead(status=SeriesStatus.FALLBACK, reason="page_setup_failed", rows=0)

        _execute(_crawl(reporter, rows=[]))

        enqueue.assert_not_called()


class TestABlockedSourceIsNotAFailure:
    def test_a_compliance_skip_leaves_the_run_open(self, ledger):
        """The source was never consulted, so a retry has nothing to fix."""
        run, _, enqueue = ledger
        reporter = SeriesRead(status=SeriesStatus.BLOCKED, reason="compliance_blocked", rows=2)

        rows = _execute(_crawl(reporter, rows=[{"p": 1}, {"p": 2}]))

        assert len(rows) == 2
        assert run.status == RUN_STATUS_RUNNING
        assert run.checkpoint["outcome"] == "source_limited"
        enqueue.assert_not_called()


class TestAnUnreadableSeriesIsAFailure:
    def test_a_reported_failure_reaches_the_ledger_and_the_queue(self, ledger):
        run, _, enqueue = ledger
        reporter = SeriesRead(status=SeriesStatus.FAILED, reason="season_series_selection_failed", rows=0)

        _execute(_crawl(reporter, rows=[]))

        assert run.status == RUN_STATUS_FAILED
        assert run.error_code == FailureCode.PARSE_SELECTOR_MISSING.value
        enqueue.assert_called_once()
        letter = enqueue.call_args.args[0]
        assert letter.target_id == f"{YEAR}:{SERIES}"
        assert letter.season == YEAR

    def test_a_raised_crawl_is_recorded_rather_than_escaping(self, ledger):
        """A crawl that raises used to leave the failure to the caller.

        The loud failures are the ones most worth queueing, so the runner
        classifies them at the boundary instead of letting them past.
        """
        run, _, enqueue = ledger

        rows = _execute(_crawl(raises=RuntimeError("browser died")))

        assert rows == []
        assert run.status == RUN_STATUS_FAILED
        enqueue.assert_called_once()

    def test_a_replay_does_not_queue_a_second_letter(self, ledger):
        _, _, enqueue = ledger
        reporter = SeriesRead(status=SeriesStatus.FAILED, reason="page_setup_failed", rows=0)

        _execute(_crawl(reporter, rows=[]), record_dead_letters=False)

        enqueue.assert_not_called()


class TestTheVocabularyCarriesRetryability:
    def test_a_changed_control_is_terminal(self) -> None:
        for reason in ("compliance_blocked", "season_series_selection_failed"):
            assert SeriesRead(status=SeriesStatus.FAILED, reason=reason).is_terminal, reason

    def test_a_momentary_fault_stays_retryable(self) -> None:
        for reason in ("page_setup_failed", "crawl_error"):
            assert not SeriesRead(status=SeriesStatus.FAILED, reason=reason).is_terminal, reason

    def test_a_readable_series_carries_no_code(self) -> None:
        for status in (SeriesStatus.SUCCESS, SeriesStatus.EMPTY, SeriesStatus.FALLBACK, SeriesStatus.BLOCKED):
            read = SeriesRead(status=status)

            assert read.error_code is None
            assert read.is_terminal is False


class TestTheCrawlersAreWiredToTheSharedRunner:
    """The wiring the twins do not share, checked where it lives."""

    @pytest.mark.parametrize(
        ("module_path", "run_name", "crawler", "source"),
        [
            (
                "src.crawlers.player_batting_all_series_crawler",
                "run_batting_series",
                "player_batting_all_series",
                "Basic1.aspx",
            ),
            (
                "src.crawlers.player_pitching_all_series_crawler",
                "run_pitching_series",
                "player_pitching_all_series",
                "Basic1.aspx",
            ),
        ],
    )
    def test_each_run_passes_its_own_identity_and_page(
        self,
        module_path: str,
        run_name: str,
        crawler: str,
        source: str,
    ) -> None:
        module = __import__(module_path, fromlist=[run_name])
        request_cls = getattr(module, "BattingSeriesCrawlRequest", None) or module.PitchingSeriesCrawlRequest
        request = request_cls(year=YEAR, series_key=SERIES)

        with patch.object(module, "run_series_crawl", return_value=[]) as runner:
            getattr(module, run_name)(request)

        kwargs = runner.call_args.kwargs
        assert kwargs["crawler"] == crawler
        assert kwargs["year"] == YEAR
        assert kwargs["series_key"] == SERIES
        assert source in kwargs["source_url"]
        assert kwargs["exceptions"], "a crawl with no recorded faults would let every failure escape"
