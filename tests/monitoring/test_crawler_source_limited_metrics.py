"""A run that never asked its source must not look like one that did.

`record_crawl_run` sets `kbo_crawl_last_success_timestamp` for any run whose
status is a success, and a compliance or robots skip is deliberately recorded as
`success` because nothing failed. The two facts together made a skipped crawler
indistinguishable from a productive one: `roster_transactions` and
`player_movement` were skipped on every run while their domain tables held no
rows after 2026-08-16, and `KboCrawlerNoRecentSuccess` stayed quiet throughout
because every skip refreshed the freshness gauge (BUG-014).

The fix reads the checkpoint the crawler already writes, so the ledger's own
evidence becomes a metric instead of staying invisible. These tests pin the
property that matters: the freshness gauge and the consulted gauge disagree
exactly when the source was never consulted.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pytest
from prometheus_client import REGISTRY

from src.monitoring import crawler_metrics as metrics


def _run(**overrides: object) -> Any:
    """Build a finished-run double, successful and consulted unless told otherwise."""
    fields: dict[str, object] = {
        "crawler": "roster_transactions",
        "status": "success",
        "finished_at": datetime(2026, 10, 10, 3, 0, 0),
        "records_read": 0,
        "records_written": 0,
        "records_failed": 0,
        "error_code": None,
        "started_at": None,
        "checkpoint": None,
    }
    fields.update(overrides)
    return type("Run", (), fields)()


def _limited(reason: str = "compliance_blocked", crawler: str = "roster_transactions") -> Any:
    return _run(
        crawler=crawler,
        status="success",
        checkpoint={"outcome": "source_limited", "reason": reason, "target_date": "2026-10-04"},
    )


def _consulted_value(crawler: str) -> float:
    return REGISTRY.get_sample_value(
        "kbo_crawl_last_source_consulted_timestamp",
        {"crawler": crawler},
    )


def _success_value(crawler: str) -> float:
    return REGISTRY.get_sample_value(
        "kbo_crawl_last_success_timestamp",
        {"crawler": crawler},
    )


def _limited_total(crawler: str, reason: str) -> float:
    """Read the counter by its exported name, which is not its `_name`.

    Three spellings are in play and only one pair agrees: `_name` is
    `kbo_crawl_source_limited`, the exposition and `get_sample_value` both want
    `..._total`, and `Counter.collect()` on the collector directly reports the
    short form again. The rule reads the exposition's spelling, so that is what
    this uses -- and the fallback below tries the short form only so a future
    prometheus_client change surfaces as a wrong number rather than as a silent
    zero.
    """
    value = REGISTRY.get_sample_value(
        "kbo_crawl_source_limited_total",
        {"crawler": crawler, "reason": reason},
    )
    if value is None:
        value = REGISTRY.get_sample_value(
            "kbo_crawl_source_limited",
            {"crawler": crawler, "reason": reason},
        )
    return value or 0.0


class TestASkippedRunIsCountedButNotConsulted:
    """The pair that was previously the same number."""

    def test_a_skip_counts_toward_the_limited_counter(self) -> None:
        before = _limited_total("roster_transactions", "compliance_blocked")
        metrics.record_crawl_run(_limited())

        assert _limited_total("roster_transactions", "compliance_blocked") == before + 1

    def test_a_skip_does_not_advance_the_consulted_gauge(self) -> None:
        """The whole fix: a run that did not ask must not claim it did.

        Left at 0 when nothing has ever asked, which reads as maximally stale --
        the correct meaning, and what makes the alert able to fire.
        """
        metrics.record_crawl_run(_limited(crawler="fresh_crawler"))

        assert _consulted_value("fresh_crawler") == 0.0

    def test_a_skip_still_advances_the_success_gauge(self) -> None:
        """Pinned so the change is not mistaken for reclassifying the run.

        Nothing failed, so the run is still a success and freshness still moves.
        Only the *consulted* signal is withheld, and the two disagreeing is the
        observable fact, not a bug to reconcile.
        """
        metrics.record_crawl_run(_limited())

        assert _success_value("roster_transactions") > 0.0

    def test_the_two_gauges_disagree_for_a_skipped_run(self) -> None:
        """Stated as the property, so a future refactor cannot quietly re-merge."""
        metrics.record_crawl_run(_limited(crawler="disagreeing"))

        assert _success_value("disagreeing") > 0.0
        assert _consulted_value("disagreeing") == 0.0


class TestAConsultedRunAdvancesBoth:
    def test_an_ordinary_success_updates_the_consulted_gauge(self) -> None:
        run = _run(crawler="game_detail", finished_at=datetime(2026, 10, 10, 5, 0, 0))

        metrics.record_crawl_run(run)

        expected = run.finished_at.replace(tzinfo=__import__("datetime").UTC).timestamp()
        assert _consulted_value("game_detail") == expected

    def test_a_failed_run_also_counts_as_consulted(self) -> None:
        """A fetch that failed still asked. Otherwise the rule would fire for
        every crawler that is erroring, which is KboCrawlerNoRecentSuccess's job
        and would double-page.
        """
        run = _run(crawler="failing", status="failed", error_code="FETCH_TIMEOUT")

        metrics.record_crawl_run(run)

        assert _consulted_value("failing") > 0.0

    def test_a_source_limited_value_is_not_double_counted(self) -> None:
        """A checkpoint that is not the limited shape must not be read as one."""
        before = _limited_total("game_detail", "compliance_blocked")

        metrics.record_crawl_run(_run(crawler="game_detail", checkpoint={"outcome": "success"}))

        assert _limited_total("game_detail", "compliance_blocked") == before


class TestTheReasonLabelIsBounded:
    """A free-form reason would let a caller mistake multiply series."""

    @pytest.mark.parametrize("reason", ["compliance_blocked", "kbo_robots_blocked"])
    def test_a_known_reason_is_used_verbatim(self, reason: str) -> None:
        before = _limited_total("bounded", reason)
        metrics.record_crawl_run(_limited(reason=reason, crawler="bounded"))

        assert _limited_total("bounded", reason) == before + 1

    def test_an_unknown_reason_is_bucketed_rather_than_dropped(self) -> None:
        """Counted under `other`, so a new reason is still visible.

        Dropping it would lose the signal entirely, and accepting it would let
        one crawler invent an unbounded label set.
        """
        before = _limited_total("bucketed", "other")
        metrics.record_crawl_run(_limited(reason="something_new", crawler="bucketed"))

        assert _limited_total("bucketed", "other") == before + 1

    def test_a_missing_reason_is_bucketed(self) -> None:
        before = _limited_total("no_reason", "other")
        metrics.record_crawl_run(
            _run(crawler="no_reason", checkpoint={"outcome": "source_limited"}),
        )

        assert _limited_total("no_reason", "other") == before + 1


class TestAMalformedCheckpointDoesNotBreakTheProjection:
    """`checkpoint` is a crawler-owned JSON column.

    The ledger is what makes runs observable, so a projection that throws on one
    malformed row would lose every row after it -- a worse failure than the one
    being fixed.
    """

    @pytest.mark.parametrize(
        "checkpoint",
        [
            "not a dict",
            ["a", "list"],
            42,
            {"outcome": "source_limited", "reason": None},
            {"outcome": None},
            {},
        ],
        ids=["string", "list", "int", "null-reason", "null-outcome", "empty"],
    )
    def test_a_checkpoint_that_is_not_the_limited_shape_is_ignored(self, checkpoint: object) -> None:
        run = _run(crawler="malformed", checkpoint=checkpoint)

        assert metrics.record_crawl_run(run) is True
        assert _consulted_value("malformed") > 0.0

    def test_a_run_without_the_attribute_is_treated_as_consulted(self) -> None:
        """The schema column is nullable, and an old row may predate it."""

        class PreadtColumnRun:
            crawler = "no_checkpoint_column"
            status = "success"
            finished_at = datetime(2026, 10, 10, 3, 0, 0)
            records_read = 0
            records_written = 0
            records_failed = 0
            error_code = None
            started_at = None

        assert metrics.record_crawl_run(PreadtColumnRun()) is True
        assert _consulted_value("no_checkpoint_column") > 0.0


class TestTheCountersStillMeanWhatTheyMeant:
    """A skipped run is still a run, and still wrote nothing."""

    def test_a_skip_is_not_counted_as_a_failure(self) -> None:
        """It did not fail. Turning it into a failure series would raise the
        fetch-error alert for a policy decision.
        """
        before = REGISTRY.get_sample_value(
            "kbo_crawl_failures_total",
            {"crawler": "not_failing", "error_code": "UNKNOWN", "failure_stage": "unknown"},
        )

        metrics.record_crawl_run(_limited(crawler="not_failing"))

        after = REGISTRY.get_sample_value(
            "kbo_crawl_failures_total",
            {"crawler": "not_failing", "error_code": "UNKNOWN", "failure_stage": "unknown"},
        )
        assert (after or 0.0) == (before or 0.0)

    def test_a_skip_records_zero_reads_and_writes(self) -> None:
        """The ledger numbers are unchanged; only the new gauge differs."""
        run = _limited(crawler="zeroes")

        metrics.record_crawl_run(run)

        assert (
            REGISTRY.get_sample_value(
                "kbo_crawl_records_written_last",
                {"crawler": "zeroes"},
            )
            == 0.0
        )


class TestTheLedgerAndTheMetricAgree:
    """The projection must read what the crawlers actually write.

    `roster_transaction_crawler` and `player_movement_crawler` set
    `checkpoint = {"outcome": "source_limited", ...}` and the two reason strings
    come from their call sites and `log_source_limited` respectively. If either
    side changed shape, the metric would silently stop counting and the alert
    would silently keep firing -- or worse, stop.
    """

    def test_the_outcome_string_matches_the_crawlers(self) -> None:
        from pathlib import Path

        crawler_dir = Path("src/crawlers")
        setters = [
            p
            for p in crawler_dir.glob("*_crawler.py")
            if '"outcome": "source_limited"' in p.read_text(encoding="utf-8")
        ]

        assert setters, "no crawler writes a source_limited checkpoint, so this contract is vacuous"
        assert metrics.SOURCE_LIMITED_OUTCOME == "source_limited"

    def test_the_robots_reason_matches_the_shared_constant(self) -> None:
        from src.utils.compliance import KBO_ROBOTS_BLOCKED_REASON

        assert KBO_ROBOTS_BLOCKED_REASON in metrics.SOURCE_LIMITED_REASONS

    def test_a_crawler_reason_outside_the_set_is_still_counted(self) -> None:
        """Any crawler-specific reason lands in `other` rather than vanishing."""
        before = _limited_total("crawler_specific", "other")
        metrics.record_crawl_run(_limited(reason="compliance_blocked_v2", crawler="crawler_specific"))

        assert _limited_total("crawler_specific", "other") == before + 1

    def test_the_threshold_is_a_duration_not_a_timestamp(self) -> None:
        """The rule compares `time()` against the gauge, so it must be Unix time.

        An accidental switch to a monotonic clock or a duration would compare
        incomparable numbers and produce a rule that never fires.
        """
        run = _run(crawler="unix_time", finished_at=datetime(2026, 10, 10, 3, 0, 0))

        metrics.record_crawl_run(run)

        epoch = datetime(2026, 10, 10, 3, 0, 0, tzinfo=__import__("datetime").UTC).timestamp()
        assert _consulted_value("unix_time") == epoch
        assert _consulted_value("unix_time") > 1_000_000_000

    def test_the_recorded_time_is_not_a_duration(self) -> None:
        """A five-minute run must not report five minutes as its timestamp."""
        started = datetime(2026, 10, 10, 3, 0, 0)
        run = _run(
            crawler="not_a_duration",
            started_at=started,
            finished_at=started + timedelta(minutes=5),
        )

        metrics.record_crawl_run(run)

        assert _consulted_value("not_a_duration") > 1_000_000_000


class TestTheRuleReadsTheNameTheEndpointExposes:
    """The counter's internal name is not the name PromQL sees.

    `prometheus_client` strips `_total` from `_name` and re-adds it on export, so
    a rule written against the internal name would reference a series that does
    not exist and stay silent forever -- while every unit test that read the
    counter through the registry kept passing. This asserts against the rendered
    exposition instead, which is what Prometheus actually scrapes.
    """

    def test_the_exported_counter_name_matches_the_rule(self) -> None:
        from prometheus_client import generate_latest
        from pathlib import Path

        metrics.CRAWL_SOURCE_LIMITED_TOTAL.labels(crawler="exposition_probe", reason="compliance_blocked").inc()
        exposition = generate_latest().decode()
        rules = Path("monitoring/prometheus/alert_rules_crawler.yml").read_text(encoding="utf-8")

        assert "kbo_crawl_source_limited_total{" in exposition
        assert "kbo_crawl_source_limited_total" in rules

    def test_the_exported_gauge_name_matches_the_rule(self) -> None:
        from prometheus_client import generate_latest
        from pathlib import Path

        metrics.CRAWL_LAST_SOURCE_CONSULTED_TIMESTAMP.labels(crawler="exposition_probe").set(1)
        exposition = generate_latest().decode()
        rules = Path("monitoring/prometheus/alert_rules_crawler.yml").read_text(encoding="utf-8")

        assert "kbo_crawl_last_source_consulted_timestamp{" in exposition
        assert "kbo_crawl_last_source_consulted_timestamp" in rules

    def test_the_internal_name_is_the_short_form(self) -> None:
        """Documents the three spellings so a reader does not "fix" one to match.

        `_name` drops the suffix, the exposition and `get_sample_value` add it
        back, and `Counter.collect()` on the collector reports it dropped. Only
        the exposition matters to the rule; the others are pinned because
        assuming any two agree is how a working rule gets edited into a silent
        one.
        """
        assert metrics.CRAWL_SOURCE_LIMITED_TOTAL._name == "kbo_crawl_source_limited"
        assert metrics.CRAWL_LAST_SOURCE_CONSULTED_TIMESTAMP._name == "kbo_crawl_last_source_consulted_timestamp"

    def test_get_sample_value_accepts_the_exported_name(self) -> None:
        """The spelling the tests and the rule both depend on."""
        collector = metrics.CRAWL_SOURCE_LIMITED_TOTAL
        collector.labels(crawler="spelling_probe", reason="compliance_blocked").inc()

        assert (
            REGISTRY.get_sample_value(
                "kbo_crawl_source_limited_total",
                {"crawler": "spelling_probe", "reason": "compliance_blocked"},
            )
            == 1.0
        )
