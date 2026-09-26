"""Prometheus projection of crawl run ledger entries.

The run ledger in `crawl_execution_runs` is the durable record of what a crawl
did. This module projects one finished run onto Prometheus so the same facts are
queryable as time series, without duplicating the ledger's fields.

Two rules shape the design:

* **Label cardinality is bounded.** ``target_id``, ``source_url`` and ``run_id``
  stay in the ledger. Putting them on a metric would create one time series per
  game per run, which is how a Prometheus install falls over. Only the crawler
  name, the run status, and the failure classification are used, and the crawler
  name is validated before it becomes a label.
* **Measurement must never break a crawl.** A label error here would surface as
  a failed crawl run, so emission is defensive: an unusable label value is
  logged and skipped rather than raised.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

from prometheus_client import Counter, Gauge, Histogram

if TYPE_CHECKING:
    from src.models.crawl_execution import CrawlExecutionRun

logger = logging.getLogger(__name__)

#: A crawler label must be a short, stable identifier. Anything that looks like
#: a URL, a path, or a free-form string is rejected so a caller mistake cannot
#: multiply series.
SAFE_LABEL_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")

#: Buckets cover a fast team-page crawl through a full historical backfill.
DURATION_BUCKETS = (0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, float("inf"))

CRAWL_RUNS_TOTAL = Counter(
    "kbo_crawl_runs_total",
    "Crawl runs by terminal status.",
    ["crawler", "status"],
)

CRAWL_FAILURES_TOTAL = Counter(
    "kbo_crawl_failures_total",
    "Failed crawl runs by error code and pipeline stage.",
    ["crawler", "error_code", "failure_stage"],
)

CRAWL_RECORDS_READ_TOTAL = Counter(
    "kbo_crawl_records_read_total",
    "Records produced by crawl runs.",
    ["crawler"],
)

CRAWL_RECORDS_WRITTEN_TOTAL = Counter(
    "kbo_crawl_records_written_total",
    "Records persisted by crawl runs.",
    ["crawler"],
)

CRAWL_RECORDS_FAILED_TOTAL = Counter(
    "kbo_crawl_records_failed_total",
    "Records rejected during crawl persistence.",
    ["crawler"],
)

CRAWL_DURATION_SECONDS = Histogram(
    "kbo_crawl_duration_seconds",
    "Wall-clock duration of crawl runs.",
    ["crawler"],
    buckets=DURATION_BUCKETS,
)

CRAWL_LAST_SUCCESS_TIMESTAMP = Gauge(
    "kbo_crawl_last_success_timestamp",
    "Unix time of the last successful run, or 0 when a crawler has never succeeded.",
    ["crawler"],
)

CRAWL_RECORDS_WRITTEN_LAST = Gauge(
    "kbo_crawl_records_written_last",
    "Records written by the most recent run. A 7-day window over this gauge is the "
    "time-weighted baseline a write-drop rule compares against, not a true "
    "per-run average; use the run ledger for exact per-run analysis.",
    ["crawler"],
)

#: Statuses that count as a success for freshness purposes.
SUCCESS_STATUSES = frozenset({"success", "partial"})

UNKNOWN_ERROR_CODE = "UNKNOWN"
UNKNOWN_FAILURE_STAGE = "unknown"

#: Crawlers whose freshness gauge child has been created in this process.
#: Prometheus state lives in the process, so this resets with it, which is
#: exactly the lifetime the gauge needs.
_INITIALIZED_CRAWLERS: set[str] = set()


def is_safe_crawler_label(value: object) -> bool:
    """Return whether a value may be used as a bounded metric label."""
    return isinstance(value, str) and bool(SAFE_LABEL_PATTERN.match(value))


def _resolve_failure_labels(error_code: object) -> tuple[str, str]:
    """Return `(error_code, failure_stage)` for a run's error code.

    The stage is derived rather than stored: `stage_for_code` is a pure
    function, so historical runs classify identically without a schema column
    that could drift from the taxonomy.

    Args:
        error_code: The run's recorded error code, if any.

    Returns:
        A `(error_code, failure_stage)` pair, both safe label values.

    """
    from src.crawlers.failure_taxonomy import stage_for_code

    code = str(error_code) if error_code else UNKNOWN_ERROR_CODE
    if not is_safe_crawler_label(code):
        logger.warning("Unusable crawl error_code %r; reporting as %s", error_code, UNKNOWN_ERROR_CODE)
        return UNKNOWN_ERROR_CODE, UNKNOWN_FAILURE_STAGE
    try:
        return code, str(stage_for_code(code))
    except ValueError:
        # `error_code` is a free-form column, so a code written by an older or
        # different producer need not exist in the current taxonomy. Emitting a
        # metric must not be how that surfaces.
        logger.warning("Crawl error_code %r is not in the failure taxonomy; reporting as unknown", code)
        return code, UNKNOWN_FAILURE_STAGE


def record_crawl_run(run: CrawlExecutionRun) -> bool:
    """Project one finished crawl run onto the Prometheus metrics.

    The `crawler` series are created for every run, including failures, so a
    crawler that has never succeeded still exposes
    `kbo_crawl_last_success_timestamp = 0`. Without that, a "no recent success"
    rule would find no series at all and stay silent for exactly the crawler it
    needs to report on.

    Args:
        run: A `CrawlExecutionRun` that has reached a terminal state.

    Returns:
        True when the run was measured, False when it was skipped because its
        labels were unusable.

    """
    crawler = getattr(run, "crawler", None)
    if not is_safe_crawler_label(crawler):
        logger.warning("Refusing to record crawl run with unusable crawler label %r", crawler)
        return False

    status = str(getattr(run, "status", "") or "unknown")
    if not is_safe_crawler_label(status):
        status = "unknown"

    # Create the freshness series first so a never-successful crawler is visible.
    if crawler not in _INITIALIZED_CRAWLERS:
        CRAWL_LAST_SUCCESS_TIMESTAMP.labels(crawler=crawler).set(0)
        CRAWL_RECORDS_WRITTEN_LAST.labels(crawler=crawler).set(0)
        _INITIALIZED_CRAWLERS.add(crawler)

    CRAWL_RUNS_TOTAL.labels(crawler=crawler, status=status).inc()

    records_read = _safe_int(getattr(run, "records_read", 0))
    records_written = _safe_int(getattr(run, "records_written", 0))
    records_failed = _safe_int(getattr(run, "records_failed", 0))

    CRAWL_RECORDS_READ_TOTAL.labels(crawler=crawler).inc(max(0, records_read))
    CRAWL_RECORDS_WRITTEN_TOTAL.labels(crawler=crawler).inc(max(0, records_written))
    if records_failed:
        CRAWL_RECORDS_FAILED_TOTAL.labels(crawler=crawler).inc(records_failed)
    CRAWL_RECORDS_WRITTEN_LAST.labels(crawler=crawler).set(max(0, records_written))

    duration = _duration_seconds(run)
    if duration is not None:
        CRAWL_DURATION_SECONDS.labels(crawler=crawler).observe(duration)

    if status in SUCCESS_STATUSES:
        CRAWL_LAST_SUCCESS_TIMESTAMP.labels(crawler=crawler).set(_epoch_seconds(getattr(run, "finished_at", None)))
    else:
        error_code, failure_stage = _resolve_failure_labels(getattr(run, "error_code", None))
        CRAWL_FAILURES_TOTAL.labels(
            crawler=crawler,
            error_code=error_code,
            failure_stage=failure_stage,
        ).inc()

    return True


def _safe_int(value: Any) -> int:  # noqa: ANN401 - ledger columns are untyped
    """Coerce a ledger counter to a non-negative int, defaulting to zero."""
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _duration_seconds(run: CrawlExecutionRun) -> float | None:
    """Return the run duration, or None when the run has no finish time."""
    started = getattr(run, "started_at", None)
    finished = getattr(run, "finished_at", None)
    if started is None or finished is None:
        return None
    try:
        return max(0.0, (finished - started).total_seconds())
    except (AttributeError, TypeError):
        return None


def _epoch_seconds(value: object) -> float:
    """Return a datetime as a Unix timestamp, falling back to now."""
    from datetime import UTC, datetime

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.timestamp()
    return datetime.now(UTC).timestamp()


__all__ = [
    "CRAWL_DURATION_SECONDS",
    "CRAWL_FAILURES_TOTAL",
    "CRAWL_LAST_SUCCESS_TIMESTAMP",
    "CRAWL_RECORDS_FAILED_TOTAL",
    "CRAWL_RECORDS_READ_TOTAL",
    "CRAWL_RECORDS_WRITTEN_LAST",
    "CRAWL_RECORDS_WRITTEN_TOTAL",
    "CRAWL_RUNS_TOTAL",
    "DURATION_BUCKETS",
    "SAFE_LABEL_PATTERN",
    "SUCCESS_STATUSES",
    "is_safe_crawler_label",
    "record_crawl_run",
    "reset_initialized_crawlers",
]


def reset_initialized_crawlers() -> None:
    """Forget which freshness gauges were initialized.

    Prometheus registries are process-global, so tests that assert on a gauge
    need a way to start from the state a fresh process would have.
    """
    _INITIALIZED_CRAWLERS.clear()
