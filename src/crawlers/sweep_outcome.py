"""What a child crawl sweep did, in the shape its caller can aggregate.

`crawl_p1p2_data_job` runs three CLI entrypoints in-process and used to judge
them by whether they raised -- which was the only thing it could judge, because
they returned `None`. On 2026-10-19 that produced the opposite of a report: the
job logged "Completed Successfully" and alerted success while `seat` lost every
team to `FETCH_HTTP_ERROR` and `food` parsed nothing. A sweep that stored zero
rows and a sweep that stored everything were the same event.

The entrypoints now return this instead of nothing. The status vocabulary is the
ledger's (`src.models.crawl_execution`), not a parallel one: a child's outcome
describes the same run its ``track_crawl_run`` row records, and two vocabularies
for one fact is how the two readings drift apart.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from src.models.crawl_execution import (
    RUN_STATUS_FAILED,
    RUN_STATUS_PARTIAL,
    RUN_STATUS_RUNNING,
    RUN_STATUS_SUCCESS,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from src.models.crawl_execution import CrawlExecutionRun


@dataclass(frozen=True)
class CrawlSweepOutcome:
    """One child sweep's result, small enough for its caller to sum.

    Attributes:
        crawler: The child's short name, for attribution in logs and incidents.
        status: ``success``, ``partial`` or ``failed``, in the ledger's vocabulary.
        rows: Rows parsed before writing.
        written: Rows stored.
        failed: Rows the store rejected, plus units that could not be read.
        failed_units: The team codes that failed, so a warning can name them.
        error_code: The first failure's taxonomy code, when there is one.
        detail: Free text for failures this module did not observe itself.

    """

    crawler: str
    status: str
    rows: int = 0
    written: int = 0
    failed: int = 0
    failed_units: tuple[str, ...] = ()
    error_code: str | None = None
    detail: str | None = None

    @classmethod
    def from_run(cls, run: CrawlExecutionRun, *, crawler: str, failed_units: Sequence[str] = ()) -> CrawlSweepOutcome:
        """Read a run from inside its own ``track_crawl_run`` block.

        ``RUNNING`` maps to ``success`` because that is what the block's exit
        does with it: a crawler that recorded no failure of its own is finalized
        as a success. Reading it literally would make every healthy child look
        unresolved, and the caller would have no way to tell that apart from a
        genuine shortfall.

        Args:
            run: The live run row, still owned by the block that created it.
            crawler: The child's short name.
            failed_units: Team codes whose page or write failed.

        Returns:
            The outcome, detached from the session.

        """
        return cls(
            crawler=crawler,
            status=RUN_STATUS_SUCCESS if run.status == RUN_STATUS_RUNNING else run.status,
            rows=run.records_read or 0,
            written=run.records_written or 0,
            failed=run.records_failed or 0,
            failed_units=tuple(dict.fromkeys(failed_units)),
            error_code=run.error_code,
        )

    @classmethod
    def launch_failure(cls, *, crawler: str, error: BaseException) -> CrawlSweepOutcome:
        """Describe a child that raised instead of returning.

        The child recorded its own ledger row and dead letters before raising,
        so this carries no status of its own; it exists so the exception is
        visible in the sweep's report rather than deciding the whole job.

        Args:
            crawler: The child's short name.
            error: What the child raised.

        Returns:
            A ``failed`` outcome whose detail names the exception.

        """
        return cls(crawler=crawler, status=RUN_STATUS_FAILED, detail=f"{type(error).__name__}: {error}")

    @classmethod
    def unreported(cls, *, crawler: str) -> CrawlSweepOutcome:
        """Describe a child that returned without an outcome.

        Failure rather than success, deliberately. The defect this module exists
        for was a caller assuming that a returned call meant collected data;
        a child that reports nothing must not be able to reinstate that
        assumption, however healthy the build it came from.

        Args:
            crawler: The child's short name.

        Returns:
            A ``failed`` outcome with no counters.

        """
        return cls(crawler=crawler, status=RUN_STATUS_FAILED, detail="returned no outcome")

    @property
    def collected(self) -> bool:
        """Whether the child stored anything at all."""
        return self.written > 0

    def describe(self) -> str:
        """Render one line naming what happened, for a log or an incident."""
        parts = [f"{self.crawler}={self.status}", f"written={self.written}", f"failed={self.failed}"]
        if self.failed_units:
            parts.append(f"units={','.join(self.failed_units)}")
        if self.error_code:
            parts.append(f"code={self.error_code}")
        if self.detail:
            parts.append(f"detail={self.detail}")
        return " ".join(parts)

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-able form for the job registry, the marker and incidents."""
        return {
            "crawler": self.crawler,
            "status": self.status,
            "written": self.written,
            "failed": self.failed,
            "units": list(self.failed_units),
            "error_code": self.error_code,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class SweepSummary:
    """The children's outcomes, collapsed to one status and one payload.

    Attributes:
        status: ``success``, ``partial`` or ``failed`` for the sweep as a whole.
        children: The child outcomes, in the order they ran.

    """

    status: str
    children: tuple[CrawlSweepOutcome, ...]

    @property
    def written_total(self) -> int:
        """Rows stored across every child."""
        return sum(child.written for child in self.children)

    @property
    def failed_total(self) -> int:
        """Row and unit failures across every child."""
        return sum(child.failed for child in self.children)

    @property
    def unsuccessful_children(self) -> tuple[CrawlSweepOutcome, ...]:
        """Children that did not report a clean success."""
        return tuple(child for child in self.children if child.status != RUN_STATUS_SUCCESS)

    def describe(self) -> str:
        """Render every child on one line, in run order."""
        return "; ".join(child.describe() for child in self.children)

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-able form of the whole sweep."""
        return {
            "status": self.status,
            "written": self.written_total,
            "failed": self.failed_total,
            "children": [child.as_dict() for child in self.children],
        }


def summarize_sweep(outcomes: Sequence[CrawlSweepOutcome]) -> SweepSummary:
    """Collapse child outcomes into the sweep's own status.

    All children succeeded is ``success``; every child failed is ``failed``;
    anything in between is ``partial``. A child that reported ``success`` with
    nothing written is not a partial sweep -- an empty page is a legitimate
    result, and re-deciding that from counts would replace the children's
    readings with a fresh guess. What the sweep must never do is call a
    genuinely failed child healthy, and the statuses cannot: a child that lost a
    team to an unreadable page or an unwritten row records ``partial`` or
    ``failed`` for it.

    An empty sequence is ``failed``: a sweep that ran nothing collected nothing,
    and that is the one reading it must not round up.

    Args:
        outcomes: The child outcomes, in the order they ran.

    Returns:
        The summary the caller reports, alerts and marks.

    """
    children = tuple(outcomes)
    if not children:
        status = RUN_STATUS_FAILED
    elif all(child.status == RUN_STATUS_SUCCESS for child in children):
        status = RUN_STATUS_SUCCESS
    elif all(child.status == RUN_STATUS_FAILED for child in children):
        status = RUN_STATUS_FAILED
    else:
        status = RUN_STATUS_PARTIAL
    return SweepSummary(status=status, children=children)
