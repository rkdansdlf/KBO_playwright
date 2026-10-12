"""One owner for the vocabulary the run ledger writes.

The ledger speaks three dialects and none of them was written down. A run's
`status` says whether the run completed. An `error_code` says why it failed. A
`checkpoint` JSON blob carries an `outcome` and a `reason` for runs that completed
without collecting -- and those are recorded as `success`, because the run did
what it was asked and the answer was "nothing here".

That last part cost a wrong conclusion twice. `source_limited` was looked for in
`status` and found zero times, which read as "the fix is not deployed" when it was
-- 90 rows carry it. The refusal code was then hardcoded as `FETCH_BLOCKED`, which
is the retired spelling; the live one was invisible. An operator reading a column
had nowhere to look up what a value meant.

So the vocabulary lives here, and the writers and the readers both come to this
module. Nothing here changes what is written; it names what is.
"""

from __future__ import annotations

import json
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime


class RunStatus(StrEnum):
    """What `crawl_execution_runs.status` can hold."""

    SUCCESS = "success"
    FAILED = "failed"
    RUNNING = "running"
    PARTIAL = "partial"
    INTERRUPTED = "interrupted"

    @property
    def terminal(self) -> bool:
        """Return whether the run has finished under this status."""
        return self is not RunStatus.RUNNING


class CheckpointOutcome(StrEnum):
    """What a completed run carries in `checkpoint.outcome` when it collected nothing.

    Recorded beside a `success` status on purpose. The run finished; the answer is
    that the source was not consulted, and calling that a failure would put a
    robots refusal into the retry path, which is where it caused the most damage:
    115 letters that no retry could ever clear.
    """

    SOURCE_LIMITED = "source_limited"


class SourceLimitReason(StrEnum):
    """Why a run was source-limited."""

    KBO_ROBOTS_BLOCKED = "kbo_robots_blocked"


#: Outcomes that mean a source was refused rather than unreachable.
#:
#: Separate from the error codes on purpose. A refusal and an outage produce the
#: same empty result and opposite remedies, and a reader that conflated them
#: would send a robots block into the retry queue.
REFUSAL_OUTCOMES = frozenset({CheckpointOutcome.SOURCE_LIMITED.value})

#: Refusals recorded the older way, as an error code. `FETCH_BLOCKED` is what the
#: food crawler still writes; new code writes the outcome above.
REFUSAL_ERROR_CODES = frozenset({"FETCH_BLOCKED"})


def parse_checkpoint(value: object) -> dict[str, object]:
    """Return a `checkpoint` column as a mapping, whatever dialect stored it.

    Args:
        value: The raw column value -- a mapping already, a JSON string, or None.

    Returns:
        The parsed mapping, empty when there is nothing to parse.

    """
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def is_refusal(*, error_code: str | None = None, checkpoint: object = None) -> bool:
    """Return whether a run recorded a refusal rather than an unreachable source.

    Args:
        error_code: The run's `error_code`.
        checkpoint: The run's raw `checkpoint` column.

    Returns:
        True when the run was refused by a source's policy.

    """
    if error_code and error_code in REFUSAL_ERROR_CODES:
        return True
    parsed = parse_checkpoint(checkpoint)
    return str(parsed.get("outcome") or "") in REFUSAL_OUTCOMES


def is_source_limited(checkpoint: object = None) -> bool:
    """Return whether the run was source-limited under the current vocabulary."""
    return str(parse_checkpoint(checkpoint).get("outcome") or "") == CheckpointOutcome.SOURCE_LIMITED.value


#: What each column's values mean, as data rather than as branches.
#:
#: A vocabulary is a table, and writing it as control flow meant every new value
#: added a branch to walk past. Absence is meaningful here too: a value that is not
#: in the table has no explanation, and saying so is more useful than a confident
#: guess, because the reader can then go looking for it.
EXPLANATIONS: dict[str, dict[str, str]] = {
    "status": {
        RunStatus.SUCCESS.value: (
            "the run completed; what it collected is in the checkpoint outcome or the record counts"
        ),
        RunStatus.PARTIAL.value: "the run completed for some targets and not others",
        RunStatus.FAILED.value: "the run did not complete; error_code says why",
        RunStatus.RUNNING.value: "started but not finished, or interrupted before recovery ran",
        RunStatus.INTERRUPTED.value: "was running when the process ended; recovery recorded it as such",
    },
    "error_code": dict.fromkeys(REFUSAL_ERROR_CODES, "a source refused the request; no retry can change the answer"),
    "checkpoint.outcome": dict.fromkeys(
        REFUSAL_OUTCOMES, "a source refused the request; no retry can change the answer"
    ),
    "checkpoint.reason": {
        SourceLimitReason.KBO_ROBOTS_BLOCKED.value: (
            "koreabaseball.com's robots.txt disallows this path for generic agents"
        )
    },
}


def explain(column: str, value: object) -> str | None:
    """Return what a ledger column value means, when this module knows it.

    Args:
        column: The column name, or the dotted path within `checkpoint`.
        value: The value found in it.

    Returns:
        A sentence, or None when the value is not one this module names.

    """
    return EXPLANATIONS.get(column, {}).get("" if value is None else str(value))


def run_facts(row: object) -> dict[str, object]:
    """Summarise one ledger row in the terms this module names.

    Args:
        row: A mapping-like row or object carrying `status`, `error_code`,
            `checkpoint`, `records_read` and `records_written`.

    Returns:
        The collected facts, ready to render or assert on.

    """

    def field(name: str) -> object:
        getter = getattr(row, "get", None)
        return getter(name) if callable(getter) else getattr(row, name, None)

    status = field("status")
    error_code = field("error_code")
    checkpoint = field("checkpoint")
    collected = bool((field("records_read") or 0) or (field("records_written") or 0))
    return {
        "status": str(status) if status is not None else None,
        "refused": is_refusal(error_code=error_code, checkpoint=checkpoint),
        "source_limited": is_source_limited(checkpoint),
        "collected": collected,
        "terminal": str(status) in {s.value for s in RunStatus if s.terminal},
    }


def vocabulary() -> dict[str, dict[str, str]]:
    """Return everything this module names, for display and for tests.

    Returns:
        Column name to value to description. Values this module does not name are
        absent rather than described as unknown.

    """
    return {column: dict(values) for column, values in EXPLANATIONS.items()}


def latest(window_hours: int = 72, *, session_factory: object | None = None) -> dict[str, object]:
    """Return what the ledger says about recent runs, in named terms.

    Args:
        window_hours: How far back to look.
        session_factory: Opens a session. The default engine when omitted.

    Returns:
        Counts per outcome, and the latest run per crawler.

    """
    from datetime import UTC, timedelta
    from datetime import datetime as _datetime

    from sqlalchemy import text

    from src.db.engine import SessionLocal

    factory = session_factory or SessionLocal
    since = _datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=window_hours)
    with factory() as session:  # type: ignore[operator]
        rows = session.execute(
            text(
                "SELECT crawler, status, error_code, checkpoint, records_read, records_written, started_at "
                "FROM crawl_execution_runs WHERE started_at > :since ORDER BY started_at DESC"
            ),
            {"since": since},
        ).all()

    totals = {"runs": 0, "succeeded": 0, "collected": 0, "refused": 0, "failed": 0}
    latest_at: dict[str, datetime] = {}
    per_crawler: dict[str, dict[str, int]] = {}
    for crawler, status, error_code, checkpoint, records_read, records_written, started_at in rows:
        facts = run_facts(
            {
                "status": status,
                "error_code": error_code,
                "checkpoint": checkpoint,
                "records_read": records_read,
                "records_written": records_written,
            }
        )
        counts = per_crawler.setdefault(str(crawler), {"runs": 0, "succeeded": 0, "collected": 0, "refused": 0})
        counts["runs"] += 1
        totals["runs"] += 1
        if facts["status"] == RunStatus.SUCCESS.value:
            counts["succeeded"] += 1
            totals["succeeded"] += 1
        if facts["collected"]:
            counts["collected"] += 1
            totals["collected"] += 1
        if facts["refused"]:
            counts["refused"] += 1
            totals["refused"] += 1
        if facts["status"] == RunStatus.FAILED.value:
            totals["failed"] += 1
        seen = latest_at.get(str(crawler))
        if started_at is not None and (seen is None or started_at > seen):
            latest_at[str(crawler)] = started_at
    return {"window_hours": window_hours, "totals": totals, "per_crawler": per_crawler, "latest_at": latest_at}


__all__ = [
    "EXPLANATIONS",
    "REFUSAL_ERROR_CODES",
    "REFUSAL_OUTCOMES",
    "CheckpointOutcome",
    "RunStatus",
    "SourceLimitReason",
    "explain",
    "is_refusal",
    "is_source_limited",
    "latest",
    "parse_checkpoint",
    "run_facts",
    "vocabulary",
]
