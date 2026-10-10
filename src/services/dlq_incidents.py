"""Open and recover DLQ incidents from queue *state*, not from a job's outcome.

The dead letter recovery job reports through ``alert_warning``, which keys on the
job name. That conflates two unrelated questions under one incident key:

* *Did the check itself work?* -- the job ran, reached the queue, and completed.
* *Is the queue in a bad state?* -- letters are stranded, exhausted, or ageing.

An operator reading ``scheduler:crawl_dead_letter_recovery:warning`` cannot tell
them apart, and they need different responses. A check that failed wants the
scheduler looked at; a queue full of stranded letters wants the runbook. And
because the key is the job name, one noisy run of the check overwrites whatever
the queue state was trying to say -- the state signal is lost even when it was
the more urgent of the two.

So the two are separated by key namespace. ``scheduler:*`` stays the coarse
"this check misbehaved" signal the scheduler layer already owns, and ``dlq:*``
here is the durable statement about the queue itself, independent of who looked.

The other half of the problem is that a healthy queue cannot be recorded.
``apply_incidents`` with no events and no resolve keys returns immediately, and
"the sweep ran and found nothing" produces exactly that -- so the absence of a
``dlq:*`` incident was indistinguishable from nobody having looked. That is what
the sweep heartbeat metrics (``kbo_crawl_dlq_last_successful_sweep_timestamp``)
exist for, and it is why this module resolves explicitly: a passing evaluation
names the keys it cleared rather than staying silent.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from src.notifications.alert_dto import AlertEvent, AlertSeverity, AlertSource
from src.notifications.bridge import apply_incidents

if TYPE_CHECKING:
    from collections.abc import Sequence

    from src.services.crawl_dead_letter_stats import DlqStats

logger = logging.getLogger(__name__)

#: Incident keys this module owns. Named so the scheduler layer can resolve them
#: explicitly and so a contract test can assert the namespace is not shared with
#: the coarse scheduler incidents.
DLQ_INCIDENT_STRANDED = "dlq:stranded_retrying"
DLQ_INCIDENT_BACKLOG = "dlq:backlog_age"
DLQ_INCIDENT_EXHAUSTED = "dlq:exhausted"

#: Every key this module can open. Used for reconciliation, so a condition that
#: recovers resolves even when it was opened by an earlier evaluation whose key is
#: no longer produced -- which is exactly what happens when a threshold is
#: retuned.
DLQ_INCIDENT_KEYS: tuple[str, ...] = (
    DLQ_INCIDENT_STRANDED,
    DLQ_INCIDENT_BACKLOG,
    DLQ_INCIDENT_EXHAUSTED,
)

#: A letter is worth retrying for days, so a backlog a day old is normal drain.
#: Mirrors ``KboDlqBacklogAgeHigh`` in the Prometheus rules so the alert a human
#: receives and the incident they read say the same number.
BACKLOG_AGE_WARNING_SECONDS = 86400

#: How many letters to name in a message before summarising.
DLQ_INCIDENT_DETAIL_LIMIT = 5


def stranded_letters(stats: DlqStats) -> int:
    """Return how many letters are stuck in `retrying` past the staleness cutoff."""
    return max(0, stats.stale_retrying)


def _stranded_event(stats: DlqStats, threshold: int) -> AlertEvent:
    """Build the incident for letters stranded in replay."""
    return AlertEvent(
        source=AlertSource.RECOVERY,
        component="crawl_dlq",
        severity=AlertSeverity.ERROR,
        title="DLQ 복구 정체: replay에 묶인 dead letter",
        message=(
            f"{stats.stale_retrying}건의 dead letter가 retrying 상태에서 "
            f"{threshold}초 이상 머물렀습니다. replay 중 프로세스가 종료된 것으로 보이며, "
            f"`crawl_dead_letter_recovery` 잡이 이를 회수하지 못한 상태입니다. "
            f"`python3 -m src.cli.kbo dlq list --status retrying` 로 확인하세요."
        ),
        incident_key=DLQ_INCIDENT_STRANDED,
        remediation=(
            "python3 -m src.cli.kbo dlq list --status retrying",
            "Docs/runbooks/DATA_RELIABILITY.md",
        ),
        metadata={
            "stranded": stats.stale_retrying,
            "stale_threshold_seconds": threshold,
        },
    )


def _backlog_event(stats: DlqStats) -> AlertEvent:
    """Build the incident for a queue whose oldest letter is not draining."""
    return AlertEvent(
        source=AlertSource.RECOVERY,
        component="crawl_dlq",
        severity=AlertSeverity.WARNING,
        title="DLQ 회수 대기 열화",
        message=(
            f"가장 오래된 대기 중 dead letter가 {int(stats.oldest_due_age_seconds)}초"
            f"({BACKLOG_AGE_WARNING_SECONDS}초 기준) 동안 회수되지 않았습니다. "
            f"대기 {stats.due}건, retrying {stats.retrying}건. "
            f"`python3 -m src.cli.kbo dlq stats` 로 확인하세요."
        ),
        incident_key=DLQ_INCIDENT_BACKLOG,
        remediation=(
            "python3 -m src.cli.kbo dlq stats",
            "Docs/runbooks/DATA_RELIABILITY.md",
        ),
        metadata={
            "due": stats.due,
            "retrying": stats.retrying,
            "oldest_due_age_seconds": int(stats.oldest_due_age_seconds),
        },
    )


def _exhausted_event(stats: DlqStats) -> AlertEvent:
    """Build the incident for letters that ran out of retries.

    Distinct from the two above because it is not transient. A stranded letter
    recovers on its own and an ageing backlog drains; an exhausted letter has
    stopped by decision -- the retry budget is spent and only an operator can
    decide whether to requeue it or ignore it. Reporting it as a warning that
    clears itself would be the same conflation this module exists to remove.
    """
    return AlertEvent(
        source=AlertSource.RECOVERY,
        component="crawl_dlq",
        severity=AlertSeverity.ERROR,
        title="DLQ 재시도 소진: dead letter가 자동 처리가능 범위를 벗어남",
        message=(
            f"{stats.exhausted}건의 dead letter가 재시도 횟수를 모두 소진해 "
            f"exhausted 상태입니다. 자동 회수로 복구될 수 없으며, 운영자가 "
            f"재시도(requeue) 또는 무시(ignore)를 결정해야 합니다. "
            f"`python3 -m src.cli.kbo dlq list --status exhausted` 로 확인하세요."
        ),
        incident_key=DLQ_INCIDENT_EXHAUSTED,
        # Not `retry` first: a letter that spent its budget already answered the
        # question five times, and the usual cause in production is a policy
        # block that no retry can clear. The runbook's decision table is the
        # entry point; `requeue` stays available for a cause that was fixed.
        remediation=(
            "python3 -m src.cli.kbo dlq list --status exhausted",
            "python3 -m src.cli.kbo dlq ignore <dlq_id> --reason <why> --apply",
            "Docs/runbooks/DATA_RELIABILITY.md",
        ),
        metadata={"exhausted": stats.exhausted},
    )


def dlq_incident_events(
    stats: DlqStats,
    *,
    stale_retry_threshold: int,
    backlog_age_threshold: int = BACKLOG_AGE_WARNING_SECONDS,
) -> list[AlertEvent]:
    """Return the DLQ incidents implied by one queue reading.

    Args:
        stats: A consistent snapshot of the queue.
        stale_retry_threshold: The `DLQ_STALE_RETRYING_SECONDS` value the reading
            was taken with, reported in the message so an operator can tell a
            1800s cutoff from a raised one.
        backlog_age_threshold: Age at which the backlog becomes an incident.

    Returns:
        One event per abnormal condition; empty when the queue is healthy.

    """
    events: list[AlertEvent] = []
    if stranded_letters(stats) > 0:
        events.append(_stranded_event(stats, stale_retry_threshold))
    if stats.oldest_due_age_seconds > backlog_age_threshold:
        events.append(_backlog_event(stats))
    if stats.exhausted > 0:
        events.append(_exhausted_event(stats))
    return events


def apply_dlq_incidents(
    stats: DlqStats,
    *,
    stale_retry_threshold: int,
    backlog_age_threshold: int = BACKLOG_AGE_WARNING_SECONDS,
    session_factory: object | None = None,
) -> list[AlertEvent]:
    """Publish the queue's state as incidents, recovering what has cleared.

    A healthy reading resolves the keys it did not open rather than doing
    nothing. Without that, a recovered queue left its incident open forever and
    the only way to close it was the operator command in the runbook -- which is
    the manual step this ledger exists to remove.

    Reconciles the whole ``dlq:`` namespace rather than listing resolved keys, so
    a key retired in a later version is still closed by a healthy reading.

    Args:
        stats: A consistent snapshot of the queue.
        stale_retry_threshold: The staleness cutoff the reading was taken with.
        backlog_age_threshold: Age at which the backlog becomes an incident.
        session_factory: Optional ledger session override, for tests.

    Returns:
        The events that were published.

    """
    events = dlq_incident_events(
        stats,
        stale_retry_threshold=stale_retry_threshold,
        backlog_age_threshold=backlog_age_threshold,
    )
    try:
        apply_incidents(
            events,
            reconcile_prefix="dlq:",
            session_factory=session_factory,
        )
    except Exception:
        # The incident ledger is a reporting side channel here. A queue reading
        # that succeeded must not be reported as a failed job because notifying
        # about it failed, and the next tick will re-derive the same state.
        logger.exception("Failed to apply DLQ state incidents")
    return events


def resolved_dlq_keys(events: Sequence[AlertEvent]) -> list[str]:
    """Return the `dlq:` keys a reading did not open, for assertions and logging."""
    opened = {event.incident_key for event in events}
    return [key for key in DLQ_INCIDENT_KEYS if key not in opened]


__all__ = [
    "BACKLOG_AGE_WARNING_SECONDS",
    "DLQ_INCIDENT_BACKLOG",
    "DLQ_INCIDENT_EXHAUSTED",
    "DLQ_INCIDENT_KEYS",
    "DLQ_INCIDENT_STRANDED",
    "apply_dlq_incidents",
    "dlq_incident_events",
    "resolved_dlq_keys",
    "stranded_letters",
]
