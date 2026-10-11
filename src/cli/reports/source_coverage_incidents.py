"""Surface a blocked source as an incident that stays open until it is decided.

A crawl a robots policy refuses is recorded as a run that collected nothing, and
a run that collected nothing is recorded as ``success`` -- a refusal is not an
error and there is nothing to retry. Both halves are correct. Together they are
invisible: the queue stays empty, the ledger stays green, and the table the
crawler was supposed to fill stops moving with nothing pointing at the reason.

The source inventory already measures that pair, per crawler. This turns the
measurement into an incident, because the only thing that closes one is a
decision -- a permitted route, or an explicit acceptance that the gap is the
price -- and neither arrives because the data stopped moving.

One incident rather than one per crawler: a policy change takes every crawler on
that host at the same moment, and twenty-four notifications in a minute is how an
alert channel stops being read. Which crawlers are involved travels in the
metadata, where ``crawler_source_inventory`` renders the detail.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.notifications.alert_dto import AlertEvent, AlertSeverity, AlertSource
from src.reporting.source_inventory import Freshness, PolicyStatus

if TYPE_CHECKING:
    from collections.abc import Sequence

    from src.reporting.source_inventory import SourceInventory, SourceRow

#: Semantic key. Stable across crawlers because the decision it waits on is one
#: decision, not twenty-four of them.
INCIDENT_KEY = "source:blocked"

_SEVERITY = AlertSeverity.WARNING


def _stopped_moving(row: SourceRow) -> bool:
    """Return whether this crawler has stopped delivering despite saying success."""
    if row.runs is not None and not row.runs.yields:
        return True
    return any(m.freshness is Freshness.STALE for m in row.measurements)


#: How many crawlers the message names before summarising the rest.
MAX_NAMED = 3


def _age_summary(row: SourceRow) -> str:
    """Return a compact "table: age" list for the message body."""
    parts = [
        f"{m.table} {m.age_days}d"
        for m in sorted(row.measurements, key=lambda m: m.age_days or 0, reverse=True)
        if m.freshness is Freshness.STALE
    ]
    return ", ".join(parts[:MAX_NAMED])


def build_source_coverage_incidents(inventory: SourceInventory) -> tuple[list[AlertEvent], list[str]]:
    """Return the events to publish and the keys that should close.

    Args:
        inventory: A built source inventory. ``src.reporting.source_inventory``
            owns the measurement; this reads it and decides what it means.

    Returns:
        ``(events, resolve_keys)`` for ``apply_incidents``. The key resolves when
        no blocked crawler is silent any more, which happens when the policy
        lifts or a second route starts filling the tables.

    """
    silent: list[SourceRow] = []
    blocked = 0
    for row in sorted(inventory.rows, key=lambda r: r.crawler):
        if row.policy_status is not PolicyStatus.BLOCKED:
            continue
        blocked += 1
        if _stopped_moving(row):
            silent.append(row)

    if not silent:
        return [], [INCIDENT_KEY] if blocked else []

    names = [row.crawler for row in silent]
    detail = "; ".join(f"{row.crawler} ({_age_summary(row)})" for row in silent[:MAX_NAMED])
    if len(silent) > MAX_NAMED:
        detail = f"{detail}; +{len(silent) - MAX_NAMED} more"

    event = AlertEvent(
        source=AlertSource.CRAWLER,
        component=INCIDENT_KEY,
        severity=_SEVERITY,
        title=f"수집 원천 차단: {len(silent)}개 크롤러가 데이터를 갱신하지 못함",
        message=(
            f"robots 정책으로 차단된 {len(silent)}/{blocked}개 크롤러가 성공을 보고하지만 "
            f"테이블이 갱신되지 않습니다: {detail}. "
            "허용된 경로를 열거나, 공백을 감수하기로 명시적으로 결정해야 이 인시던트가 닫힙니다."
        ),
        incident_key=INCIDENT_KEY,
        metadata={
            "crawlers": names,
            "blocked_total": blocked,
            "silent_total": len(silent),
            "stale_tables": {
                row.crawler: [m.table for m in row.measurements if m.freshness is Freshness.STALE] for row in silent
            },
        },
    )
    return [event], []


def incident_crawler_names(events: Sequence[AlertEvent]) -> tuple[str, ...]:
    """Return the crawler names carried by a coverage event.

    Args:
        events: Events returned by :func:`build_source_coverage_incidents`.

    Returns:
        The crawler names, or an empty tuple when the events are not ours.

    """
    for event in events:
        if event.incident_key == INCIDENT_KEY:
            return tuple(str(name) for name in event.metadata.get("crawlers", ()))
    return ()
