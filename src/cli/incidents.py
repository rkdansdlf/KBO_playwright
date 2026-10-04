"""Read-only inspection of the notification incident ledger and delivery audit.

Operations previously reached both tables through hand-written SQL, which is why
``Docs/runbooks/NOTIFICATIONS.md`` documented table names rather than commands.
This module supplies the safe half of the missing operator surface: ``list``,
``show``, ``deliveries list`` and ``deliveries stats`` never mutate, so they
need no guard.

Deliberately not routed through ``IncidentManager`` for reads. The manager
answers "what is active", which cannot express ``--state recovered`` or a
severity filter, and a read-only CLI should not have to open a write-capable
service to render a table.

The delivery audit is append-only and outlives the incident it belongs to
(``notification_id`` is a soft reference, and the two retention windows differ),
so it gets its own query surface instead of only the per-incident tail that
``show`` renders.

Mutations live in :mod:`src.cli.incidents_operator`.

Exit codes: 0 ok, 1 not found (``show``).
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING

from sqlalchemy import desc, func, select

from src.cli.common import non_negative_int
from src.db.engine import get_db_session
from src.models.notification_delivery import DELIVERY_STATUSES, NotificationDelivery
from src.models.notification_incident import (
    ACTIVE_INCIDENT_STATES,
    INCIDENT_STATE_ACKNOWLEDGED,
    INCIDENT_STATE_OPEN,
    INCIDENT_STATE_RECOVERED,
    NotificationIncident,
)
from src.notifications.alert_dto import utcnow
from src.notifications.dto import NotificationChannel

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import datetime

    from sqlalchemy.orm import Session

EXIT_OK = 0
EXIT_NOT_FOUND = 1

#: ``--state`` values that are not stored states but are the common questions.
STATE_ACTIVE = "active"
STATE_ALL = "all"
_STATE_CHOICES = (STATE_ACTIVE, INCIDENT_STATE_OPEN, INCIDENT_STATE_ACKNOWLEDGED, INCIDENT_STATE_RECOVERED, STATE_ALL)

DEFAULT_LIMIT = 50
#: Cap on deliveries rendered by ``show``; the ledger is append-only and unbounded.
DEFAULT_DELIVERY_LIMIT = 10
#: Outcome-mix window. The audit is retained for months, so an all-time mix says
#: more about history than about the current transport. ``--days 0`` opts out.
DEFAULT_AUDIT_DAYS = 7

#: ``DELIVERY_STATUSES`` is a frozenset; sorted so ``--help`` output is stable.
_STATUS_CHOICES = sorted(DELIVERY_STATUSES)
_CHANNEL_CHOICES = [channel.value for channel in NotificationChannel]

_TABLE_SEP = "  "

_COLUMNS = "STATE", "SEVERITY", "OCC", "NOTIF", "LAST_SEEN", "SOURCE", "COMPONENT", "KEY"
_INCIDENT_CELL_KEYS = (
    "state",
    "severity",
    "occurrence_count",
    "notification_count",
    "last_seen_at",
    "source",
    "component",
    "incident_key",
)

_DELIVERY_COLUMNS = "DISPATCHED", "CHANNEL", "STATUS", "ATT", "LAT_MS", "ERROR_CODE", "BATCH_ID"
_DELIVERY_CELL_KEYS = (
    "dispatched_at",
    "channel",
    "status",
    "attempt_count",
    "latency_ms",
    "error_code",
    "batch_id",
)


def _write(text: str) -> None:
    sys.stdout.write(text + "\n")


def _error(text: str) -> None:
    sys.stderr.write(text + "\n")


def _stamp(value: datetime | None) -> str:
    return value.isoformat(sep=" ", timespec="seconds") if value is not None else "-"


def _incident_row(incident: NotificationIncident) -> dict[str, object]:
    """Flatten one incident into the JSON/table shape."""
    return {
        "incident_key": incident.incident_key,
        "state": incident.state,
        "severity": incident.severity,
        "source": incident.source,
        "component": incident.component,
        "title": incident.title,
        "occurrence_count": incident.occurrence_count,
        "notification_count": incident.notification_count,
        "first_opened_at": incident.first_opened_at.isoformat(sep=" ", timespec="seconds"),
        "last_seen_at": incident.last_seen_at.isoformat(sep=" ", timespec="seconds"),
        "last_notified_at": _stamp(incident.last_notified_at),
        "resolved_at": _stamp(incident.resolved_at),
    }


def _delivery_row(row: NotificationDelivery) -> dict[str, object]:
    """Flatten one delivery audit row into the JSON/table shape."""
    return {
        "incident_id": row.incident_id,
        "channel": row.channel,
        "destination": row.destination,
        "status": row.status,
        "attempt_count": row.attempt_count,
        "dispatched_at": _stamp(row.dispatched_at),
        "latency_ms": row.latency_ms,
        "error_code": row.error_code,
        "error_message": row.error_message,
        "batch_id": row.batch_id,
    }


@dataclass(frozen=True)
class DeliveryFilter:
    """Narrowing conditions for a delivery audit query.

    Bundled into one object rather than passed as loose keyword arguments so the
    filter set stays a single value the query, the stats projection and the
    argparse wiring all agree on.
    """

    channel: str | None = None
    status: str | None = None
    error_code: str | None = None
    incident_id: int | None = None
    batch_id: str | None = None
    days: int = DEFAULT_AUDIT_DAYS

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> DeliveryFilter:
        """Build a filter from parsed ``deliveries`` arguments."""
        return cls(
            channel=args.channel,
            status=args.status,
            error_code=args.error_code,
            incident_id=args.incident_id,
            batch_id=args.batch_id,
            days=args.days,
        )

    def since(self, now: datetime) -> datetime | None:
        """Return the window start, or ``None`` when the filter is unbounded."""
        return now - timedelta(days=self.days) if self.days > 0 else None


@dataclass(frozen=True)
class DeliveryAuditStats:
    """Snapshot of delivery outcomes inside one time window.

    A projection of the current rows, not a running counter, so it is recomputed
    per invocation rather than carried in Prometheus.
    """

    days: int = DEFAULT_AUDIT_DAYS
    total: int = 0
    by_status: dict[str, int] = field(default_factory=dict)
    by_channel: dict[str, int] = field(default_factory=dict)
    by_channel_status: dict[tuple[str, str], int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        """Return the stats as a JSON-serializable mapping."""
        return {
            "days": self.days,
            "total": self.total,
            "by_status": dict(sorted(self.by_status.items())),
            "by_channel": dict(sorted(self.by_channel.items())),
            "by_channel_status": {
                f"{channel}:{status}": count for (channel, status), count in sorted(self.by_channel_status.items())
            },
        }


def _apply_filters(
    stmt: object,
    *,
    state: str | None,
    source: str | None,
    severity: str | None,
) -> object:
    """Narrow the incident query by state/source/severity.

    ``None`` for ``state`` means "no filter", which is why the caller maps the
    ``all`` choice to it rather than comparing against a literal.
    """
    if state is not None:
        if state == STATE_ACTIVE:
            stmt = stmt.where(NotificationIncident.state.in_(tuple(ACTIVE_INCIDENT_STATES)))
        else:
            stmt = stmt.where(NotificationIncident.state == state)
    if source is not None:
        stmt = stmt.where(NotificationIncident.source == source)
    if severity is not None:
        stmt = stmt.where(NotificationIncident.severity == severity)
    return stmt


def _apply_delivery_filters(stmt: object, flt: DeliveryFilter, now: datetime) -> object:
    """Narrow a delivery audit query by channel/status/error/owner and window.

    The time window is part of the filter rather than a separate argument: the
    audit is append-only and retained far longer than an incident, so every
    delivery answer needs an explicit "over what period" to mean anything.
    """
    if flt.channel is not None:
        stmt = stmt.where(NotificationDelivery.channel == flt.channel)
    if flt.status is not None:
        stmt = stmt.where(NotificationDelivery.status == flt.status)
    if flt.error_code is not None:
        stmt = stmt.where(NotificationDelivery.error_code == flt.error_code)
    if flt.incident_id is not None:
        stmt = stmt.where(NotificationDelivery.incident_id == flt.incident_id)
    if flt.batch_id is not None:
        stmt = stmt.where(NotificationDelivery.batch_id == flt.batch_id)
    since = flt.since(now)
    if since is not None:
        stmt = stmt.where(NotificationDelivery.dispatched_at >= since)
    return stmt


def _query_incidents(
    session: Session,
    *,
    state: str | None,
    source: str | None,
    severity: str | None,
    limit: int,
) -> list[NotificationIncident]:
    """Return incidents most-recently-seen first."""
    stmt = select(NotificationIncident)
    stmt = _apply_filters(stmt, state=state, source=source, severity=severity)
    stmt = stmt.order_by(desc(NotificationIncident.last_seen_at)).limit(limit)
    return list(session.execute(stmt).scalars().all())


def _query_deliveries(session: Session, flt: DeliveryFilter, limit: int, now: datetime) -> list[NotificationDelivery]:
    """Return the most recent delivery attempts matching ``flt``."""
    stmt = select(NotificationDelivery)
    stmt = _apply_delivery_filters(stmt, flt, now)
    stmt = stmt.order_by(desc(NotificationDelivery.dispatched_at)).limit(limit)
    return list(session.execute(stmt).scalars().all())


def collect_delivery_audit_stats(session: Session, flt: DeliveryFilter, now: datetime) -> DeliveryAuditStats:
    """Group delivery outcomes by channel and status inside the filter window."""
    stmt = _apply_delivery_filters(
        select(NotificationDelivery.channel, NotificationDelivery.status, func.count()),
        flt,
        now,
    )
    stmt = stmt.group_by(NotificationDelivery.channel, NotificationDelivery.status)
    rows = session.execute(stmt).all()

    by_channel_status = {(str(channel), str(status)): int(count) for channel, status, count in rows}
    by_status: dict[str, int] = {}
    by_channel: dict[str, int] = {}
    for (channel, status), count in by_channel_status.items():
        by_status[status] = by_status.get(status, 0) + count
        by_channel[channel] = by_channel.get(channel, 0) + count
    return DeliveryAuditStats(
        days=flt.days,
        total=sum(by_status.values()),
        by_status=by_status,
        by_channel=by_channel,
        by_channel_status=by_channel_status,
    )


def _render_table(
    rows: list[dict[str, object]],
    columns: tuple[str, ...],
    extract: Callable[[dict[str, object]], list[str]],
) -> None:
    """Render rows as a fixed-width table sized to its own content."""
    body = [extract(row) for row in rows]
    if body:
        widths = [max(len(col), *(len(cells[i]) for cells in body)) for i, col in enumerate(columns)]
    else:
        widths = [len(col) for col in columns]
    _write(_TABLE_SEP.join(col.ljust(width) for col, width in zip(columns, widths, strict=True)))
    for cells in body:
        _write(_TABLE_SEP.join(cell.ljust(width) for cell, width in zip(cells, widths, strict=True)))


def _incident_cells(row: dict[str, object]) -> list[str]:
    """Project an incident payload onto the rendered column order."""
    return [str(row[key]) for key in _INCIDENT_CELL_KEYS]


def _delivery_cells(row: dict[str, object]) -> list[str]:
    """Project a delivery payload onto the rendered column order."""
    return [str(row[key]) for key in _DELIVERY_CELL_KEYS]


def _render_deliveries(deliveries: list[NotificationDelivery]) -> None:
    """Render the delivery audit tail for one incident."""
    if not deliveries:
        _write("  (no delivery rows)")
        return
    for row in deliveries:
        detail = f" error={row.error_code}" if row.error_code else ""
        _write(
            f"  {_stamp(row.dispatched_at)}  {row.channel:<8} {row.status:<20}"
            f" attempts={row.attempt_count} latency_ms={row.latency_ms}{detail}",
        )


def _cmd_list(args: argparse.Namespace) -> int:
    """List incidents, optionally narrowed by state/source/severity."""
    state = None if args.state == STATE_ALL else args.state
    with get_db_session() as session:
        incidents = _query_incidents(
            session,
            state=state,
            source=args.source,
            severity=args.severity,
            limit=args.limit,
        )
        rows = [_incident_row(incident) for incident in incidents]

    if args.json:
        _write(json.dumps({"count": len(rows), "incidents": rows}, ensure_ascii=False, default=str))
        return EXIT_OK
    if not rows:
        _write("(no incidents match)")
        return EXIT_OK
    _render_table(rows, _COLUMNS, _incident_cells)
    return EXIT_OK


def _cmd_show(args: argparse.Namespace) -> int:
    """Show one incident with the tail of its delivery audit."""
    with get_db_session() as session:
        stmt = select(NotificationIncident).where(NotificationIncident.incident_key == args.incident_key)
        incident = session.execute(stmt).scalar_one_or_none()
        if incident is None:
            _error(f"incident not found: {args.incident_key}")
            return EXIT_NOT_FOUND
        # ``show`` is explicitly one incident's own history, so the window is off.
        tail = DeliveryFilter(incident_id=incident.id, days=0)
        deliveries = _query_deliveries(session, tail, args.deliveries, utcnow())
        payload = _incident_row(incident)
        payload["metadata"] = incident.metadata_json
        delivery_rows = [_delivery_row(row) for row in deliveries]

    if args.json:
        _write(json.dumps({"incident": payload, "deliveries": delivery_rows}, ensure_ascii=False, default=str))
        return EXIT_OK
    for key, value in payload.items():
        _write(f"{key}: {value}")
    _write("deliveries:")
    _render_deliveries(deliveries)
    return EXIT_OK


def _cmd_deliveries_list(args: argparse.Namespace) -> int:
    """List delivery audit rows, optionally narrowed by channel/status/error/owner."""
    flt = DeliveryFilter.from_args(args)
    with get_db_session() as session:
        rows = [_delivery_row(row) for row in _query_deliveries(session, flt, args.limit, utcnow())]

    if args.json:
        _write(json.dumps({"count": len(rows), "deliveries": rows}, ensure_ascii=False, default=str))
        return EXIT_OK
    if not rows:
        _write("(no delivery rows)")
        return EXIT_OK
    _render_table(rows, _DELIVERY_COLUMNS, _delivery_cells)
    return EXIT_OK


def _cmd_deliveries_stats(args: argparse.Namespace) -> int:
    """Show the delivery outcome mix for one channel inside a time window."""
    flt = DeliveryFilter.from_args(args)
    with get_db_session() as session:
        stats = collect_delivery_audit_stats(session, flt, utcnow())

    if args.json:
        _write(json.dumps(stats.to_dict(), ensure_ascii=False, indent=2))
        return EXIT_OK
    window = "all time" if flt.days == 0 else f"last {flt.days}d"
    _write(f"Delivery audit ({window})")
    _write(f"  total  {stats.total}")
    if stats.by_status:
        _write("  by status")
        for status, count in sorted(stats.by_status.items()):
            _write(f"    {status:<22} {count}")
    if stats.by_channel:
        _write("  by channel")
        for channel, count in sorted(stats.by_channel.items()):
            _write(f"    {channel:<22} {count}")
    return EXIT_OK


def _add_delivery_filters(parser: argparse.ArgumentParser) -> None:
    """Add the delivery audit filters shared by ``list`` and ``stats``."""
    parser.add_argument("--channel", choices=_CHANNEL_CHOICES, default=None, help="Filter by channel.")
    parser.add_argument("--status", choices=_STATUS_CHOICES, default=None, help="Filter by delivery status.")
    parser.add_argument("--error-code", dest="error_code", default=None, help="Filter by error code.")
    parser.add_argument("--incident-id", dest="incident_id", type=int, default=None, help="Filter by incident id.")
    parser.add_argument("--batch-id", dest="batch_id", default=None, help="Filter by fan-out batch id.")
    parser.add_argument(
        "--days",
        type=non_negative_int,
        default=DEFAULT_AUDIT_DAYS,
        help="Only rows dispatched within N days; 0 means unbounded.",
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the read-only incident parser."""
    parser = argparse.ArgumentParser(
        prog="kbo incidents",
        description="Inspect notification incidents and their delivery audit (read-only).",
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    list_parser = subparsers.add_parser("list", help="List incidents, most recently seen first.")
    list_parser.add_argument("--state", choices=_STATE_CHOICES, default=STATE_ACTIVE)
    list_parser.add_argument("--source", default=None, help="Filter by alert source.")
    list_parser.add_argument("--severity", default=None, help="Filter by severity.")
    list_parser.add_argument("--limit", type=non_negative_int, default=DEFAULT_LIMIT)
    list_parser.add_argument("--json", action="store_true", help="Emit JSON.")

    show_parser = subparsers.add_parser("show", help="Show one incident and its recent deliveries.")
    show_parser.add_argument("incident_key")
    show_parser.add_argument("--deliveries", type=non_negative_int, default=DEFAULT_DELIVERY_LIMIT)
    show_parser.add_argument("--json", action="store_true", help="Emit JSON.")

    deliveries_parser = subparsers.add_parser("deliveries", help="Inspect the append-only delivery audit.")
    delivery_subs = deliveries_parser.add_subparsers(dest="deliveries_command", required=True)

    deliveries_list = delivery_subs.add_parser("list", help="List delivery attempts, most recent first.")
    _add_delivery_filters(deliveries_list)
    deliveries_list.add_argument("--limit", type=non_negative_int, default=DEFAULT_LIMIT)
    deliveries_list.add_argument("--json", action="store_true", help="Emit JSON.")

    deliveries_stats = delivery_subs.add_parser("stats", help="Show the delivery outcome mix.")
    _add_delivery_filters(deliveries_stats)
    deliveries_stats.add_argument("--json", action="store_true", help="Emit JSON.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the read-only incident CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.subcommand == "show":
        return _cmd_show(args)
    if args.subcommand == "deliveries":
        if args.deliveries_command == "stats":
            return _cmd_deliveries_stats(args)
        return _cmd_deliveries_list(args)
    return _cmd_list(args)


if __name__ == "__main__":
    raise SystemExit(main())
