"""Guarded operator mutations for the notification incident ledger.

The ledger had no operator surface at all: acknowledging a stuck incident or
recovering a false positive meant hand-written SQL against
``notification_incidents``. This module adds that surface under the same
default-deny contract the crawl dead letter queue uses.

Two independent conditions must both hold before anything is written: an
explicit ``--apply`` **and** ``KBO_ALLOW_INCIDENT_MUTATION=1``. Without either,
the command still validates existence and state and prints what it would do, so
a dry-run tells an operator whether the action is even legal.

Every mutation goes through :class:`AlertPublisher`, never a direct model
update, because the publisher owns the parts that are easy to lose: recovery
notices are sent exactly once, and the state change stays inside the
incident-ledger's conditional-UPDATE concurrency guard.

Command flow: ``load -> validate -> preview -> guard -> mutate``.

Exit codes: 0 ok/preview, 1 not found, 2 invalid state, 3 guard denied.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.db.engine import get_db_session
from src.models.notification_incident import (
    ACTIVE_INCIDENT_STATES,
    INCIDENT_STATE_ACKNOWLEDGED,
    INCIDENT_STATE_OPEN,
)
from src.notifications.bridge import alerts_dry_run
from src.notifications.publisher import AlertPublisher

if TYPE_CHECKING:
    from collections.abc import Sequence

    from src.models.notification_incident import NotificationIncident

EXIT_OK = 0
EXIT_NOT_FOUND = 1
EXIT_INVALID_STATE = 2
EXIT_GUARD_DENIED = 3

#: Environment variable that unlocks the mutations, mirroring
#: ``KBO_ALLOW_DLQ_MUTATION`` for the dead letter queue.
MUTATION_ENV_VAR = "KBO_ALLOW_INCIDENT_MUTATION"

#: Only an OPEN incident can be acknowledged; ACKNOWLEDGED is already acknowledged
#: and RECOVERED is terminal, so re-acknowledging either is a caller mistake.
ACKNOWLEDGEABLE_STATES = frozenset({INCIDENT_STATE_OPEN})


def _write(text: str) -> None:
    sys.stdout.write(text + "\n")


def _error(text: str) -> None:
    sys.stderr.write(text + "\n")


def mutation_enabled() -> bool:
    """Return whether the environment unlocks incident mutations."""
    return os.getenv(MUTATION_ENV_VAR) == "1"


def build_parser() -> argparse.ArgumentParser:
    """Build the guarded operator parser."""
    parser = argparse.ArgumentParser(
        prog="kbo incidents",
        description="Guarded notification incident operator actions.",
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)
    for name, help_text in (
        ("ack", "Acknowledge an open incident so it stops re-notifying."),
        ("resolve", "Recover an active incident and send the recovery notice once."),
    ):
        sub = subparsers.add_parser(name, help=help_text)
        sub.add_argument("incident_key", help="Incident key, for example drift:selector:schedule.")
        sub.add_argument("--reason", default=None, help="Operator note recorded in the output.")
        sub.add_argument("--apply", action="store_true", help="Apply the mutation.")
        sub.add_argument("--json", action="store_true", help="Emit JSON.")
    return parser


@dataclass(frozen=True)
class OperatorOutcome:
    """What one guarded action did, or would do.

    A value object rather than six parameters so the preview and applied paths
    build the same record and cannot drift in field order or naming.
    """

    action: str
    incident_key: str
    status: str
    applied: bool
    reason: str | None = None


def _render(outcome: OperatorOutcome, *, json_out: bool) -> int:
    """Print the shared operator envelope (text or JSON)."""
    if json_out:
        _write(
            json.dumps(
                {
                    "action": outcome.action,
                    "incident_key": outcome.incident_key,
                    "applied": outcome.applied,
                    "status": outcome.status,
                    "reason": outcome.reason,
                },
                ensure_ascii=False,
            ),
        )
        return EXIT_OK
    if outcome.applied:
        _write(f"{outcome.action} {outcome.incident_key}: {outcome.status}")
    else:
        _write(
            f"would {outcome.action} {outcome.incident_key} (status={outcome.status}; "
            f"pass --apply and set {MUTATION_ENV_VAR}=1)",
        )
    if outcome.reason is not None:
        _write(f"  reason: {outcome.reason}")
    return EXIT_OK


def _preview(action: str, incident_key: str, *, status: str, reason: str | None, json_out: bool) -> int:
    """Render the dry-run envelope for a validated but unapplied action."""
    return _render(
        OperatorOutcome(
            action=action,
            incident_key=incident_key,
            status=status,
            applied=False,
            reason=reason,
        ),
        json_out=json_out,
    )


def _load(incident_key: str) -> NotificationIncident | None:
    """Load the incident row for the key, or ``None`` when it does not exist."""
    from sqlalchemy import select

    from src.models.notification_incident import NotificationIncident

    with get_db_session() as session:
        stmt = select(NotificationIncident).where(NotificationIncident.incident_key == incident_key)
        incident = session.execute(stmt).scalar_one_or_none()
        if incident is None:
            return None
        # Detach a plain snapshot: the session closes before the mutation runs.
        return NotificationIncident(
            id=incident.id,
            incident_key=incident.incident_key,
            state=incident.state,
            severity=incident.severity,
            source=incident.source,
        )


def _ack(incident_key: str, *, apply: bool, reason: str | None, json_out: bool) -> int:
    """Acknowledge an open incident."""
    incident = _load(incident_key)
    if incident is None:
        _error(f"incident not found: {incident_key}")
        return EXIT_NOT_FOUND
    if incident.state not in ACKNOWLEDGEABLE_STATES:
        _error(
            f"cannot acknowledge {incident_key}: state={incident.state} "
            f"(only {sorted(ACKNOWLEDGEABLE_STATES)} is acknowledgeable)",
        )
        return EXIT_INVALID_STATE
    if not apply or not mutation_enabled():
        if apply and not mutation_enabled():
            _error(f"refusing mutation: --apply requires {MUTATION_ENV_VAR}=1")
            return EXIT_GUARD_DENIED
        return _preview(
            "ack",
            incident_key,
            status=f"{incident.state} -> {INCIDENT_STATE_ACKNOWLEDGED}",
            reason=reason,
            json_out=json_out,
        )

    with get_db_session() as session:
        publisher = AlertPublisher(session)
        transition = publisher.acknowledge(incident_key)
    if transition is None:
        _error(f"acknowledge had no effect on {incident_key}; it was resolved concurrently")
        return EXIT_INVALID_STATE
    return _render(
        OperatorOutcome(
            action="ack",
            incident_key=incident_key,
            status=f"{INCIDENT_STATE_ACKNOWLEDGED} (notifications suppressed until escalation)",
            applied=True,
            reason=reason,
        ),
        json_out=json_out,
    )


def _resolve(incident_key: str, *, apply: bool, reason: str | None, json_out: bool) -> int:
    """Recover an active incident, sending the recovery notice at most once."""
    incident = _load(incident_key)
    if incident is None:
        _error(f"incident not found: {incident_key}")
        return EXIT_NOT_FOUND
    if incident.state not in ACTIVE_INCIDENT_STATES:
        _error(f"cannot resolve {incident_key}: state={incident.state} (not active)")
        return EXIT_INVALID_STATE
    if not apply or not mutation_enabled():
        if apply and not mutation_enabled():
            _error(f"refusing mutation: --apply requires {MUTATION_ENV_VAR}=1")
            return EXIT_GUARD_DENIED
        return _preview(
            "resolve",
            incident_key,
            status=f"{incident.state} -> RECOVERED",
            reason=reason,
            json_out=json_out,
        )

    with get_db_session() as session:
        publisher = AlertPublisher(session)
        # Honour the global ALERT_DRY_RUN switch so an operator can resolve
        # during an active alerting storm without generating a recovery notice.
        result = publisher.resolve(incident_key, dry_run=alerts_dry_run())
    if result is None:
        _error(f"resolve had no effect on {incident_key}; it was resolved concurrently")
        return EXIT_INVALID_STATE
    notified = result.delivery_report is not None
    return _render(
        OperatorOutcome(
            action="resolve",
            incident_key=incident_key,
            status="RECOVERED" + (" (recovery notice sent)" if notified else " (no notice due)"),
            applied=True,
            reason=reason,
        ),
        json_out=json_out,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the guarded incident operator CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.subcommand == "resolve":
        return _resolve(args.incident_key, apply=args.apply, reason=args.reason, json_out=args.json)
    return _ack(args.incident_key, apply=args.apply, reason=args.reason, json_out=args.json)


if __name__ == "__main__":
    raise SystemExit(main())
