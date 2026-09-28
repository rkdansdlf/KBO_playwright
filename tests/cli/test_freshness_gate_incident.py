"""The freshness-gate incident contract.

A failed gate evaluation opens ``freshness:gate`` and a passing one resolves it,
so a gate that recovers emits exactly one RECOVERED notice instead of leaving a
permanently open incident. Transport-level behaviour is covered elsewhere; this
module pins the incident lifecycle the gate now drives.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.reports.freshness_gate import (
    FRESHNESS_GATE_INCIDENT_KEY,
    _apply_freshness_incident,
    main,
)

#: The compatibility shim aliases ``src.cli.reports.freshness_gate`` into
#: ``src.cli.freshness_gate``, so patching either path reaches the same module.
PATCH_ROOT = "src.cli.freshness_gate"


class TestApplyFreshnessIncident:
    def test_failures_open_the_gate_incident(self) -> None:
        with patch(f"{PATCH_ROOT}.apply_incidents") as apply:
            _apply_freshness_incident(["RELAY: 3 games missing"], ok=False)

        events = apply.call_args.args[0]
        assert len(events) == 1
        assert events[0].incident_key == FRESHNESS_GATE_INCIDENT_KEY
        assert "RELAY: 3 games missing" in events[0].message
        assert apply.call_args.kwargs.get("resolve_keys", []) == []

    def test_passing_evaluation_resolves_the_incident(self) -> None:
        with patch(f"{PATCH_ROOT}.apply_incidents") as apply:
            _apply_freshness_incident([], ok=True)

        assert apply.call_args.args[0] == []
        assert apply.call_args.kwargs["resolve_keys"] == [FRESHNESS_GATE_INCIDENT_KEY]


class TestMainIncidentWiring:
    def test_alert_on_failure_opens_the_incident(self) -> None:
        session_local, session = (MagicMock(), MagicMock())
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []

        with (
            patch(f"{PATCH_ROOT}.SessionLocal", session_local),
            patch(f"{PATCH_ROOT}.collect_freshness_issues", return_value={}),
            patch(f"{PATCH_ROOT}.evaluate_freshness_gate", return_value=["RELAY: missing"]),
            patch(f"{PATCH_ROOT}._log_sla_metrics"),
            patch(f"{PATCH_ROOT}.apply_incidents") as apply,
        ):
            session_local.return_value.__enter__.return_value = session
            code = main(["--alert"])

        assert code == 1
        assert [e.incident_key for e in apply.call_args.args[0]] == [FRESHNESS_GATE_INCIDENT_KEY]

    def test_alert_on_pass_resolves_the_incident(self) -> None:
        session_local, session = (MagicMock(), MagicMock())
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []

        with (
            patch(f"{PATCH_ROOT}.SessionLocal", session_local),
            patch(f"{PATCH_ROOT}.collect_freshness_issues", return_value={}),
            patch(f"{PATCH_ROOT}.evaluate_freshness_gate", return_value=[]),
            patch(f"{PATCH_ROOT}._log_sla_metrics"),
            patch(f"{PATCH_ROOT}.apply_incidents") as apply,
        ):
            session_local.return_value.__enter__.return_value = session
            code = main(["--alert"])

        assert code == 0
        assert apply.call_args.args[0] == []
        assert apply.call_args.kwargs["resolve_keys"] == [FRESHNESS_GATE_INCIDENT_KEY]

    def test_without_alert_the_incident_is_untouched(self) -> None:
        session_local, session = (MagicMock(), MagicMock())
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []

        with (
            patch(f"{PATCH_ROOT}.SessionLocal", session_local),
            patch(f"{PATCH_ROOT}.collect_freshness_issues", return_value={}),
            patch(f"{PATCH_ROOT}.evaluate_freshness_gate", return_value=[]),
            patch(f"{PATCH_ROOT}._log_sla_metrics"),
            patch(f"{PATCH_ROOT}.apply_incidents") as apply,
        ):
            session_local.return_value.__enter__.return_value = session
            main([])

        apply.assert_not_called()
