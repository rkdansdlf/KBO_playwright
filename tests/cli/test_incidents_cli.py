"""Tests for the read-only incident and delivery audit CLI.

These run against a real in-memory SQLite session rather than a mocked one: the
commands exist to answer "which incidents are active" and "which channel is
failing", and both answers are produced by the SQL filters themselves. A mocked
session would keep passing if a ``WHERE`` clause regressed.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.cli.incidents import _delivery_notice_state
from src.cli.incidents import main as incidents_main
from src.cli.kbo import main as kbo_main
from src.models.base import Base
from src.models.notification_delivery import (
    DELIVERY_STATUS_DRY_RUN,
    DELIVERY_STATUS_FAILED,
    DELIVERY_STATUS_SENT,
    DELIVERY_STATUS_SKIPPED,
    NotificationDelivery,
)
from src.models.notification_incident import (
    INCIDENT_STATE_ACKNOWLEDGED,
    INCIDENT_STATE_OPEN,
    INCIDENT_STATE_RECOVERED,
    NotificationIncident,
)
from src.notifications.alert_dto import utcnow

if TYPE_CHECKING:
    from collections.abc import Iterator
    from sqlalchemy.orm import Session

NOW = utcnow()
STALE = NOW - timedelta(days=30)

OPEN_KEY = "integrity:game_stats:20260925"
ACKED_KEY = "freshness:gate"
RECOVERED_KEY = "drift:snapshot"


@pytest.fixture
def session() -> Iterator[Session]:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    sess = sessionmaker(bind=engine, expire_on_commit=False)()
    try:
        yield sess
    finally:
        sess.close()


def _incident(
    key: str,
    state: str,
    severity: str,
    source: str,
    seen: object,
    *,
    occurrences: int = 3,
    notifications: int = 1,
) -> NotificationIncident:
    return NotificationIncident(
        incident_key=key,
        source=source,
        component=key.split(":", maxsplit=1)[0],
        severity=severity,
        state=state,
        title=f"{source} alert",
        message=f"{key} is failing",
        details_hash="h",
        occurrence_count=occurrences,
        notification_count=notifications,
        first_opened_at=seen,
        last_seen_at=seen,
        metadata_json={"probe": key},
    )


@pytest.fixture
def seeded(session: Session) -> Session:
    """Two active incidents, one recovered, and five deliveries across windows.

    The three incidents carry one of each notice state so the derived column is
    exercised by ordinary fixture data rather than only by its own test:
    ``notified`` on the open one, ``silent`` on the acknowledged one, ``partly``
    on the recovered one.
    """
    session.add_all(
        [
            _incident(
                OPEN_KEY,
                INCIDENT_STATE_OPEN,
                "ERROR",
                "integrity",
                NOW - timedelta(hours=1),
                occurrences=3,
                notifications=3,
            ),
            _incident(
                ACKED_KEY,
                INCIDENT_STATE_ACKNOWLEDGED,
                "WARNING",
                "freshness",
                NOW - timedelta(hours=2),
                occurrences=4,
                notifications=0,
            ),
            _incident(
                RECOVERED_KEY,
                INCIDENT_STATE_RECOVERED,
                "CRITICAL",
                "drift",
                NOW - timedelta(hours=3),
                occurrences=6,
                notifications=2,
            ),
        ],
    )
    session.flush()
    open_id = _incident_id(session, OPEN_KEY)
    acked_id = _incident_id(session, ACKED_KEY)

    session.add_all(
        [
            NotificationDelivery(
                incident_id=open_id,
                batch_id="b1",
                channel="telegram",
                destination="chat-1",
                status=DELIVERY_STATUS_SENT,
                attempt_count=1,
                dispatched_at=NOW - timedelta(hours=1),
                latency_ms=120,
            ),
            NotificationDelivery(
                incident_id=open_id,
                batch_id="b1",
                channel="slack",
                destination="slack-webhook",
                status=DELIVERY_STATUS_FAILED,
                attempt_count=3,
                dispatched_at=NOW - timedelta(minutes=90),
                latency_ms=900,
                error_code="HTTP_500",
                error_message="HTTP 500",
            ),
            NotificationDelivery(
                incident_id=acked_id,
                batch_id="b2",
                channel="telegram",
                destination="chat-2",
                status=DELIVERY_STATUS_SKIPPED,
                attempt_count=1,
                dispatched_at=NOW - timedelta(hours=2),
            ),
            NotificationDelivery(
                batch_id="b3",
                channel="console",
                status=DELIVERY_STATUS_DRY_RUN,
                attempt_count=1,
                dispatched_at=NOW - timedelta(hours=3),
            ),
            # Same incident as b1 but far outside the default window: proves the
            # window is a delivery-audit concern, not an incident-lifecycle one.
            NotificationDelivery(
                incident_id=open_id,
                batch_id="b4",
                channel="telegram",
                destination="chat-1",
                status=DELIVERY_STATUS_SENT,
                attempt_count=1,
                dispatched_at=STALE,
                latency_ms=110,
            ),
        ],
    )
    session.commit()
    return session


@pytest.fixture
def use_session(monkeypatch: pytest.MonkeyPatch, seeded: Session) -> Session:
    """Point the CLI at the seeded in-memory session."""
    # Imported lazily so the fixture can be defined before the module is patched.
    from src.cli import incidents as module

    @contextmanager
    def _fake_session() -> Iterator[Session]:
        yield seeded

    monkeypatch.setattr(module, "get_db_session", _fake_session)
    return seeded


def _payload(capsys: pytest.CaptureFixture[str]) -> dict:
    return json.loads(capsys.readouterr().out)


def _incident_id(session: Session, key: str) -> int:
    stmt = select(NotificationIncident.id).where(NotificationIncident.incident_key == key)
    return int(session.execute(stmt).scalar_one())


class TestIncidentList:
    def test_defaults_to_active_states_only(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["list", "--json"]) == 0
        keys = {row["incident_key"] for row in _payload(capsys)["incidents"]}
        assert keys == {OPEN_KEY, ACKED_KEY}

    def test_state_all_includes_recovered(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["list", "--state", "all", "--json"]) == 0
        assert _payload(capsys)["count"] == 3

    def test_state_recovered_filters(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["list", "--state", INCIDENT_STATE_RECOVERED, "--json"]) == 0
        assert [row["incident_key"] for row in _payload(capsys)["incidents"]] == [RECOVERED_KEY]

    def test_source_and_severity_filters(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["list", "--source", "integrity", "--severity", "ERROR", "--json"]) == 0
        payload = _payload(capsys)
        assert payload["count"] == 1
        assert payload["incidents"][0]["incident_key"] == OPEN_KEY

    def test_empty_result_message(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["list", "--source", "crawler"]) == 0
        assert capsys.readouterr().out.strip() == "(no incidents match)"

    def test_renders_column_header(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["list"]) == 0
        header = capsys.readouterr().out.splitlines()[0]
        assert header.split() == [
            "STATE",
            "SEVERITY",
            "OCC",
            "NOTIF",
            "NOTICE",
            "LAST_SEEN",
            "SOURCE",
            "COMPONENT",
            "KEY",
        ]

    def test_limit_is_honoured(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["list", "--limit", "1", "--json"]) == 0
        assert _payload(capsys)["count"] == 1


class TestNoticeState:
    """``notice_state`` is derived, because the ledger stores no decision column.

    ``AlertDecision.SUPPRESSED`` is computed per ``process`` call and discarded,
    so a suppressed notification leaves no row anywhere. What survives is the
    counter pair, and the gap between them is how many occurrences were never
    announced. These tests pin that derivation, including the case where the gap
    cannot be read as a suppression at all.
    """

    @staticmethod
    def _notice_of(session: Session, key: str) -> str:
        row = session.get(NotificationIncident, _incident_id(session, key))
        return str(_delivery_notice_state(row))

    @pytest.mark.parametrize(
        ("occurrences", "notifications", "expected"),
        [
            (3, 3, "notified"),
            (5, 5, "notified"),
            # More sends than occurrences must not read as "partly": a fan-out or
            # a replayed batch can push notification_count past occurrence_count.
            (2, 5, "notified"),
            (0, 0, "notified"),
            (5, 2, "partly"),
            (4, 0, "silent"),
            (1, 0, "silent"),
        ],
    )
    def test_the_gap_selects_the_state(
        self,
        session: Session,
        occurrences: int,
        notifications: int,
        expected: str,
    ) -> None:
        incident = _incident(
            "probe:key",
            INCIDENT_STATE_OPEN,
            "WARNING",
            "probe",
            NOW,
            occurrences=occurrences,
            notifications=notifications,
        )
        session.add(incident)
        session.commit()

        assert self._notice_of(session, "probe:key") == expected

    def test_fixture_covers_all_three_states(self, use_session: Session) -> None:
        assert self._notice_of(use_session, OPEN_KEY) == "notified"
        assert self._notice_of(use_session, ACKED_KEY) == "silent"
        assert self._notice_of(use_session, RECOVERED_KEY) == "partly"

    def test_the_column_is_in_the_json_payload(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["list", "--state", "all", "--json"]) == 0
        rows = {row["incident_key"]: row["notice_state"] for row in _payload(capsys)["incidents"]}
        assert rows == {OPEN_KEY: "notified", ACKED_KEY: "silent", RECOVERED_KEY: "partly"}

    def test_the_column_is_in_show_output(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["show", ACKED_KEY]) == 0
        assert "notice_state: silent" in capsys.readouterr().out


class TestSilentFilter:
    def test_selects_only_never_announced_incidents(
        self,
        use_session: Session,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        assert incidents_main(["list", "--notice", "silent", "--state", "all", "--json"]) == 0
        assert [row["incident_key"] for row in _payload(capsys)["incidents"]] == [ACKED_KEY]

    def test_combines_with_the_default_active_scope(
        self,
        use_session: Session,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """The default ``--state active`` still applies.

        The recovered incident has a gap too, so without the scope it would leak
        in beside the acknowledged one.
        """
        assert incidents_main(["list", "--notice", "silent", "--json"]) == 0
        assert _payload(capsys)["count"] == 1

    def test_returns_nothing_when_every_incident_notified(
        self,
        use_session: Session,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        assert incidents_main(["list", "--state", "all", "--json"]) == 0
        _payload(capsys)  # drain the previous read
        use_session.get(NotificationIncident, _incident_id(use_session, ACKED_KEY)).notification_count = 2
        use_session.commit()

        assert incidents_main(["list", "--notice", "silent", "--json"]) == 0
        assert _payload(capsys)["count"] == 0


class TestIncidentShow:
    def test_missing_key_returns_not_found(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["show", "nope"]) == 1
        assert "incident not found: nope" in capsys.readouterr().err

    def test_includes_metadata_and_full_delivery_tail(
        self,
        use_session: Session,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        assert incidents_main(["show", OPEN_KEY, "--json"]) == 0
        payload = _payload(capsys)
        assert payload["incident"]["metadata"] == {"probe": OPEN_KEY}
        # The stale row shares this incident but sits outside any default window.
        assert len(payload["deliveries"]) == 3

    def test_delivery_limit_truncates_tail(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["show", OPEN_KEY, "--deliveries", "1", "--json"]) == 0
        assert len(_payload(capsys)["deliveries"]) == 1

    def test_text_output_labels_delivery_section(
        self, use_session: Session, capsys: pytest.CaptureFixture[str]
    ) -> None:
        assert incidents_main(["show", ACKED_KEY]) == 0
        out = capsys.readouterr().out
        assert f"incident_key: {ACKED_KEY}" in out
        assert "deliveries:" in out


class TestDeliveryList:
    def test_default_window_excludes_stale_rows(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["deliveries", "list", "--json"]) == 0
        assert _payload(capsys)["count"] == 4

    def test_unbounded_window_includes_stale_rows(
        self,
        use_session: Session,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        assert incidents_main(["deliveries", "list", "--days", "0", "--json"]) == 0
        assert _payload(capsys)["count"] == 5

    def test_status_filter(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["deliveries", "list", "--status", "FAILED", "--json"]) == 0
        rows = _payload(capsys)["deliveries"]
        assert [row["error_code"] for row in rows] == ["HTTP_500"]
        assert rows[0]["error_message"] == "HTTP 500"

    def test_channel_filter(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["deliveries", "list", "--channel", "telegram", "--json"]) == 0
        assert {row["channel"] for row in _payload(capsys)["deliveries"]} == {"telegram"}

    def test_batch_id_groups_a_fanout(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["deliveries", "list", "--batch-id", "b1", "--json"]) == 0
        assert {row["channel"] for row in _payload(capsys)["deliveries"]} == {"telegram", "slack"}

    def test_incident_id_filter_respects_window(
        self,
        use_session: Session,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        incident_id = _incident_id(use_session, OPEN_KEY)
        assert incidents_main(["deliveries", "list", "--incident-id", str(incident_id), "--json"]) == 0
        assert _payload(capsys)["count"] == 2

    def test_error_code_filter(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["deliveries", "list", "--error-code", "HTTP_500", "--json"]) == 0
        assert _payload(capsys)["count"] == 1

    def test_limit_is_honoured(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["deliveries", "list", "--limit", "2", "--json"]) == 0
        assert _payload(capsys)["count"] == 2

    def test_empty_result_message(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["deliveries", "list", "--channel", "webhook"]) == 0
        assert capsys.readouterr().out.strip() == "(no delivery rows)"

    def test_renders_column_header(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["deliveries", "list"]) == 0
        header = capsys.readouterr().out.splitlines()[0]
        assert header.split() == ["DISPATCHED", "CHANNEL", "STATUS", "ATT", "LAT_MS", "ERROR_CODE", "BATCH_ID"]

    def test_negative_window_is_rejected(self, use_session: Session) -> None:
        with pytest.raises(SystemExit) as excinfo:
            incidents_main(["deliveries", "list", "--days", "-1"])
        assert excinfo.value.code == 2


class TestDeliveryStats:
    def test_groups_outcomes_inside_default_window(
        self,
        use_session: Session,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        assert incidents_main(["deliveries", "stats", "--json"]) == 0
        payload = _payload(capsys)
        assert payload["days"] == 7
        assert payload["total"] == 4
        assert payload["by_status"] == {
            DELIVERY_STATUS_DRY_RUN: 1,
            DELIVERY_STATUS_FAILED: 1,
            DELIVERY_STATUS_SENT: 1,
            DELIVERY_STATUS_SKIPPED: 1,
        }
        assert payload["by_channel"] == {"console": 1, "slack": 1, "telegram": 2}

    def test_channel_status_pairs_are_flattened_for_json(
        self,
        use_session: Session,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        assert incidents_main(["deliveries", "stats", "--json"]) == 0
        assert "slack:FAILED" in _payload(capsys)["by_channel_status"]

    def test_unbounded_window_reports_more(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["deliveries", "stats", "--days", "0", "--json"]) == 0
        payload = _payload(capsys)
        assert payload["total"] == 5
        assert payload["by_status"][DELIVERY_STATUS_SENT] == 2

    def test_status_filter_narrows_the_mix(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["deliveries", "stats", "--status", "FAILED", "--json"]) == 0
        assert _payload(capsys)["total"] == 1

    def test_text_output_names_the_window(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["deliveries", "stats"]) == 0
        out = capsys.readouterr().out
        assert "Delivery audit (last 7d)" in out
        assert "by status" in out
        assert "by channel" in out

    def test_text_output_on_empty_window(self, use_session: Session, capsys: pytest.CaptureFixture[str]) -> None:
        assert incidents_main(["deliveries", "stats", "--channel", "webhook"]) == 0
        assert "total  0" in capsys.readouterr().out


class TestMasterRouting:
    """Routing only; payloads are asserted through the direct entry point above.

    The router's first import prints the ``.env`` loader notice to stdout, so
    these assert on substrings instead of parsing the whole stream.
    """

    def test_master_cli_dispatches_incidents_list(
        self, use_session: Session, capsys: pytest.CaptureFixture[str]
    ) -> None:
        assert kbo_main(["incidents", "list", "--json"]) == 0
        assert '"count": 2' in capsys.readouterr().out

    def test_master_cli_dispatches_deliveries_stats(
        self,
        use_session: Session,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        assert kbo_main(["incidents", "deliveries", "stats", "--json"]) == 0
        assert '"total": 4' in capsys.readouterr().out
