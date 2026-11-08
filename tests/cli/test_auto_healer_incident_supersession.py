"""A healer alert must not outlive the run that made it stale.

The start alert is a point-in-time summary keyed by the earliest anomaly date,
so when the anomaly set changes the key changes -- and the previous key, closed
by nothing, stays OPEN forever. Measured 2026-10-30 -> 11-01: the summary moved
from ``auto_healer:anomaly:2011-07-23`` to ``auto_healer:anomaly:2011-10-25``
and the first key was still on the incident board a week later. The same class
was already fixed for ``recovery:detail:`` keys and for date-scoped integrity
keys; this is the healer's turn.

Two behaviours are pinned: a run that publishes resolves the keys it
supersedes, and a run that finds nothing resolves all of them -- it is the
current truth either way. A dry run resolves nothing: it may read, never write.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

import src.cli.backfill.auto_healer as auto_healer_module
from src.models.notification_incident import NotificationIncident
from src.services.recovery_manager import RecoveryManager

NOW = datetime(2026, 11, 8, 3, 0, 0)
OLD_KEY = "auto_healer:anomaly:2011-07-23"
NEWER_KEY = "auto_healer:anomaly:2011-10-25"


@pytest.fixture
def session_factory():
    engine = create_engine("sqlite:///:memory:")
    NotificationIncident.__table__.create(engine)
    return sessionmaker(bind=engine)


@pytest.fixture(autouse=True)
def _use_the_test_session(session_factory, monkeypatch):
    monkeypatch.setattr(auto_healer_module, "SessionLocal", session_factory)


def _incident(session_factory, key: str, *, state: str = "OPEN") -> None:
    with session_factory() as session:
        session.add(
            NotificationIncident(
                incident_key=key,
                source="recovery",
                component="auto_healer",
                severity="WARNING",
                state=state,
                title="t",
                message="m",
                details_hash="x",
                occurrence_count=1,
                notification_count=1,
                first_opened_at=NOW,
                last_seen_at=NOW,
            ),
        )
        session.commit()


def _capturing_apply(monkeypatch) -> dict:
    captured: dict = {"events": [], "resolve_keys": []}

    def _fake(events, **kwargs):
        captured["events"] = list(events)
        captured["resolve_keys"] = list(kwargs.get("resolve_keys") or ())

    monkeypatch.setattr(auto_healer_module, "apply_incidents", _fake)
    return captured


class TestTheSupersededKeysHelper:
    def test_an_older_open_key_is_returned(self, session_factory) -> None:
        _incident(session_factory, OLD_KEY)

        assert auto_healer_module._superseded_anomaly_keys(NEWER_KEY) == [OLD_KEY]

    def test_the_current_key_is_not_returned(self, session_factory) -> None:
        _incident(session_factory, NEWER_KEY)

        assert auto_healer_module._superseded_anomaly_keys(NEWER_KEY) == []

    def test_recovered_incidents_are_not_listed(self, session_factory) -> None:
        _incident(session_factory, OLD_KEY, state="RECOVERED")

        assert auto_healer_module._superseded_anomaly_keys(None) == []

    def test_keys_outside_the_namespace_are_left_alone(self, session_factory) -> None:
        _incident(session_factory, "integrity:no_null_player_ids:20261029")

        assert auto_healer_module._superseded_anomaly_keys(None) == []

    def test_a_read_failure_returns_nothing(self, monkeypatch) -> None:
        """The listing is additive; losing it must not cost the alert itself."""

        def _unreachable():
            message = "database is down"
            raise SQLAlchemyError(message)

        monkeypatch.setattr(auto_healer_module, "SessionLocal", _unreachable)

        assert auto_healer_module._superseded_anomaly_keys(None) == []


class TestThePublishingRunClosesThePreviousKey:
    def test_the_start_alert_resolves_the_superseded_key(self, session_factory, monkeypatch) -> None:
        _incident(session_factory, OLD_KEY)
        captured = _capturing_apply(monkeypatch)

        auto_healer_module._send_healer_start_alert(2, [], [MagicMock(), MagicMock()], [date(2011, 10, 25)])

        assert [event.incident_key for event in captured["events"]] == [NEWER_KEY]
        assert captured["resolve_keys"] == [OLD_KEY]


class TestTheNoAnomalyRunClosesEverything:
    def _clean_finders(self, monkeypatch, tmp_path) -> None:
        monkeypatch.setattr(auto_healer_module, "_find_stuck_games", MagicMock(return_value=[]))
        monkeypatch.setattr(auto_healer_module, "_find_inconsistent_games", MagicMock(return_value=[]))
        monkeypatch.setattr(auto_healer_module, "_find_pa_formula_inconsistent_games", MagicMock(return_value=[]))
        monkeypatch.setattr(auto_healer_module, "automatic_write_allowed", MagicMock(return_value=True))
        monkeypatch.setattr(auto_healer_module, "current_season_year", MagicMock(return_value=2026))
        monkeypatch.setattr(auto_healer_module, "reconcile_pending_quarantines", MagicMock(return_value=0))
        monkeypatch.setattr(
            auto_healer_module,
            "RecoveryManager",
            lambda **kwargs: RecoveryManager(str(tmp_path / "healer_checkpoint.json"), **kwargs),
        )

    def test_a_clean_run_resolves_every_open_key(self, session_factory, tmp_path, monkeypatch) -> None:
        _incident(session_factory, OLD_KEY)
        _incident(session_factory, NEWER_KEY)
        captured = _capturing_apply(monkeypatch)
        self._clean_finders(monkeypatch, tmp_path)

        asyncio.run(auto_healer_module.run_healer_async(dry_run=False))

        assert captured["events"] == []
        assert sorted(captured["resolve_keys"]) == [OLD_KEY, NEWER_KEY]

    def test_a_dry_run_resolves_nothing(self, session_factory, tmp_path, monkeypatch) -> None:
        _incident(session_factory, OLD_KEY)
        applied = MagicMock()
        monkeypatch.setattr(auto_healer_module, "apply_incidents", applied)
        self._clean_finders(monkeypatch, tmp_path)

        asyncio.run(auto_healer_module.run_healer_async(dry_run=True))

        applied.assert_not_called()
