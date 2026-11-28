"""The DB watchdog probe must say *how* the database is unreachable.

TCP reachability is not availability: the 2026-11-07/08 outage spent most of its
two and a half days with the port accepting connections while PostgreSQL never
completed a handshake. That is why the probe reports ``tcp_unreachable`` and
``postgres_silent`` as different reasons -- an operator wakes up to a different
problem in each case. These tests pin the exit-code contract the workflow reads
(0 reachable, 1 unreachable, 2 configuration error), both failure reasons, and
the property that the password in the URL never reaches the output.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import psycopg
import pytest

import scripts.verification.check_db_availability as module

URL = "postgresql+psycopg://kbo_ci_ro:hunter2@100.81.73.13:5432/bega_prod"


class _FakeConnection:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute(self, statement: str) -> None:
        self.executed.append(statement)

    def __enter__(self) -> _FakeConnection:
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False


def _socket_reachable(monkeypatch) -> None:
    monkeypatch.setattr(module.socket, "create_connection", lambda *args, **kwargs: MagicMock())


def _socket_refused(monkeypatch) -> None:
    def _refuse(*_args, **_kwargs):
        raise OSError("no route to host")

    monkeypatch.setattr(module.socket, "create_connection", _refuse)


class TestTargetParsing:
    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            ("postgresql://user@host:5432/db", ("host", 5432)),
            ("postgresql+psycopg://user@host:5432/db", ("host", 5432)),
            ("postgres+psycopg2://user@host/db", ("host", 5432)),
            ("postgresql://user:pw@100.81.73.13:5433/db", ("100.81.73.13", 5433)),
        ],
    )
    def test_accepted_forms(self, url: str, expected: tuple[str, int]) -> None:
        assert module.target_from_url(url) == expected

    @pytest.mark.parametrize("url", ["mysql://user@host/db", "postgresql://", "not-a-url"])
    def test_non_postgres_targets(self, url: str) -> None:
        assert module.target_from_url(url) is None


class TestExitCodes:
    def test_no_url_is_a_config_error(self, monkeypatch) -> None:
        monkeypatch.delenv("LIVE_DB_URL", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)

        assert module.main([]) == 2

    def test_a_non_postgres_url_is_a_config_error(self) -> None:
        assert module.main(["--url", "mysql://user@host/db"]) == 2


class TestReachability:
    def test_a_tcp_failure_reports_tcp_unreachable_and_skips_the_handshake(self, monkeypatch, capsys) -> None:
        _socket_refused(monkeypatch)
        connect = MagicMock()
        monkeypatch.setattr(module.psycopg, "connect", connect)

        assert module.main(["--url", URL, "--json"]) == 1

        payload = json.loads(capsys.readouterr().out)
        assert payload["reachable"] is False
        assert payload["reason"] == "tcp_unreachable"
        assert payload["host"] == "100.81.73.13"
        assert payload["port"] == 5432
        connect.assert_not_called()

    def test_a_silent_postgres_reports_postgres_silent(self, monkeypatch, capsys) -> None:
        """Port open, handshake never completes -- the shape of the 11-07 outage."""
        _socket_reachable(monkeypatch)

        def _timeout(*_args, **_kwargs):
            raise psycopg.OperationalError("connection timeout expired")

        monkeypatch.setattr(module.psycopg, "connect", _timeout)

        assert module.main(["--url", URL, "--json"]) == 1

        payload = json.loads(capsys.readouterr().out)
        assert payload["reachable"] is False
        assert payload["reason"] == "postgres_silent"

    def test_a_handshake_that_answers_is_reachable(self, monkeypatch, capsys) -> None:
        _socket_reachable(monkeypatch)
        connection = _FakeConnection()
        monkeypatch.setattr(module.psycopg, "connect", lambda *_args, **_kwargs: connection)

        assert module.main(["--url", URL, "--json"]) == 0

        payload = json.loads(capsys.readouterr().out)
        assert payload["reachable"] is True
        assert payload["reason"] == "reachable"
        assert connection.executed == ["SELECT 1"]

    def test_the_plain_output_names_the_failure_reason(self, monkeypatch, capsys) -> None:
        _socket_refused(monkeypatch)

        assert module.main(["--url", URL]) == 1

        out = capsys.readouterr().out
        assert "UNREACHABLE (tcp_unreachable)" in out


class TestThePasswordNeverLeaks:
    @pytest.mark.parametrize("mode", ["reachable", "tcp_refused", "silent"])
    def test_output_never_contains_the_password(self, monkeypatch, capsys, mode: str) -> None:
        if mode == "reachable":
            _socket_reachable(monkeypatch)
            monkeypatch.setattr(module.psycopg, "connect", lambda *_a, **_k: _FakeConnection())
        elif mode == "tcp_refused":
            _socket_refused(monkeypatch)
        else:
            _socket_reachable(monkeypatch)

            def _timeout(*_args, **_kwargs):
                raise psycopg.OperationalError("connection timeout expired")

            monkeypatch.setattr(module.psycopg, "connect", _timeout)

        module.main(["--url", URL, "--json"])

        captured = capsys.readouterr()
        assert "hunter2" not in captured.out
        assert "hunter2" not in captured.err
