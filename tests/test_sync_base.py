from __future__ import annotations

from datetime import datetime

import pytest
from sqlalchemy.exc import DBAPIError, OperationalError

from src.sync.sync_base import OCISyncBase, _serialize_scalar


# ── _serialize_scalar ─────────────────────────────────────────────────

class TestSerializeScalar:
    def test_none_returns_none(self):
        assert _serialize_scalar(None) is None

    def test_string_returns_as_is(self):
        assert _serialize_scalar("hello") == "hello"

    def test_int_returns_as_is(self):
        assert _serialize_scalar(42) == 42

    def test_float_returns_as_is(self):
        assert _serialize_scalar(3.14) == 3.14

    def test_bool_returns_as_is(self):
        assert _serialize_scalar(True) is True

    def test_datetime_isoformat(self):
        dt = datetime(2025, 4, 1, 14, 30, 0)
        assert _serialize_scalar(dt) == "2025-04-01T14:30:00"

    def test_date_isoformat(self):
        from datetime import date
        d = date(2025, 4, 1)
        assert _serialize_scalar(d) == "2025-04-01"

    def test_dict_passes_through(self):
        val = {"a": 1}
        assert _serialize_scalar(val) is val

    def test_list_passes_through(self):
        val = [1, 2, 3]
        assert _serialize_scalar(val) is val


# ── _chunked ──────────────────────────────────────────────────────────

class TestChunked:
    def test_empty_list(self):
        assert OCISyncBase._chunked([], 10) == []

    def test_smaller_than_chunk_size(self):
        assert OCISyncBase._chunked([1, 2], 10) == [[1, 2]]

    def test_exact_chunk_size(self):
        assert OCISyncBase._chunked([1, 2, 3], 3) == [[1, 2, 3]]

    def test_remainder_chunk(self):
        result = OCISyncBase._chunked([1, 2, 3, 4, 5], 2)
        assert result == [[1, 2], [3, 4], [5]]
        assert len(result) == 3

    def test_chunk_size_one(self):
        assert OCISyncBase._chunked([1, 2, 3], 1) == [[1], [2], [3]]

    def test_chunk_size_larger_than_items(self):
        assert OCISyncBase._chunked([1], 100) == [[1]]

    def test_preserves_strings(self):
        result = OCISyncBase._chunked(["a", "b", "c", "d"], 2)
        assert result == [["a", "b"], ["c", "d"]]


# ── _is_transient_oci_error ───────────────────────────────────────────

class TestIsTransientOciError:
    def test_operational_error_is_transient(self):
        assert OCISyncBase._is_transient_oci_error(OperationalError("stmt", {}, None)) is True

    def test_dbapi_error_with_invalidated_connection(self):
        exc = DBAPIError("stmt", {}, None)
        exc.connection_invalidated = True
        assert OCISyncBase._is_transient_oci_error(exc) is True

    def test_dbapi_error_without_invalidated_connection(self):
        exc = DBAPIError("stmt", {}, None)
        exc.connection_invalidated = False
        assert OCISyncBase._is_transient_oci_error(exc) is False

    def test_connection_closed_message(self):
        exc = RuntimeError("connection already closed")
        assert OCISyncBase._is_transient_oci_error(exc) is True

    def test_server_closed_connection_message(self):
        exc = RuntimeError("server closed the connection unexpectedly")
        assert OCISyncBase._is_transient_oci_error(exc) is True

    def test_could_not_receive_data_message(self):
        exc = RuntimeError("could not receive data from server")
        assert OCISyncBase._is_transient_oci_error(exc) is True

    def test_operation_timed_out_message(self):
        exc = RuntimeError("operation timed out")
        assert OCISyncBase._is_transient_oci_error(exc) is True

    def test_ssl_syscall_message(self):
        exc = RuntimeError("SSL syscall error: connection reset")
        assert OCISyncBase._is_transient_oci_error(exc) is True

    def test_terminating_connection_message(self):
        exc = RuntimeError("terminating connection due to administrator")
        assert OCISyncBase._is_transient_oci_error(exc) is True

    def test_connection_reset_message(self):
        exc = RuntimeError("connection reset by peer")
        assert OCISyncBase._is_transient_oci_error(exc) is True

    def test_generic_exception_not_transient(self):
        exc = ValueError("something else")
        assert OCISyncBase._is_transient_oci_error(exc) is False

    def test_mixed_case_message_matched(self):
        exc = RuntimeError("Connection Already Closed")
        assert OCISyncBase._is_transient_oci_error(exc) is True

    def test_lookup_error_subclass_not_transient(self):
        exc = KeyError("missing")
        assert OCISyncBase._is_transient_oci_error(exc) is False

    def test_attribute_error_not_transient(self):
        exc = AttributeError("no such attr")
        assert OCISyncBase._is_transient_oci_error(exc) is False


# ── _run_target_session_with_retries ──────────────────────────────────

class TestRunTargetSessionWithRetries:
    def test_success_on_first_attempt(self):
        syncer = _build_syncer()
        op = lambda: "done"
        assert syncer._run_target_session_with_retries(op, label="test") == "done"

    def test_transient_failure_then_retry_succeeds(self, monkeypatch):
        syncer = _build_syncer()
        call_count = 0

        def op():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OperationalError("stmt", {}, None)
            return "done"

        monkeypatch.setattr(syncer, "_reconnect_oci", lambda: None)
        monkeypatch.setattr(syncer, "_rollback_target_session", lambda *, label: None)
        monkeypatch.setattr("src.sync.sync_base.time.sleep", lambda _: None)

        assert syncer._run_target_session_with_retries(op, label="test") == "done"
        assert call_count == 2

    def test_transient_failure_exhaustion_raises(self, monkeypatch):
        syncer = _build_syncer()

        def op():
            raise OperationalError("stmt", {}, None)

        monkeypatch.setattr(syncer, "_reconnect_oci", lambda: None)
        monkeypatch.setattr(syncer, "_rollback_target_session", lambda *, label: None)
        monkeypatch.setattr("src.sync.sync_base.time.sleep", lambda _: None)

        with pytest.raises(OperationalError):
            syncer._run_target_session_with_retries(op, label="test", max_retries=2)

    def test_persistent_error_raises_immediately(self, monkeypatch):
        syncer = _build_syncer()

        def op():
            raise ValueError("not transient")

        monkeypatch.setattr(syncer, "_rollback_target_session", lambda *, label: None)

        with pytest.raises(ValueError, match="not transient"):
            syncer._run_target_session_with_retries(op, label="test")

    def test_rollback_called_on_failure(self, monkeypatch):
        syncer = _build_syncer()
        rollback_log = []

        def op():
            raise RuntimeError("connection reset")

        monkeypatch.setattr(syncer, "_reconnect_oci", lambda: None)
        monkeypatch.setattr(syncer, "_rollback_target_session", lambda *, label: rollback_log.append(label))
        monkeypatch.setattr("src.sync.sync_base.time.sleep", lambda _: None)

        with pytest.raises(RuntimeError):
            syncer._run_target_session_with_retries(op, label="test_rollback", max_retries=1)
        assert len(rollback_log) == 2
        assert all(r == "test_rollback" for r in rollback_log)

    def test_reconnect_called_on_transient_retry(self, monkeypatch):
        syncer = _build_syncer()
        reconnect_log = []
        call_count = 0

        def op():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OperationalError("stmt", {}, None)
            return "ok"

        monkeypatch.setattr(syncer, "_reconnect_oci", lambda: reconnect_log.append("reconnect"))
        monkeypatch.setattr(syncer, "_rollback_target_session", lambda *, label: None)
        monkeypatch.setattr("src.sync.sync_base.time.sleep", lambda _: None)

        syncer._run_target_session_with_retries(op, label="test", max_retries=1)
        assert len(reconnect_log) == 1
        assert call_count == 2

    def test_exponential_backoff(self, monkeypatch):
        syncer = _build_syncer()
        sleep_times = []
        call_count = 0

        def op():
            nonlocal call_count
            call_count += 1
            raise OperationalError("stmt", {}, None)

        monkeypatch.setattr(syncer, "_reconnect_oci", lambda: None)
        monkeypatch.setattr(syncer, "_rollback_target_session", lambda *, label: None)
        monkeypatch.setattr("src.sync.sync_base.time.sleep", lambda t: sleep_times.append(t))

        with pytest.raises(OperationalError):
            syncer._run_target_session_with_retries(op, label="test", max_retries=2, base_delay_seconds=2.0)

        # attempt 1: no sleep (first attempt fails)
        # attempt 2: sleep 2.0 (2^0 * 2.0)
        # attempt 3 (exhausted): sleep 4.0 (2^1 * 2.0)
        assert sleep_times == pytest.approx([2.0, 4.0])

    def test_custom_max_retries(self, monkeypatch):
        syncer = _build_syncer()
        call_count = 0

        def op():
            nonlocal call_count
            call_count += 1
            raise OperationalError("stmt", {}, None)

        monkeypatch.setattr(syncer, "_reconnect_oci", lambda: None)
        monkeypatch.setattr(syncer, "_rollback_target_session", lambda *, label: None)
        monkeypatch.setattr("src.sync.sync_base.time.sleep", lambda _: None)

        with pytest.raises(OperationalError):
            syncer._run_target_session_with_retries(op, label="test", max_retries=0)
        assert call_count == 1  # no retry

    def test_non_transient_after_reconnect_still_raises(self, monkeypatch):
        syncer = _build_syncer()
        call_count = 0

        def op():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OperationalError("stmt", {}, None)
            raise ValueError("still bad after reconnect")

        monkeypatch.setattr(syncer, "_reconnect_oci", lambda: None)
        monkeypatch.setattr(syncer, "_rollback_target_session", lambda *, label: None)
        monkeypatch.setattr("src.sync.sync_base.time.sleep", lambda _: None)

        with pytest.raises(ValueError, match="still bad"):
            syncer._run_target_session_with_retries(op, label="test")
        assert call_count == 2


# ── helpers ───────────────────────────────────────────────────────────

def _build_syncer():
    import types
    syncer = object.__new__(OCISyncBase)
    syncer.sqlite_session = None
    syncer.target_session = None
    syncer.oci_engine = None
    return syncer
