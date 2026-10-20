"""Tests for the scheduled RAG incremental sync write-intent gating."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any
from unittest.mock import patch

import pytest

from src.constants import KST
from src.scheduler import locks
from src.scheduler.jobs.maintenance import sync_rag_incremental_job

WRITE_ENV_KEYS = (
    "RAG_TARGET_ENV",
    "RAG_INDEX_ALLOW_WRITE",
    "RAG_INDEX_ALLOW_PRODUCTION_WRITE",
)


def _run_job_with_fake_build(captured: dict[str, Any], *, build_error: Exception | None = None) -> None:
    def fake_main(argv: list[str]) -> None:
        captured["argv"] = list(argv)
        captured.setdefault("calls", []).append(list(argv))
        captured["env"] = {key: os.environ.get(key) for key in WRITE_ENV_KEYS}
        if build_error is not None:
            raise build_error

    with (
        patch.dict(os.environ, {"PGVECTOR_TEST_URL": "postgresql://vector:secret@127.0.0.1:5432/rag_vector"}),
        patch("src.scheduler.jobs.maintenance._scheduler_job_lock") as mock_lock,
        patch("src.cli.build_rag_index.main", side_effect=fake_main),
    ):
        mock_lock.return_value.__enter__.return_value = None
        sync_rag_incremental_job()


@pytest.mark.usefixtures("_clean_write_env")
def test_sync_rag_incremental_skips_without_vector_backend(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://source:secret@127.0.0.1:5432/bega_prod")
    monkeypatch.delenv("PGVECTOR_URL", raising=False)
    monkeypatch.delenv("PGVECTOR_TEST_URL", raising=False)
    build = patch("src.cli.build_rag_index.main")

    with patch("src.scheduler.jobs.maintenance._scheduler_job_lock") as mock_lock, build as build_main:
        mock_lock.return_value.__enter__.return_value = None
        sync_rag_incremental_job()

    build_main.assert_not_called()


@pytest.mark.usefixtures("_clean_write_env")
def test_sync_rag_incremental_declares_production_write_intent() -> None:
    captured: dict[str, Any] = {}
    _run_job_with_fake_build(captured)

    assert captured["env"] == {
        "RAG_TARGET_ENV": "production",
        "RAG_INDEX_ALLOW_WRITE": "1",
        "RAG_INDEX_ALLOW_PRODUCTION_WRITE": "1",
    }


@pytest.mark.usefixtures("_clean_write_env")
def test_sync_rag_incremental_passes_incremental_source_argv() -> None:
    """Every source but PBP runs as before; PBP runs last, alone, with its presence filters."""
    from src.scheduler.jobs.maintenance import rag_incremental_invocations

    captured: dict[str, Any] = {}
    _run_job_with_fake_build(captured)

    assert captured["calls"] == rag_incremental_invocations(datetime.now(KST).year)
    assert captured["argv"][:2] == ["--source", "pbp"]
    assert "all" not in [argv[1] for argv in captured["calls"] if len(argv) > 1]


@pytest.mark.usefixtures("_clean_write_env")
def test_sync_rag_incremental_restores_env_after_success() -> None:
    captured: dict[str, Any] = {}
    _run_job_with_fake_build(captured)

    assert all(os.environ.get(key) is None for key in WRITE_ENV_KEYS)


@pytest.mark.usefixtures("_clean_write_env")
def test_sync_rag_incremental_restores_env_after_failure() -> None:
    captured: dict[str, Any] = {}
    _run_job_with_fake_build(captured, build_error=RuntimeError("embedding provider down"))

    assert all(os.environ.get(key) is None for key in WRITE_ENV_KEYS)


def _run_job_recording_alerts(
    failing_sources: set[str],
) -> tuple[list[str], list[tuple[str, str]], list[tuple[str, str]]]:
    """Run the job with a fake builder that fails for the named sources; return calls and the alerts raised."""
    calls: list[str] = []
    warnings: list[tuple[str, str]] = []
    successes: list[tuple[str, str]] = []

    def fake_main(argv: list[str]) -> None:
        source = argv[argv.index("--source") + 1]
        calls.append(source)
        if source in failing_sources:
            message = f"{source} down"
            raise RuntimeError(message)

    with (
        patch.dict(os.environ, {"PGVECTOR_TEST_URL": "postgresql://vector:secret@127.0.0.1:5432/rag_vector"}),
        patch("src.scheduler.jobs.maintenance._scheduler_job_lock") as mock_lock,
        patch("src.cli.build_rag_index.main", side_effect=fake_main),
        patch("src.scheduler.jobs.maintenance.alert_warning", lambda *a, **_k: warnings.append((a[0], a[1]))),
        patch("src.scheduler.jobs.maintenance.alert_success", lambda *a, **_k: successes.append((a[0], a[1]))),
    ):
        mock_lock.return_value.__enter__.return_value = None
        sync_rag_incremental_job()
    return calls, warnings, successes


@pytest.mark.usefixtures("_clean_write_env")
def test_a_failing_source_does_not_stop_the_sources_after_it() -> None:
    """PBP is last; an earlier failure used to abort the run so PBP never ran and nothing alerted."""
    from src.scheduler.jobs.maintenance import rag_incremental_invocations

    expected = [argv[argv.index("--source") + 1] for argv in rag_incremental_invocations(datetime.now(KST).year)]
    calls, _warnings, _successes = _run_job_recording_alerts({"batting"})

    assert calls == expected
    assert calls[-1] == "pbp"


@pytest.mark.usefixtures("_clean_write_env")
def test_failures_raise_a_warning_naming_the_sources_and_no_success_alert() -> None:
    _calls, warnings, successes = _run_job_recording_alerts({"batting", "pbp"})

    assert len(warnings) == 1
    assert warnings[0][0] == "sync_rag_incremental"
    assert "batting" in warnings[0][1]
    assert "pbp" in warnings[0][1]
    assert successes == []


@pytest.mark.usefixtures("_clean_write_env")
def test_all_sources_succeeding_raises_only_the_success_alert() -> None:
    _calls, warnings, successes = _run_job_recording_alerts(set())

    assert warnings == []
    assert len(successes) == 1


@pytest.fixture(autouse=True)
def _silence_alert_bridge(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the job's build-outcome alerts out of the incident ledger.

    The job now reports both outcomes through ``alert_warning``/``alert_success``,
    and the real bridge writes a delivery-audit row. ``tests/scheduler/conftest.py``
    refuses that write from a scheduler unit test, because whether it happens at
    all depends on ambient channel config rather than on the behaviour under test.
    Nothing here asserts on the alerts, so stubbing the seam is enough.
    """
    monkeypatch.setattr("src.scheduler.jobs.maintenance.alert_warning", lambda *_a, **_k: None)
    monkeypatch.setattr("src.scheduler.jobs.maintenance.alert_success", lambda *_a, **_k: None)


@pytest.fixture(autouse=True)
def _reachable_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the DB fail-fast gate out of these tests.

    The job now gates on every database a build would open, which in these
    fixtures is a ``127.0.0.1`` PostgreSQL URL that is not listening. Without
    this the job skips on the probe and every assertion about the build itself
    becomes vacuously true.
    """
    monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: True)
    locks._reset_db_gate()
    yield
    locks._reset_db_gate()


@pytest.fixture
def _clean_write_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in WRITE_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
