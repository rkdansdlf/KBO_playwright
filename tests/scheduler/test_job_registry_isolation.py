"""Scheduler tests must not write the runtime job registry.

`data/last_runs/job_registry.json` is how a human answers "did the pipeline
run?", so it has to mean one thing: what the scheduler did. A unit test that
updates a job status persists the registry as a side effect, and its mocked
report lands in that file looking exactly like a real run.

This cost a fabricated investigation on 2026-10-12. The file was observed
changing during the day and holding `crawl_daily_games` messages from two
moments that looked like consecutive DAG runs -- a partial failure, then a
quality-gate warning. They were test fixtures. Reading them as evidence produced
a conclusion about a stuck incident that no scheduler could explain, because no
scheduler was running at all.

The redirect lives in `tests/scheduler/conftest.py`. These tests check the
redirect actually holds, including the case where the fixture is bypassed --
`monkeypatch.delenv` is how a test would reach the real path.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from src.scheduler.jobs.daily import (
    JobStatus,
    _persist_job_registry,
    _register_job,
    _update_job_status,
    clear_job_registry,
)

RUNTIME_REGISTRY = Path("data/last_runs/job_registry.json")


def _runtime_snapshot() -> tuple[str | None, float]:
    """Return the runtime registry's bytes and mtime, or (None, 0) if absent."""
    if not RUNTIME_REGISTRY.exists():
        return None, 0.0
    stat = RUNTIME_REGISTRY.stat()
    return RUNTIME_REGISTRY.read_text(encoding="utf-8"), stat.st_mtime


class TestTheRuntimeRegistryIsNotATestOutput:
    def test_updating_a_job_status_does_not_touch_the_runtime_file(self) -> None:
        """The shape that actually happened: a test calls the job function.

        ``_update_job_status`` is a no-op for an unregistered job, so the
        write only happens because the job registers itself on entry. Calling
        ``_register_job`` first is what makes this the real scenario.
        """
        before = _runtime_snapshot()

        clear_job_registry()
        _register_job("crawl_daily_games")
        _update_job_status("crawl_daily_games", JobStatus.FAILURE, "mocked partial failure")

        assert _runtime_snapshot() == before
        clear_job_registry()

    def test_persisting_directly_does_not_touch_the_runtime_file(self) -> None:
        """The guard covers the helper itself, not only one caller."""
        before = _runtime_snapshot()

        clear_job_registry()
        _register_job("crawl_daily_games")
        _persist_job_registry()

        assert _runtime_snapshot() == before
        clear_job_registry()

    def test_the_override_is_where_the_write_actually_landed(self) -> None:
        """Otherwise the first two tests would pass by the write simply failing."""
        clear_job_registry()
        _register_job("crawl_daily_games")
        _update_job_status("crawl_daily_games", JobStatus.WARNING, "routed elsewhere")

        override = Path(os.environ["JOB_REGISTRY_FILE"])
        assert override != RUNTIME_REGISTRY
        payload = json.loads(override.read_text(encoding="utf-8"))
        assert payload["jobs"]["crawl_daily_games"]["status"] == "warning"
        assert payload["jobs"]["crawl_daily_games"]["message"] == "routed elsewhere"
        clear_job_registry()


class TestTheGuardIsLoadBearing:
    def test_without_the_override_the_write_reaches_the_runtime_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Proof the redirect is what protects the file, not luck.

        Removing the override restores the production path, and the write lands
        on the runtime file. The content is restored afterwards so this test
        cannot leak the damage it demonstrates into the working tree.
        """
        before = _runtime_snapshot()
        monkeypatch.delenv("JOB_REGISTRY_FILE", raising=False)
        try:
            clear_job_registry()
            _register_job("crawl_daily_games")
            _update_job_status("crawl_daily_games", JobStatus.SUCCESS, "bypass probe")
            written, _ = _runtime_snapshot()
            assert written is not None
            assert "bypass probe" in written
        finally:
            if before[0] is not None:
                RUNTIME_REGISTRY.write_text(before[0], encoding="utf-8")
            elif RUNTIME_REGISTRY.exists():
                RUNTIME_REGISTRY.unlink()
            clear_job_registry()
