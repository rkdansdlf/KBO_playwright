"""The nightly vector backfill can be held, and holding it must not leave a warning.

One run a night, bounded to 2,000 identities, so the job cannot drain a large
backlog -- that is why its incident has been open since 2026-10-17. The brake that
held it lived in a commented-out registry entry on the deployment host, which is
invisible to git: a ``git checkout -- .`` would have resumed it silently. It is an
env flag now.

The second contract is the one the repository keeps rediscovering: a check that
does not run still owns its incident key. Nothing else closes it, so a job that
returns silently leaves an OPEN warning that no run can clear.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from src.scheduler.jobs.daily import RAG_VECTOR_BACKFILL_ENABLED_ENV, rag_vector_backfill_job

_BACKFILL = "src.cli.rag.backfill_missing_vectors.main"
_ALERT_SUCCESS = "src.scheduler.jobs.daily.alert_success"
_ALERT_WARNING = "src.scheduler.jobs.daily.alert_warning"


@pytest.fixture
def calls() -> dict[str, Any]:
    return {"backfill": [], "success": [], "warning": []}


@pytest.fixture
def _stubbed_backfill(calls):
    """Replace the backfill entrypoint and the two alert helpers."""

    def fake_main(argv: list[str]) -> int:
        calls["backfill"].append(list(argv))
        return 0

    with (
        patch(_BACKFILL, side_effect=fake_main),
        patch(_ALERT_SUCCESS, side_effect=lambda name, details=None: calls["success"].append((name, details))),
        patch(_ALERT_WARNING, side_effect=lambda name, **kw: calls["warning"].append((name, kw))),
        patch("src.scheduler.jobs.daily._update_job_status"),
    ):
        yield


class TestWhileTheJobIsHeld:
    def test_the_backfill_does_not_run(self, calls, _stubbed_backfill, monkeypatch) -> None:
        monkeypatch.setenv(RAG_VECTOR_BACKFILL_ENABLED_ENV, "0")

        rag_vector_backfill_job()

        assert calls["backfill"] == []

    def test_the_hold_closes_its_own_warning(self, calls, _stubbed_backfill, monkeypatch) -> None:
        """Otherwise the incident opened on 10-17 would outlive the job forever."""
        monkeypatch.setenv(RAG_VECTOR_BACKFILL_ENABLED_ENV, "0")

        rag_vector_backfill_job()

        assert calls["warning"] == []
        assert [name for name, _ in calls["success"]] == ["rag_vector_backfill"]
        assert RAG_VECTOR_BACKFILL_ENABLED_ENV in calls["success"][0][1]


class TestWhenTheJobRuns:
    def test_it_runs_by_default(self, calls, _stubbed_backfill, monkeypatch) -> None:
        """The flag holds the job; its absence is not a hold."""
        monkeypatch.delenv(RAG_VECTOR_BACKFILL_ENABLED_ENV, raising=False)

        rag_vector_backfill_job()

        assert len(calls["backfill"]) == 1
        assert calls["backfill"][0][:2] == ["--apply", "--limit"]
        assert [name for name, _ in calls["success"]] == ["rag_vector_backfill"]

    def test_another_value_for_the_flag_is_not_a_hold(self, calls, _stubbed_backfill, monkeypatch) -> None:
        """``0`` is the hold; anything else leaves the job as it was."""
        monkeypatch.setenv(RAG_VECTOR_BACKFILL_ENABLED_ENV, "1")

        rag_vector_backfill_job()

        assert len(calls["backfill"]) == 1

    def test_a_failing_backfill_still_warns(self, calls, monkeypatch) -> None:
        monkeypatch.delenv(RAG_VECTOR_BACKFILL_ENABLED_ENV, raising=False)

        with (
            patch(_BACKFILL, return_value=2),
            patch(_ALERT_SUCCESS, side_effect=lambda name, details=None: calls["success"].append((name, details))),
            patch(_ALERT_WARNING, side_effect=lambda name, **kw: calls["warning"].append((name, kw))),
            patch("src.scheduler.jobs.daily._update_job_status"),
        ):
            rag_vector_backfill_job()

        assert [name for name, _ in calls["success"]] == []
        assert [name for name, _ in calls["warning"]] == ["rag_vector_backfill"]

    def test_the_flag_name_is_the_one_the_deployment_sets(self) -> None:
        """Renaming the constant would silently ignore the host's ``.env``."""
        assert RAG_VECTOR_BACKFILL_ENABLED_ENV == "RAG_VECTOR_BACKFILL_ENABLED"
