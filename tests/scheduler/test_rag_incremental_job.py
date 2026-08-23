"""Tests for the scheduled RAG incremental sync write-intent gating."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any
from unittest.mock import patch

import pytest

from src.constants import KST
from src.scheduler.jobs.maintenance import sync_rag_incremental_job

WRITE_ENV_KEYS = (
    "RAG_TARGET_ENV",
    "RAG_INDEX_ALLOW_WRITE",
    "RAG_INDEX_ALLOW_PRODUCTION_WRITE",
)


def _run_job_with_fake_build(captured: dict[str, Any], *, build_error: Exception | None = None) -> None:
    def fake_main(argv: list[str]) -> None:
        captured["argv"] = list(argv)
        captured["env"] = {key: os.environ.get(key) for key in WRITE_ENV_KEYS}
        if build_error is not None:
            raise build_error

    with (
        patch("src.scheduler.jobs.maintenance._scheduler_job_lock") as mock_lock,
        patch("src.cli.build_rag_index.main", side_effect=fake_main),
    ):
        mock_lock.return_value.__enter__.return_value = None
        sync_rag_incremental_job()


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
    captured: dict[str, Any] = {}
    _run_job_with_fake_build(captured)

    assert captured["argv"] == [
        "--source",
        "all",
        "--season",
        str(datetime.now(KST).year),
        "--skip-existing",
    ]


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


@pytest.fixture
def _clean_write_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in WRITE_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
