"""Tests for the scheduled sparse terms catch-up job."""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import patch

import pytest

from src.scheduler.jobs.maintenance import sparse_terms_catchup_job

WRITE_ENV_KEYS = (
    "RAG_TARGET_ENV",
    "RAG_INDEX_ALLOW_WRITE",
    "RAG_INDEX_ALLOW_PRODUCTION_WRITE",
)


def _run_job_with_fake_build(
    captured: dict[str, Any],
    *,
    exit_code: int = 0,
    build_error: Exception | None = None,
) -> None:
    def fake_main(argv: list[str]) -> int:
        captured["argv"] = list(argv)
        captured["env"] = {key: os.environ.get(key) for key in WRITE_ENV_KEYS}
        if build_error is not None:
            raise build_error
        return exit_code

    with (
        patch("src.scheduler.jobs.maintenance._scheduler_job_lock") as mock_lock,
        patch("src.cli.rag.build_oracle_sparse_index.main", side_effect=fake_main),
    ):
        mock_lock.return_value.__enter__.return_value = None
        sparse_terms_catchup_job()


@pytest.mark.usefixtures("_clean_write_env")
def test_sparse_terms_catchup_declares_production_write_intent() -> None:
    captured: dict[str, Any] = {}
    _run_job_with_fake_build(captured)

    assert captured["env"] == {
        "RAG_TARGET_ENV": "production",
        "RAG_INDEX_ALLOW_WRITE": "1",
        "RAG_INDEX_ALLOW_PRODUCTION_WRITE": "1",
    }


@pytest.mark.usefixtures("_clean_write_env")
def test_sparse_terms_catchup_passes_insert_only_resume_argv() -> None:
    captured: dict[str, Any] = {}
    _run_job_with_fake_build(captured)

    assert captured["argv"] == ["--apply", "--catch-up", "--batch-size", "40", "--json"]


@pytest.mark.usefixtures("_clean_write_env")
def test_sparse_terms_catchup_restores_env_after_success() -> None:
    captured: dict[str, Any] = {}
    _run_job_with_fake_build(captured)

    assert all(os.environ.get(key) is None for key in WRITE_ENV_KEYS)


@pytest.mark.usefixtures("_clean_write_env")
def test_sparse_terms_catchup_restores_env_after_failure() -> None:
    captured: dict[str, Any] = {}
    _run_job_with_fake_build(captured, build_error=RuntimeError("oracle connection closed"))

    assert all(os.environ.get(key) is None for key in WRITE_ENV_KEYS)


@pytest.mark.usefixtures("_clean_write_env")
def test_sparse_terms_catchup_swallows_nonzero_exit_without_raising() -> None:
    captured: dict[str, Any] = {}
    _run_job_with_fake_build(captured, exit_code=1)

    assert captured["argv"][1] == "--catch-up"


@pytest.fixture
def _clean_write_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in WRITE_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
