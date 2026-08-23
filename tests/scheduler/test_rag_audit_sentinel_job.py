"""Tests for the RAG index audit sentinel job."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from src.scheduler.jobs.sentinel import rag_audit_sentinel_job


def _run(exit_code: int | None, exc: Exception | None = None) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    def fake_main(argv: list[str]) -> int:
        captured["argv"] = list(argv)
        if exc is not None:
            raise exc
        return exit_code or 0

    def fake_alert(func_name: str, details: str | None = None) -> None:
        captured.setdefault("alerts", []).append((func_name, details))

    with (
        patch("src.cli.rag.audit_rag_index.main", side_effect=fake_main),
        patch("src.scheduler.jobs.sentinel.alert_warning", side_effect=fake_alert),
    ):
        rag_audit_sentinel_job()
    return captured


def test_sentinel_passes_gate_flags_to_audit_cli() -> None:
    captured = _run(0)

    assert captured["argv"] == ["--require-nonempty", "--require-postings", "--json"]
    assert "alerts" not in captured


@pytest.mark.parametrize("exit_code", [1, 2])
def test_sentinel_warns_on_failed_audit(exit_code: int) -> None:
    captured = _run(exit_code)

    assert len(captured["alerts"]) == 1
    func_name, details = captured["alerts"][0]
    assert func_name == "rag_audit_sentinel"
    assert str(exit_code) in details


def test_sentinel_warns_on_exception() -> None:
    captured = _run(None, exc=RuntimeError("Oracle unavailable"))

    assert len(captured["alerts"]) == 1
