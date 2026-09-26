"""Tests for guarded `kbo dlq` operator actions."""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.cli.dlq_operator import main as operator_main
from src.cli.kbo import main as kbo_main
from src.models.crawl_dead_letter import DlqStatus
from src.services.crawl_dead_letter_service import DlqNotFoundError
from src.services.crawl_dead_letter_state import InvalidDlqTransitionError


@contextlib.contextmanager
def _fake_session():
    yield MagicMock()


def _letter(*, status: str = "pending", next_retry_at: datetime | None = None) -> SimpleNamespace:
    return SimpleNamespace(dlq_id="dlq-1", status=status, next_retry_at=next_retry_at, crawler="awards")


def _patch_letter(monkeypatch: pytest.MonkeyPatch, letter: object) -> None:
    monkeypatch.setattr("src.cli.dlq_operator.get_db_session", _fake_session)
    monkeypatch.setattr(
        "src.cli.dlq_operator.CrawlDeadLetterRepository",
        lambda session: SimpleNamespace(get_by_dlq_id=lambda _dlq_id: letter),
    )
    monkeypatch.setattr("src.cli.dlq_operator.publish_dlq_state_metrics", lambda **_kwargs: None)


def test_retry_preview_without_apply(monkeypatch, capsys) -> None:
    monkeypatch.delenv("KBO_ALLOW_DLQ_MUTATION", raising=False)
    called: list[int] = []
    monkeypatch.setattr("src.cli.dlq_operator.retry_dead_letter", lambda *a, **k: called.append(1))
    assert operator_main(["retry", "dlq-1"]) == 0
    assert called == []
    assert "would retry" in capsys.readouterr().out


def test_apply_without_env_is_denied(monkeypatch, capsys) -> None:
    monkeypatch.delenv("KBO_ALLOW_DLQ_MUTATION", raising=False)
    called: list[int] = []
    monkeypatch.setattr("src.cli.dlq_operator.retry_dead_letter", lambda *a, **k: called.append(1))
    assert operator_main(["retry", "dlq-1", "--apply"]) == 3
    assert called == []
    assert "KBO_ALLOW_DLQ_MUTATION" in capsys.readouterr().err


def test_retry_apply_success(monkeypatch, capsys) -> None:
    monkeypatch.setenv("KBO_ALLOW_DLQ_MUTATION", "1")
    _patch_letter(monkeypatch, _letter(next_retry_at=datetime.now(UTC).replace(tzinfo=None) - timedelta(minutes=1)))
    monkeypatch.setattr(
        "src.cli.dlq_operator.retry_dead_letter",
        lambda *_a, **_k: SimpleNamespace(status=DlqStatus.RESOLVED, success=True),
    )
    assert operator_main(["retry", "dlq-1", "--apply"]) == 0
    assert "resolved" in capsys.readouterr().out


def test_retry_not_found(monkeypatch, capsys) -> None:
    monkeypatch.setenv("KBO_ALLOW_DLQ_MUTATION", "1")
    _patch_letter(monkeypatch, None)
    assert operator_main(["retry", "missing", "--apply"]) == 1
    assert "not found" in capsys.readouterr().err


def test_retry_rejects_non_pending(monkeypatch, capsys) -> None:
    monkeypatch.setenv("KBO_ALLOW_DLQ_MUTATION", "1")
    _patch_letter(monkeypatch, _letter(status=DlqStatus.RESOLVED.value))
    assert operator_main(["retry", "dlq-1", "--apply"]) == 2
    assert "pending" in capsys.readouterr().err


def test_retry_rejects_not_due(monkeypatch, capsys) -> None:
    monkeypatch.setenv("KBO_ALLOW_DLQ_MUTATION", "1")
    _patch_letter(monkeypatch, _letter(next_retry_at=datetime.now(UTC).replace(tzinfo=None) + timedelta(hours=1)))
    assert operator_main(["retry", "dlq-1", "--apply"]) == 2
    assert "not due" in capsys.readouterr().err


def test_requeue_success(monkeypatch, capsys) -> None:
    monkeypatch.setenv("KBO_ALLOW_DLQ_MUTATION", "1")
    monkeypatch.setattr("src.cli.dlq_operator.get_db_session", _fake_session)
    service = MagicMock()
    monkeypatch.setattr("src.cli.dlq_operator.CrawlDeadLetterService", lambda _session: service)
    monkeypatch.setattr("src.cli.dlq_operator.publish_dlq_state_metrics", lambda **_kwargs: None)
    assert operator_main(["requeue", "dlq-1", "--apply"]) == 0
    service.requeue.assert_called_once_with("dlq-1")
    assert "requeued" in capsys.readouterr().out


def test_requeue_invalid_state(monkeypatch, capsys) -> None:
    monkeypatch.setenv("KBO_ALLOW_DLQ_MUTATION", "1")
    monkeypatch.setattr("src.cli.dlq_operator.get_db_session", _fake_session)
    service = MagicMock()
    service.requeue.side_effect = InvalidDlqTransitionError("resolved", "pending")
    monkeypatch.setattr("src.cli.dlq_operator.CrawlDeadLetterService", lambda _session: service)
    assert operator_main(["requeue", "dlq-1", "--apply"]) == 2


def test_requeue_not_found(monkeypatch) -> None:
    monkeypatch.setenv("KBO_ALLOW_DLQ_MUTATION", "1")
    monkeypatch.setattr("src.cli.dlq_operator.get_db_session", _fake_session)
    service = MagicMock()
    service.requeue.side_effect = DlqNotFoundError("dlq-1")
    monkeypatch.setattr("src.cli.dlq_operator.CrawlDeadLetterService", lambda _session: service)
    assert operator_main(["requeue", "dlq-1", "--apply"]) == 1


def test_ignore_pending_only(monkeypatch, capsys) -> None:
    monkeypatch.setenv("KBO_ALLOW_DLQ_MUTATION", "1")
    _patch_letter(monkeypatch, _letter(status=DlqStatus.EXHAUSTED.value))
    assert operator_main(["ignore", "dlq-1", "--apply"]) == 2

    service = MagicMock()
    monkeypatch.setattr("src.cli.dlq_operator.CrawlDeadLetterService", lambda _session: service)
    _patch_letter(monkeypatch, _letter(status=DlqStatus.PENDING.value))
    assert operator_main(["ignore", "dlq-1", "--reason", "noise", "--apply"]) == 0
    service.mark_ignored.assert_called_once_with("dlq-1", reason="noise")


def test_master_cli_routes_operator_command(monkeypatch) -> None:
    monkeypatch.setenv("KBO_ALLOW_DLQ_MUTATION", "1")
    _patch_letter(monkeypatch, _letter(next_retry_at=datetime.now(UTC).replace(tzinfo=None) - timedelta(minutes=1)))
    monkeypatch.setattr(
        "src.cli.dlq_operator.retry_dead_letter",
        lambda *_a, **_k: SimpleNamespace(status=DlqStatus.RESOLVED, success=True),
    )
    assert kbo_main(["dlq", "retry", "dlq-1", "--apply"]) == 0
