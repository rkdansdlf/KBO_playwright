"""Tests for the read-only RAG tombstone audit CLI."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.rag.audit_rag_tombstones import main
from src.services.rag_tombstone_audit import TombstoneIdentityAudit


def _session_context() -> MagicMock:
    context = MagicMock()
    context.__enter__.return_value = MagicMock()
    return context


def test_audit_tombstones_renders_json_without_failing_by_default(capsys) -> None:
    """Report unexplained rows while keeping the default read-only command non-blocking."""
    report = TombstoneIdentityAudit(("game:g1",), (), ("game:g1",))
    with (
        patch("src.cli.rag.audit_rag_tombstones.get_rag_index_session", return_value=_session_context()),
        patch("src.cli.rag.audit_rag_tombstones.audit_tombstone_session", return_value=report),
    ):
        assert main(["--json"]) == 0

    output = capsys.readouterr().out
    assert '"classification": "UNEXPLAINED"' in output
    assert '"unexplained": 1' in output


def test_audit_tombstones_can_fail_on_unexplained_rows() -> None:
    """Support an explicit CI or scheduler gate for unexplained tombstones."""
    report = TombstoneIdentityAudit(("game:g1",), (), ("game:g1",))
    with (
        patch("src.cli.rag.audit_rag_tombstones.get_rag_index_session", return_value=_session_context()),
        patch("src.cli.rag.audit_rag_tombstones.audit_tombstone_session", return_value=report),
    ):
        assert main(["--fail-on-unexplained"]) == 1
