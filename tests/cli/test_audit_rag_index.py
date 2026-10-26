"""Tests for the read-only RAG index audit CLI contract."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from src.cli.audit_rag_index import main
from src.services.rag_index_consistency import IndexConsistencyFinding, IndexConsistencyReport


def test_audit_returns_infrastructure_code_when_pgvector_is_unavailable(capsys) -> None:
    """Return exit code 2 when the vector dependency cannot be reached."""
    with patch("src.cli.audit_rag_index.is_pgvector_available", return_value=False):
        assert main(["--json"]) == 2
    assert "pgvector is unavailable" in capsys.readouterr().out


def test_audit_returns_zero_for_consistent_indexes(capsys) -> None:
    """Return exit code 0 for an empty but successfully queried pair of indexes."""
    report = IndexConsistencyReport(0, 0, (), total_keys=0)
    with (
        patch("src.cli.audit_rag_index.is_pgvector_available", return_value=True),
        patch("src.cli.audit_rag_index.get_rag_index_session") as primary,
        patch("src.cli.audit_rag_index.get_vector_session") as vector,
        patch("src.cli.audit_rag_index.audit_index_sessions", return_value=report),
    ):
        primary.return_value.__enter__.return_value = MagicMock()
        vector.return_value.__enter__.return_value = MagicMock()
        assert main(["--json"]) == 0
    assert '"consistent": true' in capsys.readouterr().out


def test_audit_returns_one_for_findings(capsys) -> None:
    """Return exit code 1 when both indexes are reachable but inconsistent."""
    report = IndexConsistencyReport(1, 0, (IndexConsistencyFinding("game:g1", "MISSING_IN_VECTOR"),), total_keys=1)
    with (
        patch("src.cli.audit_rag_index.is_pgvector_available", return_value=True),
        patch("src.cli.audit_rag_index.get_rag_index_session") as primary,
        patch("src.cli.audit_rag_index.get_vector_session") as vector,
        patch("src.cli.audit_rag_index.audit_index_sessions", return_value=report),
    ):
        primary.return_value.__enter__.return_value = MagicMock()
        vector.return_value.__enter__.return_value = MagicMock()
        assert main(["--json"]) == 1
    assert '"consistent": false' in capsys.readouterr().out


def test_audit_require_nonempty_rejects_empty_indexes(capsys) -> None:
    """Require an actual corpus for the production acceptance gate."""
    report = IndexConsistencyReport(0, 0, (), total_keys=0)
    with (
        patch("src.cli.audit_rag_index.is_pgvector_available", return_value=True),
        patch("src.cli.audit_rag_index.get_rag_index_session") as primary,
        patch("src.cli.audit_rag_index.get_vector_session") as vector,
        patch("src.cli.audit_rag_index.audit_index_sessions", return_value=report),
    ):
        primary.return_value.__enter__.return_value = MagicMock()
        vector.return_value.__enter__.return_value = MagicMock()
        assert main(["--require-nonempty", "--json"]) == 1
    assert "RAG index is empty" in capsys.readouterr().out


def _oracle_audit_context(report):
    return (
        patch("src.cli.audit_rag_index.is_pgvector_available", return_value=True),
        patch("src.cli.audit_rag_index.is_oracle_vector_backend", return_value=True),
        patch("src.cli.audit_rag_index.get_rag_index_session"),
        patch("src.cli.audit_rag_index.audit_single_store_session", return_value=report),
    )


def test_audit_reports_postings_coverage_without_failing_by_default(capsys) -> None:
    """Report the sparse postings gap even when the default gate stays green."""
    report = IndexConsistencyReport(1, 1, (), total_keys=1)
    patches = _oracle_audit_context(report)
    with patches[0], patches[1], patches[2] as primary, patches[3]:
        primary.return_value.__enter__.return_value = MagicMock()
        with patch("src.cli.audit_rag_index._count_missing_postings", return_value=7):
            assert main(["--json"]) == 0
    out = capsys.readouterr().out
    assert '"postings_missing": 7' in out
    assert '"consistent": true' in out


def test_audit_require_postings_fails_on_coverage_gap(capsys) -> None:
    """Fail the audit when retrievable chunks lack sparse postings."""
    report = IndexConsistencyReport(1, 1, (), total_keys=1)
    patches = _oracle_audit_context(report)
    with patches[0], patches[1], patches[2] as primary, patches[3]:
        primary.return_value.__enter__.return_value = MagicMock()
        with patch("src.cli.audit_rag_index._count_missing_postings", return_value=7):
            assert main(["--require-postings", "--json"]) == 1
    out = capsys.readouterr().out
    assert "missing sparse postings" in out
    assert '"consistent": false' in out


def test_audit_require_postings_passes_when_fully_covered(capsys) -> None:
    """Pass the postings gate when every retrievable chunk is indexed."""
    report = IndexConsistencyReport(1, 1, (), total_keys=1)
    patches = _oracle_audit_context(report)
    with patches[0], patches[1], patches[2] as primary, patches[3]:
        primary.return_value.__enter__.return_value = MagicMock()
        with patch("src.cli.audit_rag_index._count_missing_postings", return_value=0):
            assert main(["--require-postings", "--json"]) == 0
    assert '"postings_missing": 0' in capsys.readouterr().out


def _colocated_audit_context(report):
    return (
        patch("src.cli.audit_rag_index.is_pgvector_available", return_value=True),
        patch("src.cli.audit_rag_index.is_oracle_vector_backend", return_value=False),
        patch("src.cli.audit_rag_index.shares_operational_database", return_value=True),
        patch("src.cli.audit_rag_index.get_rag_index_session"),
        patch("src.cli.audit_rag_index.get_vector_session"),
        patch("src.cli.audit_rag_index.audit_single_store_session", return_value=report),
        patch("src.cli.audit_rag_index.audit_index_sessions"),
    )


def test_audit_reads_a_colocated_store_once_instead_of_comparing_it_with_itself(capsys) -> None:
    """One database must not be loaded twice to compare itself with itself.

    Once ``PGVECTOR_URL`` resolves to the operational database, both ORM models
    map the same ``rag_chunks`` table. The paired audit would then report every
    finding twice and scan 606k rows twice, so the single-store path has to win.
    """
    report = IndexConsistencyReport(1, 1, (), total_keys=1)
    patches = _colocated_audit_context(report)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3] as primary,
        patches[4] as vector,
        patches[5] as single,
        patches[6] as paired,
    ):
        primary.return_value.__enter__.return_value = MagicMock()
        assert main(["--json"]) == 0
    single.assert_called_once()
    paired.assert_not_called()
    vector.assert_not_called()
    assert '"consistent": true' in capsys.readouterr().out


def test_audit_skips_the_postings_scan_on_a_colocated_store(capsys) -> None:
    """PostgreSQL retrieves through ``content_tsv``; its 27M-row terms table is not the gate.

    ``rag_chunk_terms`` holds 27,761,000 rows / 13 GB and has no leading
    ``rag_chunk_id`` index, so counting missing postings there exceeds the
    scheduler's window. The Oracle gate stays unchanged.
    """
    report = IndexConsistencyReport(1, 1, (), total_keys=1)
    patches = _colocated_audit_context(report)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3] as primary,
        patches[4],
        patches[5],
        patches[6],
        patch("src.cli.audit_rag_index._count_missing_postings") as postings,
    ):
        primary.return_value.__enter__.return_value = MagicMock()
        assert main(["--require-postings", "--json"]) == 0
    postings.assert_not_called()
    payload = capsys.readouterr().out
    assert "postings_missing" not in payload


def _run_audit(report: IndexConsistencyReport, argv: list[str]) -> int:
    """Run the audit against a report, without touching a database."""
    with (
        patch("src.cli.audit_rag_index.is_pgvector_available", return_value=True),
        patch("src.cli.audit_rag_index.get_rag_index_session") as primary,
        patch("src.cli.audit_rag_index.get_vector_session") as vector,
        patch("src.cli.audit_rag_index.audit_index_sessions", return_value=report),
    ):
        primary.return_value.__enter__.return_value = MagicMock()
        vector.return_value.__enter__.return_value = MagicMock()
        return main(argv)


class TestWorkInProgressIsNotDamage:
    """A chunk that is not written yet is behind, not wrong.

    The distinction is what keeps a daily gate readable while a crawl backlog
    drains. Reported as one number, an unfinished index warns every day until it
    is finished, and a warning that is always on is one nobody reads.
    """

    def test_a_chunk_awaiting_its_embedding_leaves_the_gate_open(self, capsys) -> None:
        report = IndexConsistencyReport(
            10,
            10,
            (IndexConsistencyFinding("game:g1", "VECTOR_EMBEDDING_MISSING"),),
            total_keys=10,
        )

        assert _run_audit(report, ["--json"]) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["damage"] == 0
        assert payload["incomplete"] == 1
        assert payload["consistent"] is True
        assert payload["fully_consistent"] is False

    def test_a_row_awaiting_its_reindex_leaves_the_gate_open(self, capsys) -> None:
        report = IndexConsistencyReport(10, 10, (), total_keys=10, stale_keys=("game:g1",))

        assert _run_audit(report, ["--json"]) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["incomplete"] == 1
        assert payload["damage"] == 0

    def test_the_caller_can_still_demand_completeness(self, capsys) -> None:
        report = IndexConsistencyReport(
            10,
            10,
            (IndexConsistencyFinding("game:g1", "VECTOR_EMBEDDING_MISSING"),),
            total_keys=10,
        )

        assert _run_audit(report, ["--require-complete", "--json"]) == 1
        payload = json.loads(capsys.readouterr().out)
        assert "not fully indexed yet" in payload["error"]
        assert payload["consistent"] is False

    def test_the_old_flag_name_still_demands_completeness(self) -> None:
        report = IndexConsistencyReport(
            10,
            10,
            (IndexConsistencyFinding("game:g1", "VECTOR_EMBEDDING_MISSING"),),
            total_keys=10,
        )

        assert _run_audit(report, ["--fail-on-findings", "--json"]) == 1

    def test_disagreeing_stores_fail_whatever_the_flags_say(self, capsys) -> None:
        # The index serving a row that says one thing while the vector says
        # another is wrong now. Waiting does not fix it, so no flag may excuse it.
        report = IndexConsistencyReport(
            10,
            10,
            (
                IndexConsistencyFinding("game:g1", "VECTOR_EMBEDDING_MISSING"),
                IndexConsistencyFinding("game:g2", "CONTENT_HASH_MISMATCH"),
            ),
            total_keys=10,
        )

        assert _run_audit(report, ["--json"]) == 1
        payload = json.loads(capsys.readouterr().out)
        assert payload["damage"] == 1
        assert payload["incomplete"] == 1
        assert "disagree" in payload["error"]

    def test_the_gate_verdict_and_the_finished_state_are_reported_separately(self, capsys) -> None:
        report = IndexConsistencyReport(10, 10, (), total_keys=10)

        assert _run_audit(report, ["--json"]) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["consistent"] is True
        assert payload["fully_consistent"] is True
