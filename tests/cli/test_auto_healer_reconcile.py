"""Tests for auto_healer quarantine reconciliation wiring (BLOCKER-03a)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import src.cli.auto_healer as auto_healer


def _session_factory() -> tuple[MagicMock, MagicMock]:
    session = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = session
    context.__exit__.return_value = False
    return MagicMock(return_value=context), session


def _quarantine(
    game_id: str = "20250615KIALG0",
    entity_type: str = "batting",
    entity_id: str = "42",
    rule_id: str = "BAT-001",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=7,
        game_id=game_id,
        entity_type=entity_type,
        entity_id=entity_id,
        rule_id=rule_id,
        status="PENDING",
        source="kbo_official",
        raw_payload={"hits": 3, "at_bats": 4},
    )


def _query_result(records: list) -> MagicMock:
    result = MagicMock()
    result.scalars.return_value.all.return_value = records
    return result


def test_reconcile_no_pending_records_returns_zero() -> None:
    """No PENDING quarantine records should short-circuit with 0."""
    session_factory, session = _session_factory()
    session.execute.return_value = _query_result([])
    with patch("src.cli.auto_healer.SessionLocal", session_factory):
        assert auto_healer.reconcile_pending_quarantines() == 0
    session_factory.assert_called_once()


def test_reconcile_heals_batting_record_with_evidence() -> None:
    """A PENDING batting record with matching evidence should be healed once."""
    session_factory, session = _session_factory()
    qr = _quarantine()
    session.execute.return_value = _query_result([qr])

    reconciler_instance = MagicMock()
    reconciler_instance.reconcile_and_heal_quarantine.return_value = True

    with (
        patch("src.cli.auto_healer.SessionLocal", session_factory),
        patch("src.cli.auto_healer._build_secondary_evidence", return_value={"hits": 3, "at_bats": 4}),
        patch("src.services.multi_source_reconciler.MultiSourceReconciler", return_value=reconciler_instance),
    ):
        assert auto_healer.reconcile_pending_quarantines() == 1

    reconciler_instance.reconcile_and_heal_quarantine.assert_called_once_with(session, 7, {"hits": 3, "at_bats": 4})
    session.commit.assert_called_once()


def test_reconcile_skips_record_without_secondary_evidence() -> None:
    """Records with no usable secondary evidence should be left untouched."""
    session_factory, session = _session_factory()
    qr = _quarantine()
    session.execute.return_value = _query_result([qr])

    reconciler_instance = MagicMock()

    with (
        patch("src.cli.auto_healer.SessionLocal", session_factory),
        patch("src.cli.auto_healer._build_secondary_evidence", return_value={}),
        patch("src.services.multi_source_reconciler.MultiSourceReconciler", return_value=reconciler_instance),
    ):
        assert auto_healer.reconcile_pending_quarantines() == 0

    reconciler_instance.reconcile_and_heal_quarantine.assert_not_called()


def test_reconcile_filters_by_game_ids() -> None:
    """The game_ids filter should be applied to the PENDING query."""
    session_factory, session = _session_factory()
    session.execute.return_value = _query_result([])

    with patch("src.cli.auto_healer.SessionLocal", session_factory):
        assert auto_healer.reconcile_pending_quarantines(game_ids=["20250615KIALG0"]) == 0

    stmt = session.execute.call_args.args[0]
    compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
    assert "20250615KIALG0" in compiled
    assert "PENDING" in compiled


def test_reconcile_rolls_back_on_reconciler_error() -> None:
    """A reconciler exception should roll back and continue with other records."""
    session_factory, session = _session_factory()
    qr_bad = _quarantine(entity_id="1")
    qr_good = _quarantine(entity_id="2")
    session.execute.return_value = _query_result([qr_bad, qr_good])

    reconciler_instance = MagicMock()
    reconciler_instance.reconcile_and_heal_quarantine.side_effect = [ValueError("boom"), True]

    with (
        patch("src.cli.auto_healer.SessionLocal", session_factory),
        patch("src.cli.auto_healer._build_secondary_evidence", return_value={"hits": 3, "at_bats": 4}),
        patch("src.services.multi_source_reconciler.MultiSourceReconciler", return_value=reconciler_instance),
    ):
        assert auto_healer.reconcile_pending_quarantines() == 1

    session.rollback.assert_called_once()
    assert session.commit.call_count == 1


def test_build_secondary_evidence_non_batting_returns_empty() -> None:
    """Non-batting entities have no usable game_batting_stats evidence."""
    session = MagicMock()
    qr = _quarantine(entity_type="game")
    assert auto_healer._build_secondary_evidence(session, qr) == {}
    session.execute.assert_not_called()


def test_build_secondary_evidence_missing_row_returns_empty() -> None:
    """A batting record with no matching game_batting_stats row yields no evidence."""
    session = MagicMock()
    session.execute.return_value.mappings.return_value.first.return_value = None
    assert auto_healer._build_secondary_evidence(session, _quarantine()) == {}


def test_build_secondary_evidence_returns_scalar_columns() -> None:
    """A batting record with a matching row returns non-null stat columns."""
    session = MagicMock()
    row = MagicMock()
    row.items.return_value = [
        ("hits", 3),
        ("at_bats", 4),
        ("plate_appearances", 4),
        ("home_runs", 1),
        ("walks", 0),
    ]
    session.execute.return_value.mappings.return_value.first.return_value = row
    evidence = auto_healer._build_secondary_evidence(session, _quarantine())
    assert evidence == {"hits": 3, "at_bats": 4, "plate_appearances": 4, "home_runs": 1, "walks": 0}
