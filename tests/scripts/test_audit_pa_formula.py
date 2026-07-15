from unittest.mock import patch

from scripts.maintenance import audit_pa_formula


def _stub_auto_fix_dependencies(monkeypatch):
    monkeypatch.setattr(audit_pa_formula, "_get_violation_game_ids", lambda _year: ["G1"])
    monkeypatch.setattr(audit_pa_formula, "_apply_pbp_fixes", lambda _game_ids: ["G1"])
    monkeypatch.setattr(audit_pa_formula, "fix_year_formula", lambda _year: 0)
    monkeypatch.setattr(audit_pa_formula, "_recalc_and_sync", lambda _year, _game_ids: None)


def test_auto_fix_skips_oci_sync_by_default(monkeypatch) -> None:
    _stub_auto_fix_dependencies(monkeypatch)
    with patch.object(audit_pa_formula, "_sync_corrected_to_oci") as sync:
        assert audit_pa_formula.auto_fix_year(2020) == 1
    sync.assert_not_called()


def test_auto_fix_syncs_to_oci_only_when_requested(monkeypatch) -> None:
    _stub_auto_fix_dependencies(monkeypatch)
    with patch.object(audit_pa_formula, "_sync_corrected_to_oci") as sync:
        assert audit_pa_formula.auto_fix_year(2020, sync_oci=True) == 1
    sync.assert_called_once_with(2020, ["G1"])
