from scripts.maintenance import audit_pa_formula


def _stub_auto_fix_dependencies(monkeypatch):
    monkeypatch.setattr(audit_pa_formula, "_get_violation_game_ids", lambda _year: ["G1"])
    monkeypatch.setattr(audit_pa_formula, "_apply_pbp_fixes", lambda _game_ids: ["G1"])
    monkeypatch.setattr(audit_pa_formula, "fix_year_formula", lambda _year: 0)
    monkeypatch.setattr(audit_pa_formula, "_recalc_and_sync", lambda _year, _game_ids: None)


def test_auto_fix_recalculates_local_stats(monkeypatch) -> None:
    _stub_auto_fix_dependencies(monkeypatch)
    assert audit_pa_formula.auto_fix_year(2020) == 1
