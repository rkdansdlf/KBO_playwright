from __future__ import annotations

import pytest

import src.cli.monthly_pa_audit as monthly_pa_audit
import src.cli.monthly_unified_audit as monthly_unified_audit


def test_monthly_pa_audit_calls_fix_year_formula_directly(monkeypatch):
    calls: list[tuple[int, bool]] = []

    def _fix_year_formula(year: int, dry_run: bool = False) -> int:
        calls.append((year, dry_run))
        return 7

    monkeypatch.setattr(monthly_pa_audit, "fix_year_formula", _fix_year_formula)

    assert monthly_pa_audit.run_monthly_pa_audit(2025) == 7
    assert calls == [(2025, False)]


def test_monthly_pa_audit_cli_exits_nonzero_on_direct_call_failure(monkeypatch, capsys):
    def _fix_year_formula(_year: int, dry_run: bool = False) -> int:
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(monthly_pa_audit, "fix_year_formula", _fix_year_formula)

    with pytest.raises(SystemExit) as exc:
        monthly_pa_audit.main(["--year", "2025"])

    assert exc.value.code == 1
    assert "PA formula audit failed" in capsys.readouterr().out


def test_monthly_unified_run_pa_fix_calls_fix_year_formula_directly(monkeypatch):
    calls: list[tuple[int, bool]] = []

    def _fix_year_formula(year: int, dry_run: bool = False) -> int:
        calls.append((year, dry_run))
        return 3

    monkeypatch.setattr(monthly_unified_audit, "fix_year_formula", _fix_year_formula)

    result = monthly_unified_audit.run_pa_fix(2025, dry_run=True)

    assert result["ok"] is True
    assert result["fixed_rows"] == 3
    assert calls == [(2025, True)]


def test_monthly_unified_run_pa_audit_calls_audit_year_directly(monkeypatch):
    calls: list[int] = []

    def _audit_year(year: int) -> dict:
        calls.append(year)
        return {"year": year, "violation_rows": 2, "violation_games": 1}

    monkeypatch.setattr(monthly_unified_audit, "audit_year", _audit_year)

    result = monthly_unified_audit.run_pa_audit(2025)

    assert result == {
        "year": 2025,
        "ok": False,
        "violation_count": 2,
        "violations": {"year": 2025, "violation_rows": 2, "violation_games": 1},
    }
    assert calls == [2025]


def test_monthly_unified_pa_helpers_return_failure_shapes(monkeypatch):
    monkeypatch.setattr(
        monthly_unified_audit,
        "fix_year_formula",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("fix failed")),
    )
    monkeypatch.setattr(
        monthly_unified_audit,
        "audit_year",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("audit failed")),
    )

    fix_result = monthly_unified_audit.run_pa_fix(2025)
    audit_result = monthly_unified_audit.run_pa_audit(2025)

    assert fix_result == {"ok": False, "error": "fix failed", "fixed_rows": 0}
    assert audit_result == {
        "year": 2025,
        "ok": False,
        "error": "audit failed",
        "violation_count": 0,
        "violations": [],
    }
