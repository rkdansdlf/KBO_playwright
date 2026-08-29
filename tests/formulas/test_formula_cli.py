"""CLI Command Execution Tests for 'kbo formula'."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.cli.formula import main
from src.formulas.models import FormulaAuditReport


def test_cli_list(capsys) -> None:
    """Test 'kbo formula list' stdout output."""
    rc = main(["list"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "KBO SABERMETRICS FORMULA REGISTRY CATALOG" in captured.out
    assert "wOBA" in captured.out
    assert "AVG" in captured.out


def test_cli_list_json(capsys) -> None:
    """Test 'kbo formula list --json' stdout output."""
    rc = main(["list", "--json", "--category", "BATTING"])
    assert rc == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert isinstance(data, list)
    assert len(data) >= 15


def test_cli_explain(capsys) -> None:
    """Test 'kbo formula explain wOBA'."""
    rc = main(["explain", "wOBA", "--season", "2024"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "Metric Specification: wOBA" in captured.out
    assert "Mathematical Formulation" in captured.out
    assert "Environmental Constants" in captured.out


def test_cli_audit_with_artifact(tmp_path: Path, capsys) -> None:
    """Test 'kbo formula audit --save-artifact'."""
    art_path = tmp_path / "formula_report.json"

    mock_report = FormulaAuditReport(
        audit_mode="SEASON",
        season=2024,
        category="BATTING",
        total_metrics_evaluated=16,
        total_entities_checked=50,
        reproducible_count=50,
        divergent_count=0,
        reproducibility_ratio=1.0,
        metric_breakdowns={
            "AVG": {"evaluations": 50, "reproducible": 50, "divergent": 0, "reproducibility_ratio": 1.0}
        },
        duration_ms=12.5,
        is_compliant=True,
        git_sha="abcdef1",
        generated_at_utc="2026-08-30T00:00:00Z",
        sha256_checksum="mock_sha256",
    )

    with patch("src.cli.formula.FormulaEngine.audit_reproducibility", return_value=mock_report):
        rc = main(["audit", "--season", "2024", "--sample", "5", "--save-artifact", str(art_path)])
        assert rc == 0
        assert art_path.exists()
        data = json.loads(art_path.read_text(encoding="utf-8"))
        assert data["total_metrics_evaluated"] == 16
        assert data["sha256_checksum"] == "mock_sha256"
