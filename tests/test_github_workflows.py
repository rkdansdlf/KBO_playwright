from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_daily_kbo_sync_includes_core_steps():
    workflow = (ROOT / ".github/workflows/daily_kbo_sync.yml").read_text(encoding="utf-8")

    assert "python3 -m src.cli.run_daily_update" in workflow
    assert "--sync --fix" in workflow
    assert "OCI Freshness Gate" in workflow
    assert "--source-url-env OCI_DB_URL" in workflow
    assert workflow.index("Run Postgame Finalize & Sync") < workflow.index("Compute Standings")
    assert workflow.index("Compute Standings") < workflow.index("OCI Freshness Gate")


def test_daily_kbo_sync_includes_quality_and_gap_report():
    workflow = (ROOT / ".github/workflows/daily_kbo_sync.yml").read_text(encoding="utf-8")

    assert "Generate Quality Report" in workflow
    assert "Run Gap Report" in workflow
    assert "\"--force-notify\"" in workflow or "--force-notify" in workflow


def test_daily_kbo_sync_includes_advanced_sync_and_quality_checks():
    workflow = (ROOT / ".github/workflows/daily_kbo_sync.yml").read_text(encoding="utf-8")

    assert "Run Advanced Daily & Sync" in workflow
    assert "Reference Integrity Gate" in workflow
    assert "Quality Gate (OCI Only)" in workflow
    assert "Completeness Audit" in workflow
    assert "Freshness Gate (Extended Window)" in workflow
    assert "--days 14 --source-url-env OCI_DB_URL" in workflow
