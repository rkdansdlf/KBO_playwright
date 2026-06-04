from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_daily_kbo_sync_hard_fails_oci_freshness_gate():
    workflow = (ROOT / ".github/workflows/daily_kbo_sync.yml").read_text(encoding="utf-8")

    assert "python3 -m src.cli.run_daily_update --date ${{ env.KST_DATE }} --sync --fix" in workflow
    assert "Run OCI Freshness Gate" in workflow
    assert 'python3 -m src.cli.freshness_gate --date "${KST_DATE}" --source-url-env OCI_DB_URL' in workflow
    assert workflow.index("Run Postgame Finalize & Sync") < workflow.index("Run OCI Freshness Gate")
    assert "continue-on-error" not in workflow[workflow.index("Run OCI Freshness Gate") :]


def test_quality_check_runs_oci_wpa_freshness_lookback():
    workflow = (ROOT / ".github/workflows/quality_check.yml").read_text(encoding="utf-8")

    assert "Run WPA Freshness Gate (OCI Only)" in workflow
    assert "python3 -m src.cli.freshness_gate --days 14 --source-url-env OCI_DB_URL" in workflow
