from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from src.monitoring.failure_diagnosis import diagnose_text, render_diagnosis_text

ROOT = Path(__file__).resolve().parents[1]


def test_diagnose_text_classifies_common_crawler_failures() -> None:
    log_text = """
    playwright._impl._errors.TimeoutError: Timeout 30000ms exceeded while waiting for selector ".tblSchedule"
    sqlalchemy.exc.IntegrityError: FOREIGN KEY constraint failed
    quality gate failed: batting_null_player_id exceeds baseline
    LIVE_LOCK already held, skipping live refresh
    """

    report = diagnose_text(log_text, source="daily.log")
    categories = {finding.category for finding in report.findings}

    assert {"selector", "database", "quality_gate", "scheduler_lock"}.issubset(categories)
    assert report.highest_severity == "high"
    assert report.exit_code == 1
    assert any("crawler_selector_gate" in command for command in report.suggested_commands)


def test_render_diagnosis_text_includes_evidence_and_commands() -> None:
    report = diagnose_text("httpx.ConnectTimeout: timed out while connecting to koreabaseball.com")

    output = render_diagnosis_text(report)

    assert "network" in output
    assert "httpx.ConnectTimeout" in output
    assert "Suggested commands" in output


def test_diagnose_crawler_failure_cli_reads_logs_and_emits_json(tmp_path: Path) -> None:
    log_path = tmp_path / "crawler.log"
    log_path.write_text(
        "KBO authentication failed: invalid KBO_USER_ID\n"
        "playwright._impl._errors.Error: strict mode violation for locator('.team')\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "src.cli.diagnose_crawler_failure",
            "--json",
            str(log_path),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    categories = {finding["category"] for finding in payload["findings"]}
    assert {"auth", "selector"}.issubset(categories)
    assert payload["source_count"] == 1
