from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.cli.crawler_selector_gate import main as crawler_selector_gate_main
from src.monitoring.crawler_selector_gate import (
    SelectorCheck,
    SelectorTarget,
    evaluate_html_target,
    load_selector_config,
    run_selector_gate,
)


def test_evaluate_html_target_passes_when_required_selectors_match() -> None:
    html = """
    <html>
      <body>
        <table class="schedule">
          <tbody>
            <tr><td class="team">LG</td><td>SSG</td></tr>
            <tr><td class="team">KT</td><td>NC</td></tr>
          </tbody>
        </table>
      </body>
    </html>
    """
    target = SelectorTarget(
        name="schedule_fixture",
        source="inline",
        source_type="inline",
        checks=[
            SelectorCheck(name="game_rows", selector="table.schedule tbody tr", min_count=2),
            SelectorCheck(name="team_label", selector=".team", min_count=1, required_text="LG"),
        ],
    )

    result = evaluate_html_target(target, html)

    assert result.ok is True
    assert result.issue_count == 0
    assert result.checks["game_rows"]["count"] == 2


def test_evaluate_html_target_reports_missing_selector() -> None:
    target = SelectorTarget(
        name="profile_fixture",
        source="inline",
        source_type="inline",
        checks=[SelectorCheck(name="profile_photo", selector=".player-photo", min_count=1)],
    )

    result = evaluate_html_target(target, "<html><body><h1>No profile</h1></body></html>")

    assert result.ok is False
    assert result.issue_count == 1
    assert result.issues[0].category == "selector_missing"
    assert result.issues[0].selector == ".player-photo"


def test_run_selector_gate_loads_file_config_and_writes_json_report(tmp_path: Path) -> None:
    html_path = tmp_path / "schedule.html"
    html_path.write_text("<table><tbody><tr><td>LG</td></tr></tbody></table>", encoding="utf-8")
    config_path = tmp_path / "selector_gate.json"
    config_path.write_text(
        json.dumps(
            {
                "targets": [
                    {
                        "name": "schedule",
                        "source": str(html_path),
                        "source_type": "file",
                        "checks": [
                            {
                                "name": "rows",
                                "selector": "tbody tr",
                                "min_count": 1,
                                "required_text": "LG",
                            },
                        ],
                    },
                ],
            },
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "selector-output"

    targets = load_selector_config(config_path)
    summary = run_selector_gate(targets, output_dir=output_dir)

    assert summary.ok is True
    report_path = output_dir / "selector_gate_report.json"
    assert report_path.exists()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["ok"] is True
    assert payload["targets"][0]["target"] == "schedule"


def test_crawler_selector_gate_cli_emits_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    html_path = tmp_path / "fixture.html"
    html_path.write_text("<main><span class='score'>3:2</span></main>", encoding="utf-8")
    config_path = tmp_path / "selector_gate.json"
    config_path.write_text(
        json.dumps(
            {
                "targets": [
                    {
                        "name": "scoreboard",
                        "source": str(html_path),
                        "source_type": "file",
                        "checks": [{"name": "score", "selector": ".score", "min_count": 1}],
                    },
                ],
            },
        ),
        encoding="utf-8",
    )

    exit_code = crawler_selector_gate_main(
        [
            "--config",
            str(config_path),
            "--json",
        ],
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["ok"] is True
    assert payload["target_count"] == 1
