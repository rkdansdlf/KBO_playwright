from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_DIR = ROOT / ".github/workflows"


def _workflow_files() -> list[Path]:
    return sorted(WORKFLOW_DIR.glob("*.yml"))


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _job_blocks(workflow: str):
    in_jobs = False
    current_job = None
    current_lines = []

    for line in workflow.splitlines():
        if line == "jobs:":
            in_jobs = True
            continue
        if not in_jobs:
            continue
        if line and not line.startswith(" "):
            break
        if (
            line.startswith("  ")
            and not line.startswith("    ")
            and line.rstrip().endswith(":")
            and not line.lstrip().startswith("#")
        ):
            if current_job is not None:
                yield current_job, "\n".join(current_lines)
            current_job = line.strip().removesuffix(":")
            current_lines = [line]
        elif current_job is not None:
            current_lines.append(line)

    if current_job is not None:
        yield current_job, "\n".join(current_lines)


def test_daily_kbo_sync_includes_core_steps():
    workflow = _read(WORKFLOW_DIR / "daily_kbo_sync.yml")

    assert "python3 -m src.cli.run_daily_update" in workflow
    assert "--sync --fix" in workflow
    assert "OCI Freshness Gate" in workflow
    assert "--source-url-env OCI_DB_URL" in workflow
    assert workflow.index("Run Postgame Finalize & Sync") < workflow.index("Compute Standings")
    assert workflow.index("Compute Standings") < workflow.index("OCI Freshness Gate")


def test_daily_kbo_sync_includes_quality_and_gap_report():
    workflow = _read(WORKFLOW_DIR / "daily_kbo_sync.yml")

    assert "Generate Quality Report" in workflow
    assert "Run Gap Report" in workflow
    assert '"--force-notify"' in workflow or "--force-notify" in workflow


def test_daily_kbo_sync_includes_advanced_sync_and_quality_checks():
    workflow = _read(WORKFLOW_DIR / "daily_kbo_sync.yml")

    assert "Run Advanced Daily & Sync" in workflow
    assert "Reference Integrity Gate" in workflow
    assert "Quality Gate (OCI Only)" in workflow
    assert "Completeness Audit" in workflow
    assert "Freshness Gate (Extended Window)" in workflow
    assert "--days 14 --source-url-env OCI_DB_URL" in workflow


def test_workflows_do_not_reference_removed_maintenance_paths():
    for path in _workflow_files():
        workflow = _read(path)
        assert "backfill_advanced_stats.sh" not in workflow, path
        assert "scripts/maintenance/" not in workflow, path


def test_backfill_advanced_stats_uses_supported_cli_flags():
    workflow = _read(WORKFLOW_DIR / "backfill_advanced_stats.yml")

    assert "Resolve Season Year" in workflow
    assert "python3 -m src.cli.hydrate_runtime_from_oci" in workflow
    assert "python3 -m src.cli.backfill_advanced_stats \\" in workflow
    assert '--years "$YEAR"' in workflow
    assert "--series regular" in workflow
    assert "python3 -m src.cli.sync_oci \\" in workflow
    assert "--season-stats" in workflow
    assert 'python3 -m src.cli.backfill_advanced_stats "$YEAR" regular' not in workflow


def test_kbo_automation_recalc_stats_uses_supported_cli_flags_and_syncs():
    workflow = _read(WORKFLOW_DIR / "kbo_automation.yml")
    recalc_start = workflow.index("recalc-stats)")
    recalc_block = workflow[recalc_start : workflow.index(";;", recalc_start)]

    assert "python3 -m src.cli.backfill_advanced_stats \\" in recalc_block
    assert '--years "${YEAR}"' in recalc_block
    assert "--series regular" in recalc_block
    assert "python3 -m src.cli.sync_oci \\" in recalc_block
    assert "--season-stats" in recalc_block
    assert 'python3 -m src.cli.backfill_advanced_stats "${YEAR}" regular' not in recalc_block


def test_local_github_actions_are_used_after_checkout():
    local_actions = ("uses: ./.github/actions/python-env", "uses: ./.github/actions/notify")

    for path in _workflow_files():
        for job_name, job_block in _job_blocks(_read(path)):
            if not any(action in job_block for action in local_actions):
                continue

            step_lines = [
                line.strip()
                for line in job_block.splitlines()
                if line.strip().startswith("- uses:") or line.strip().startswith("- name:")
            ]
            assert step_lines, f"{path.name}:{job_name} has no steps"
            assert step_lines[0] == "- uses: actions/checkout@v4", (
                f"{path.name}:{job_name} must start with actions/checkout@v4"
            )

            first_checkout = job_block.find("uses: actions/checkout@v4")
            first_local_action = min(job_block.find(action) for action in local_actions if action in job_block)
            assert first_checkout < first_local_action, f"{path.name}:{job_name} local action before checkout"

    python_env = _read(ROOT / ".github/actions/python-env/action.yml")
    assert "actions/checkout" not in python_env


def test_daily_kbo_sync_hydrates_fresh_runner_jobs():
    workflow = _read(WORKFLOW_DIR / "daily_kbo_sync.yml")
    jobs = dict(_job_blocks(workflow))

    assert "needs: [finalize, post-process]" in jobs["quality"]
    assert "needs: [finalize, quality]" in jobs["advanced-sync"]

    for job_name in ("post-process", "quality", "advanced-sync"):
        job_block = jobs[job_name]
        assert "hydrate: 'true'" in job_block
        assert "hydrate-year: ${{ needs.finalize.outputs.year }}" in job_block
        assert "hydrate-date: ${{ needs.finalize.outputs.date }}" in job_block

    assert "python3 -m scripts.verification.verify_player_game_stats --exit-code" in workflow
    assert "--player-game-stats \\\n            --year ${{ needs.finalize.outputs.year }}" in workflow
    assert "--player-game-stats \\\n            --date" not in workflow
