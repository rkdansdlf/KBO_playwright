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
    workflow = _read(WORKFLOW_DIR / "backfill.yml")

    assert "python3 -m src.cli.backfill_advanced_stats" in workflow
    assert '--years "$YEAR"' in workflow
    assert "--series regular" in workflow
    assert "python3 -m src.cli.sync_oci" in workflow
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
    job_setup_actions = ("uses: ./.github/actions/kbo-job-setup",)

    for path in _workflow_files():
        for job_name, job_block in _job_blocks(_read(path)):
            if not any(action in job_block for action in (*local_actions, *job_setup_actions)):
                continue

            step_lines = [
                line.strip()
                for line in job_block.splitlines()
                if line.strip().startswith("- uses:") or line.strip().startswith("- name:")
            ]
            assert step_lines, f"{path.name}:{job_name} has no steps"

            first_step = step_lines[0]
            uses_kbo_setup = "uses: ./.github/actions/kbo-job-setup" in first_step
            uses_checkout = first_step == "- uses: actions/checkout@v4"
            assert uses_kbo_setup or uses_checkout, (
                f"{path.name}:{job_name} must start with actions/checkout@v4 or kbo-job-setup"
            )

            if uses_checkout:
                first_checkout = job_block.find("uses: actions/checkout@v4")
                first_local_action = min(job_block.find(action) for action in local_actions if action in job_block)
                assert first_checkout < first_local_action, f"{path.name}:{job_name} local action before checkout"

    python_env = _read(ROOT / ".github/actions/python-env/action.yml")
    assert "actions/checkout" not in python_env

    kbo_setup = _read(ROOT / ".github/actions/kbo-job-setup/action.yml")
    assert "actions/checkout" in kbo_setup


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

    assert "python3 -m scripts.verification.verify_player_game_stats \\" in workflow
    assert "--date ${{ needs.finalize.outputs.date }}" in workflow
    assert "--exit-code" in workflow
    assert "--player-game-stats \\\n            --year ${{ needs.finalize.outputs.year }}" in workflow
    assert "--player-game-stats \\\n            --date" not in workflow


def test_daily_preview_uses_correct_cli_and_hydration():
    workflow = _read(WORKFLOW_DIR / "daily_preview.yml")

    assert "python3 -m src.cli.hydrate_runtime_from_oci" in workflow
    assert "--year \"${{ env.KST_YEAR }}\" --date \"${{ env.KST_DATE }}\"" in workflow
    assert "python3 -m src.cli.daily_preview_batch --date \"${{ env.KST_DATE }}\"" in workflow
    assert "KST_DATE" in workflow
    assert "KST_YEAR" in workflow
    assert workflow.index("Hydrate Runtime Cache From OCI") < workflow.index("Run Daily Preview Batch")
    assert "if: always()" in workflow


def test_pitcher_backfill_uses_correct_cli_and_hydration():
    workflow = _read(WORKFLOW_DIR / "pitcher_backfill.yml")

    assert "python3 -m src.cli.hydrate_runtime_from_oci" in workflow
    assert '--year "${KST_YEAR}" --date "${KST_DATE}"' in workflow
    assert "python3 -m src.cli.backfill_pregame_previews" in workflow
    assert '--days-ahead "${DAYS_AHEAD}"' in workflow
    assert "DAYS_AHEAD" in workflow
    assert "KST_DATE" in workflow
    assert "KST_YEAR" in workflow
    assert workflow.index("Hydrate Runtime Cache From OCI") < workflow.index("Run Pregame Backfill")


def test_security_audit_uses_pip_audit():
    workflow = _read(WORKFLOW_DIR / "security_audit.yml")

    assert "pip-audit" in workflow
    assert "--requirement requirements.txt" in workflow
    assert "--desc on" in workflow
    assert "continue-on-error: true" in workflow
    assert "timeout-minutes: 10" in workflow
    assert "Dependency Security Audit" in workflow
    assert "actions/setup-python@v5" in workflow


def test_test_suite_runs_lint_and_test_matrix():
    workflow = _read(WORKFLOW_DIR / "test_suite.yml")

    assert "ruff check" in workflow
    assert "ruff format --check" in workflow
    assert "scripts/lint_bare_except.py" in workflow
    assert "pytest --tb=short -v --durations=10" in workflow
    assert "matrix:" in workflow
    assert 'python-version: ["3.12"]' in workflow
    assert "cancel-in-progress: true" in workflow
    assert "concurrency:" in workflow
    assert "timeout-minutes: 30" in workflow

    jobs = dict(_job_blocks(workflow))
    assert "lint" in jobs
    assert "test" in jobs
    assert workflow.index("  lint:\n") < workflow.index("  test:\n")


def test_docker_build_has_full_build_chain():
    workflow = _read(WORKFLOW_DIR / "docker_build.yml")

    assert "docker/setup-qemu-action@v3" in workflow
    assert "docker/setup-buildx-action@v3" in workflow
    assert "docker/login-action@v3" in workflow
    assert "docker/metadata-action@v5" in workflow
    assert "docker/build-push-action@v6" in workflow
    assert "ghcr.io" in workflow
    assert "secrets.GITHUB_TOKEN" in workflow
    assert "packages: write" in workflow
    assert "type=gha" in workflow

    step_order = [
        "Set up QEMU",
        "Set up Docker Buildx",
        "Login to GHCR",
        "Generate tags",
        "Build and push",
    ]
    prev_idx = -1
    for step in step_order:
        idx = workflow.index(step)
        assert idx > prev_idx, f"{step} out of order"
        prev_idx = idx


def test_weekly_maintenance_uses_correct_cli_and_env():
    workflow = _read(WORKFLOW_DIR / "weekly_maintenance.yml")

    assert "python3 -m src.cli.run_weekly_maintenance" in workflow
    assert "--profile-limit" in workflow
    assert "--sync" in workflow
    assert "YOUTUBE_API_KEY" in workflow
    assert "NAVER_CLIENT_ID" in workflow
    assert "NAVER_CLIENT_SECRET" in workflow
    assert "OCI_DB_URL" in workflow
    assert workflow.index("Run Weekly Maintenance & Sync") < workflow.index("uses: ./.github/actions/notify")


def test_periodic_extras_runs_unified_audit_twice():
    workflow = _read(WORKFLOW_DIR / "periodic_extras.yml")

    assert "python3 -m src.cli.run_periodic_extras" in workflow
    assert "--year" in workflow
    assert "--sync" in workflow
    assert "python3 -m src.cli.monthly_unified_audit --year \"$PREV_YEAR\"" in workflow
    assert "python3 -m src.cli.monthly_unified_audit --year \"$YEAR\"" in workflow
    assert workflow.index("Run Periodic Extras & Sync") < workflow.index("Monthly Unified Audit")


def test_full_recalculation_full_pipeline():
    workflow = _read(WORKFLOW_DIR / "full_recalculation.yml")

    assert "python3 -m src.cli.recalc_season_stats" in workflow
    assert "--year ${{ github.event.inputs.year }}" in workflow
    assert "--series ${{ github.event.inputs.series }}" in workflow
    assert "--save" in workflow
    assert "python3 -m src.cli.recalc_player_game_stats" in workflow
    assert "--season ${{ github.event.inputs.year }}" in workflow
    assert "python3 -m src.cli.sync_oci --season-stats --player-game-stats" in workflow
    assert "${{ github.event.inputs.sync == 'true' }}" in workflow
    assert "python3 -m scripts.verification.verify_player_game_stats --exit-code" in workflow
    assert "if: always()" in workflow
    assert "concurrency:" in workflow
