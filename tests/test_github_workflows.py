from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_DIR = ROOT / ".github/workflows"
ACTION_DIR = ROOT / ".github/actions"
NODE24_CHECKOUT_REF = "actions/checkout@v5"


def _workflow_files() -> list[Path]:
    return sorted(WORKFLOW_DIR.glob("*.yml"))


def _github_action_files() -> list[Path]:
    return sorted(ACTION_DIR.glob("*/action.y*ml"))


def _github_ci_files() -> list[Path]:
    return _workflow_files() + _github_action_files()


def _joined_ref(*parts: str) -> str:
    return "".join(parts)


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


def _kbo_job_setup_blocks(job_block: str) -> list[str]:
    marker = "uses: ./.github/actions/kbo-job-setup"
    blocks = []
    search_from = 0

    while True:
        marker_idx = job_block.find(marker, search_from)
        if marker_idx == -1:
            return blocks

        step_start = job_block.rfind("\n      - ", 0, marker_idx)
        if step_start == -1:
            step_start = 0
        else:
            step_start += 1

        next_step = job_block.find("\n      - ", marker_idx + len(marker))
        step_end = len(job_block) if next_step == -1 else next_step
        blocks.append(job_block[step_start:step_end])
        search_from = step_end


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


def test_github_ci_does_not_reference_removed_maintenance_paths():
    removed_refs = (
        _joined_ref("scripts", "/legacy"),
        _joined_ref("scripts", ".legacy"),
        _joined_ref("python3 ", "scripts/maintenance/"),
        _joined_ref("backfill_advanced_stats", ".sh"),
    )

    for path in _github_ci_files():
        config = _read(path)
        for removed_ref in removed_refs:
            assert removed_ref not in config, f"{path} references removed path: {removed_ref}"


def test_github_ci_uses_node24_compatible_action_versions():
    node20_action_refs = (
        _joined_ref("actions/checkout", "@v4"),
        _joined_ref("actions/setup-python", "@v5"),
        _joined_ref("actions/cache", "@v4"),
        _joined_ref("docker/setup-qemu-action", "@v3"),
        _joined_ref("docker/setup-buildx-action", "@v3"),
        _joined_ref("docker/login-action", "@v3"),
        _joined_ref("docker/metadata-action", "@v5"),
        _joined_ref("docker/build-push-action", "@v6"),
    )

    for path in _github_ci_files():
        config = _read(path)
        for action_ref in node20_action_refs:
            assert action_ref not in config, f"{path} still uses Node 20 action ref: {action_ref}"
        assert "ACTIONS_ALLOW_USE_UNSECURE_NODE_VERSION" not in config, (
            f"{path} must not opt out of Node 24 with the temporary Node 20 fallback"
        )

    python_env = _read(ACTION_DIR / "python-env/action.yml")
    assert "actions/setup-python@v6" in python_env
    assert "actions/cache@v5" in python_env

    security_audit = _read(WORKFLOW_DIR / "security_audit.yml")
    assert "actions/setup-python@v6" in security_audit

    test_suite = _read(WORKFLOW_DIR / "test_suite.yml")
    assert 'FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: "true"' in test_suite


def test_github_ci_uses_supported_maintenance_modules():
    python_env = _read(ACTION_DIR / "python-env/action.yml")
    daily = _read(WORKFLOW_DIR / "daily_kbo_sync.yml")
    backfill = _read(WORKFLOW_DIR / "backfill.yml")

    assert "python3 -m scripts.maintenance.seed_data" in python_env
    assert "python3 -m scripts.maintenance.resolve_null_player_ids_conservative" in daily
    assert "python3 -m scripts.maintenance.quality_gate" in daily
    assert "from scripts.maintenance.backfill_sh_sf_from_pbp import" in backfill
    assert "python3 -m scripts.maintenance.resolve_null_player_ids_conservative" in backfill
    assert "from scripts.maintenance.backfill_roster_movements import" in backfill


def test_backfill_advanced_stats_uses_supported_cli_flags():
    workflow = _read(WORKFLOW_DIR / "backfill.yml")

    assert "python3 -m src.cli.backfill_advanced_stats" in workflow
    assert '--years "$YEAR"' in workflow
    assert "--series regular" in workflow
    assert "python3 -m src.cli.sync_oci" in workflow
    assert "--season-stats" in workflow
    assert 'python3 -m src.cli.backfill_advanced_stats "$YEAR" regular' not in workflow


def test_backfill_prunes_matrix_before_expensive_setup():
    workflow = _read(WORKFLOW_DIR / "backfill.yml")
    jobs = dict(_job_blocks(workflow))

    assert "select-backfills" in jobs
    assert "backfill" in jobs

    selector = jobs["select-backfills"]
    backfill = jobs["backfill"]

    assert "uses: actions/checkout" not in selector
    assert "uses: ./.github/actions/kbo-job-setup" not in selector
    assert "matrix: ${{ steps.select.outputs.matrix }}" in selector
    assert "count: ${{ steps.select.outputs.count }}" in selector

    assert "BACKFILL_DEFINITIONS" in workflow
    for backfill_id in (
        "missed_crawls",
        "player_game_stats",
        "sh_sf",
        "advanced_stats",
        "player_ids",
        "roster",
    ):
        assert f'"id":"{backfill_id}"' in workflow

    assert "needs: select-backfills" in backfill
    assert "if: ${{ needs.select-backfills.outputs.count != '0' }}" in backfill
    assert "matrix: ${{ fromJson(needs.select-backfills.outputs.matrix) }}" in backfill
    assert "Check Matrix Dispatch" not in workflow
    assert "steps.should_run.outputs.run" not in workflow
    assert backfill.index(f"uses: {NODE24_CHECKOUT_REF}") < backfill.index("uses: ./.github/actions/kbo-job-setup")


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
    local_actions = (
        "uses: ./.github/actions/kbo-job-setup",
        "uses: ./.github/actions/python-env",
        "uses: ./.github/actions/notify",
    )

    for path in _workflow_files():
        for job_name, job_block in _job_blocks(_read(path)):
            local_positions = [job_block.find(action) for action in local_actions if action in job_block]
            if not local_positions:
                continue

            step_lines = [
                line.strip()
                for line in job_block.splitlines()
                if line.strip().startswith("- uses:") or line.strip().startswith("- name:")
            ]
            assert step_lines, f"{path.name}:{job_name} has no steps"

            assert step_lines[0] == f"- uses: {NODE24_CHECKOUT_REF}", (
                f"{path.name}:{job_name} must start with {NODE24_CHECKOUT_REF}"
            )
            first_checkout = job_block.find(f"uses: {NODE24_CHECKOUT_REF}")
            first_local_action = min(local_positions)
            assert first_checkout < first_local_action, f"{path.name}:{job_name} local action before checkout"

    python_env = _read(ROOT / ".github/actions/python-env/action.yml")
    assert "actions/checkout" not in python_env

    kbo_setup = _read(ROOT / ".github/actions/kbo-job-setup/action.yml")
    assert "actions/checkout" not in kbo_setup


def test_kbo_job_setup_hydration_with_resolve_date_requires_explicit_inputs():
    for path in _workflow_files():
        for job_name, job_block in _job_blocks(_read(path)):
            for setup_block in _kbo_job_setup_blocks(job_block):
                if "resolve-date: 'true'" not in setup_block or "hydrate: 'true'" not in setup_block:
                    continue

                assert "hydrate-year:" in setup_block, (
                    f"{path.name}:{job_name} kbo-job-setup hydration runs before date resolution; "
                    "pass explicit hydrate-year or move hydration after setup"
                )
                assert "hydrate-date:" in setup_block, (
                    f"{path.name}:{job_name} kbo-job-setup hydration runs before date resolution; "
                    "pass explicit hydrate-date or move hydration after setup"
                )


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

    assert "python3 -m src.cli.daily_preview_batch" in workflow
    assert "python3 -m src.cli.hydrate_runtime_from_oci" in workflow
    assert "steps.job-setup.outputs.KST_DATE" in workflow
    assert "steps.job-setup.outputs.KST_YEAR" in workflow
    assert ".github/actions/kbo-job-setup" in workflow
    assert "resolve-date: 'true'" in workflow
    assert "if: always()" in workflow


def test_pitcher_backfill_uses_correct_cli_and_hydration():
    workflow = _read(WORKFLOW_DIR / "pitcher_backfill.yml")
    jobs = dict(_job_blocks(workflow))
    job_block = jobs["backfill-pitchers"]
    setup_idx = job_block.index("uses: ./.github/actions/kbo-job-setup")
    hydrate_idx = job_block.index("- name: Hydrate Runtime Cache From OCI")
    run_idx = job_block.index("- name: Run Pregame Backfill")
    setup_block = job_block[setup_idx:hydrate_idx]
    hydrate_block = job_block[hydrate_idx:run_idx]

    assert "python3 -m src.cli.backfill_pregame_previews" in workflow
    assert '--days-ahead "${DAYS_AHEAD}"' in workflow
    assert "DAYS_AHEAD" in workflow
    assert ".github/actions/kbo-job-setup" in job_block
    assert "resolve-date: 'true'" in setup_block
    assert "hydrate: 'true'" not in setup_block
    assert "python3 -m src.cli.hydrate_runtime_from_oci" in hydrate_block
    assert "steps.job-setup.outputs.KST_YEAR" in hydrate_block
    assert "steps.job-setup.outputs.KST_DATE" in hydrate_block
    assert setup_idx < hydrate_idx < run_idx


def test_security_audit_uses_pip_audit():
    workflow = _read(WORKFLOW_DIR / "security_audit.yml")

    assert "pip-audit" in workflow
    assert "--requirement requirements.txt" in workflow
    assert "--desc on" in workflow
    assert "continue-on-error: true" in workflow
    assert "timeout-minutes: 10" in workflow
    assert "Dependency Security Audit" in workflow
    assert "actions/setup-python@v6" in workflow


def test_test_suite_runs_lint_and_test_matrix():
    workflow = _read(WORKFLOW_DIR / "test_suite.yml")

    assert "ruff check --output-format=github src/ tests/ scripts/ 2>&1" in workflow
    assert "ruff format --check src/ tests/ scripts/ 2>&1" in workflow
    assert "scripts/lint_bare_except.py" in workflow
    assert "pytest --tb=short -v --durations=10" in workflow
    assert "matrix:" in workflow
    assert 'python-version: ["3.12"]' in workflow
    assert "cancel-in-progress: false" in workflow
    assert "concurrency:" in workflow
    assert "timeout-minutes: 3" in workflow
    assert "--exit-zero" not in workflow
    assert "continue-on-error" not in workflow
    assert "|| true" not in workflow

    pytest_config = _read(ROOT / "pytest.ini")
    assert "error::pytest.PytestUnraisableExceptionWarning" in pytest_config

    jobs = dict(_job_blocks(workflow))
    assert "lint" in jobs
    assert "test" in jobs
    assert workflow.index("  lint:\n") < workflow.index("  test:\n")


def test_docker_build_has_full_build_chain():
    workflow = _read(WORKFLOW_DIR / "docker_build.yml")

    assert "docker/setup-qemu-action@v4" in workflow
    assert "docker/setup-buildx-action@v4" in workflow
    assert "docker/login-action@v4" in workflow
    assert "docker/metadata-action@v6" in workflow
    assert "docker/build-push-action@v7" in workflow
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
    assert 'python3 -m src.cli.monthly_unified_audit --year "$PREV_YEAR"' in workflow
    assert 'python3 -m src.cli.monthly_unified_audit --year "$YEAR"' in workflow
    assert workflow.index("Run Periodic Extras & Sync") < workflow.index("Monthly Unified Audit")


def test_full_recalculation_full_pipeline():
    workflow = _read(WORKFLOW_DIR / "full_recalculation.yml")

    assert "python3 -m src.cli.recalc_season_stats" in workflow
    assert "--year ${{ github.event.inputs.year }}" in workflow
    assert "--series ${{ github.event.inputs.series }}" in workflow
    assert "--save" in workflow
    assert "python3 -m src.cli.recalc_player_game_stats" in workflow
    assert "--season ${{ github.event.inputs.year }}" in workflow
    assert "python3 -m src.cli.sync_oci --kbo-season --season-stats --player-game-stats" in workflow
    assert "${{ github.event.inputs.sync == 'true' }}" in workflow
    assert "python3 -m scripts.verification.verify_player_game_stats --exit-code" in workflow
    assert "if: always()" in workflow
    assert "concurrency:" in workflow
