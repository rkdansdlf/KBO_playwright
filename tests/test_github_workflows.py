from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_DIR = ROOT / ".github/workflows"
ACTION_DIR = ROOT / ".github/actions"

#: Lowest major of each action the Node 24 line requires. These are floors, not
#: pins: an exact `== "actions/checkout@v5"` assertion made every Dependabot
#: major bump fail the very test that is supposed to allow it, so the PR could
#: never merge. What the contract actually protects is the floor (Node 24
#: support), not the particular number.
MIN_ACTION_MAJORS = {
    "actions/checkout": 5,
    "actions/setup-python": 6,
    "actions/cache": 5,
    "actions/upload-artifact": 4,
    "actions/download-artifact": 4,
}

_USES_REF = re.compile(r"uses:\s*([\w.\-/]+)@v(\d+)")


def _workflow_files() -> list[Path]:
    return sorted(WORKFLOW_DIR.glob("*.yml"))


def _github_action_files() -> list[Path]:
    return sorted(ACTION_DIR.glob("*/action.y*ml"))


def _github_ci_files() -> list[Path]:
    return _workflow_files() + _github_action_files()


def _joined_ref(*parts: str) -> str:
    return "".join(parts)


def _action_majors(config: str) -> dict[str, set[int]]:
    """Map each pinned action to the major versions `config` references."""
    majors: dict[str, set[int]] = {}
    for action, major in _USES_REF.findall(config):
        majors.setdefault(action, set()).add(int(major))
    return majors


def _run_script_lines(config: str) -> list[str]:
    """Return the lines a composite action actually executes.

    Comments are stripped first. A contract that greps the raw text can be
    satisfied by a line that explains the gate without ever running it.
    """
    return [line for line in (raw.strip() for raw in config.splitlines()) if line and not line.startswith("#")]


def _assert_major_at_least(config: str, action: str, where: str) -> None:
    """Assert every `uses: <action>@vN` in `config` is at or above the floor.

    Raises when the action is absent, so a workflow that quietly stops using it
    still fails loudly instead of passing on a vacuous loop.
    """
    floor = MIN_ACTION_MAJORS[action]
    majors = _action_majors(config).get(action)
    assert majors, f"{where} does not pin {action} to a major version"
    below = sorted(major for major in majors if major < floor)
    assert not below, f"{where} pins {action} below the required v{floor}: found {sorted(majors)}"


def _action_ref(config: str, action: str, where: str) -> str:
    """Return the single `action@vN` reference `config` pins, for ordering checks."""
    majors = _action_majors(config).get(action)
    assert majors, f"{where} does not pin {action} to a major version"
    assert len(majors) == 1, f"{where} pins {action} at several majors: {sorted(majors)}"
    return f"{action}@v{majors.pop()}"


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

    assert "python3 -m src.cli.kbo workflow" in workflow or "python3 -m src.cli.run_daily_update" in workflow
    assert "daily_sync" in workflow or "--fix" in workflow
    assert "--source-url-env" not in workflow


def test_smart_polling_forced_gate_overrides_polling_outputs():
    workflow = _read(WORKFLOW_DIR / "kbo_smart_polling.yml")

    assert "id: force_gate" in workflow
    assert "if: ${{ github.event.inputs.skip_gate != 'true' }}" in workflow
    assert "steps.force_gate.outputs.should_proceed || steps.gate.outputs.should_proceed" in workflow
    assert "steps.force_gate.outputs.has_games || steps.gate.outputs.has_games" in workflow
    assert "steps.force_gate.outputs.reason || steps.gate.outputs.reason" in workflow


def test_daily_kbo_sync_runs_scoped_regression_pack_with_artifacts():
    workflow = _read(WORKFLOW_DIR / "daily_kbo_sync.yml")

    assert "Data Quality Regression Pack (local, preflight)" in workflow
    assert "Data Quality Regression Pack (local, post-run)" in workflow
    assert "--require-schema" in workflow
    assert '--output "$RUNNER_TEMP/data_quality_regression_local.json"' in workflow
    assert '--output "$RUNNER_TEMP/data_quality_regression_postrun.json"' in workflow
    assert "Upload Data Quality Regression Artifacts" in workflow
    _assert_major_at_least(workflow, "actions/upload-artifact", "daily_kbo_sync.yml")


def test_daily_kbo_sync_includes_quality_and_gap_report():
    workflow = _read(WORKFLOW_DIR / "daily_kbo_sync.yml")

    assert "Generate Unified Quality Report" in workflow or "Generate Quality Report" in workflow
    assert "Run Gap Report" in workflow


def test_daily_kbo_sync_includes_quality_checks():
    workflow = _read(WORKFLOW_DIR / "daily_kbo_sync.yml")

    # The advanced daily run moved into the scheduled daily pipeline; the gates stay
    # here. See test_advanced_daily_is_not_run_by_any_workflow.
    assert "Run Advanced Daily" not in workflow
    assert "Reference Integrity Gate" in workflow
    assert "Quality Gate" in workflow
    assert "Completeness Audit" in workflow
    assert "Freshness Gate (Extended Window)" in workflow
    assert "--days 14" in workflow


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
    _assert_major_at_least(python_env, "actions/setup-python", "python-env/action.yml")
    _assert_major_at_least(python_env, "actions/cache", "python-env/action.yml")

    security_audit = _read(WORKFLOW_DIR / "security_audit.yml")
    _assert_major_at_least(security_audit, "actions/setup-python", "security_audit.yml")

    test_suite = _read(WORKFLOW_DIR / "test_suite.yml")
    assert 'FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: "true"' in test_suite


def test_github_ci_uses_supported_maintenance_modules():
    python_env = _read(ACTION_DIR / "python-env/action.yml")
    daily = _read(WORKFLOW_DIR / "daily_kbo_sync.yml")
    backfill = _read(WORKFLOW_DIR / "backfill.yml")

    assert "python3 -m scripts.maintenance.seed_data" in python_env
    assert (
        "python3 -m src.cli.kbo maintenance" in daily
        or "python3 -m scripts.maintenance.resolve_null_player_ids_conservative" in daily
    )
    assert "from scripts.maintenance.backfill_sh_sf_from_pbp import" in backfill
    assert "python3 -m scripts.maintenance.resolve_null_player_ids_conservative" in backfill
    assert "from scripts.maintenance.backfill_roster_movements import" in backfill


def test_backfill_advanced_stats_uses_supported_cli_flags():
    workflow = _read(WORKFLOW_DIR / "backfill.yml")

    assert "python3 -m src.cli.backfill_advanced_stats" in workflow
    assert '--years "$YEAR"' in workflow
    assert "--series regular" in workflow
    assert "python3 -m src.cli.sync_oci" not in workflow
    assert "--season-stats" not in workflow
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
    assert backfill.index(f"uses: {_action_ref(backfill, 'actions/checkout', 'backfill.yml')}") < backfill.index(
        "uses: ./.github/actions/kbo-job-setup"
    )


def test_kbo_automation_recalc_stats_uses_supported_cli_flags_without_sync():
    workflow = _read(WORKFLOW_DIR / "kbo_automation.yml")
    recalc_start = workflow.index("recalc-stats)")
    recalc_block = workflow[recalc_start : workflow.index(";;", recalc_start)]

    assert (
        "python3 -m src.cli.kbo maintenance" in recalc_block
        or "python3 -m src.cli.backfill_advanced_stats" in recalc_block
    )
    assert "python3 -m src.cli.sync_oci" not in recalc_block


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

            checkout_ref = _action_ref(_read(path), "actions/checkout", path.name)
            assert step_lines[0] == f"- uses: {checkout_ref}", f"{path.name}:{job_name} must start with {checkout_ref}"
            first_checkout = job_block.find(f"uses: {checkout_ref}")
            first_local_action = min(local_positions)
            assert first_checkout < first_local_action, f"{path.name}:{job_name} local action before checkout"

    python_env = _read(ROOT / ".github/actions/python-env/action.yml")
    assert "actions/checkout" not in python_env

    kbo_setup = _read(ROOT / ".github/actions/kbo-job-setup/action.yml")
    assert "actions/checkout" not in kbo_setup


def test_kbo_job_setup_has_no_hydration_contract():
    for path in _workflow_files():
        for job_name, job_block in _job_blocks(_read(path)):
            for setup_block in _kbo_job_setup_blocks(job_block):
                assert "hydrate" not in setup_block


def test_daily_kbo_sync_does_not_hydrate_fresh_runner_jobs():
    workflow = _read(WORKFLOW_DIR / "daily_kbo_sync.yml")
    jobs = dict(_job_blocks(workflow))

    assert "needs: [finalize, post-process]" in jobs["quality"]
    assert "needs: [finalize, quality]" in jobs["daily-extras"]

    for job_name in ("post-process", "quality", "daily-extras"):
        job_block = jobs[job_name]
        assert "hydrate" not in job_block

    assert "python3 -m scripts.verification.verify_player_game_stats \\" in workflow
    assert "--date ${{ needs.finalize.outputs.date }}" in workflow
    assert "--exit-code" in workflow


def test_daily_preview_uses_correct_cli_without_hydration():
    workflow = _read(WORKFLOW_DIR / "daily_preview.yml")

    assert "python3 -m src.cli.daily_preview_batch" in workflow
    assert "hydrate_runtime_from_oci" not in workflow
    assert "steps.job-setup.outputs.KST_DATE" in workflow
    assert "steps.job-setup.outputs.KST_YEAR" not in workflow
    assert ".github/actions/kbo-job-setup" in workflow
    assert "resolve-date: 'true'" in workflow
    assert "if: always()" in workflow


def test_pitcher_backfill_uses_correct_cli_without_hydration():
    workflow = _read(WORKFLOW_DIR / "pitcher_backfill.yml")
    jobs = dict(_job_blocks(workflow))
    job_block = jobs["backfill-pitchers"]
    setup_idx = job_block.index("uses: ./.github/actions/kbo-job-setup")
    run_idx = job_block.index("- name: Run Pregame Backfill")
    setup_block = job_block[setup_idx:run_idx]

    assert "python3 -m src.cli.backfill_pregame_previews" in workflow
    assert '--days-ahead "${DAYS_AHEAD}"' in workflow
    assert "DAYS_AHEAD" in workflow
    assert ".github/actions/kbo-job-setup" in job_block
    assert "resolve-date: 'true'" in setup_block
    assert "hydrate" not in setup_block
    assert "hydrate_runtime_from_oci" not in job_block
    assert setup_idx < run_idx


def test_security_audit_uses_pip_audit():
    workflow = _read(WORKFLOW_DIR / "security_audit.yml")

    assert "pip-audit" in workflow
    assert "--local --desc on" in workflow
    assert "timeout-minutes: 10" in workflow
    assert "Dependency Security Audit" in workflow
    _assert_major_at_least(workflow, "actions/setup-python", "security_audit.yml")


def test_python_env_rejects_a_lockfile_that_drifted_from_pyproject():
    """`uv sync --frozen` does not check the lock still matches pyproject.

    It only refuses to update the lock, so a bump applied to pyproject alone
    installed the stale locked version and every job stayed green with the
    change silently absent. `--check` is what turns that into a failure.
    """
    steps = _run_script_lines(_read(ACTION_DIR / "python-env/action.yml"))
    check = [i for i, line in enumerate(steps) if line.strip() == "uv lock --check"]
    install = [i for i, line in enumerate(steps) if line.strip().startswith("uv sync --frozen")]

    assert check, "no lock drift gate: a pyproject-only bump installs silently"
    assert install, "expected the lockfile install to still be there"
    assert min(check) < min(install), "the check has to run before the install, or the stale lock is already used"


def test_docker_build_cannot_hang_without_a_timeout():
    """A build with no cap has to stop on its own.

    `docker_build` ran with neither a `timeout-minutes` nor a cache bound, and
    the job sat in progress for hours after the push had finished. Every run
    since 2026-09-16 either timed out on GitHub's side or was cancelled
    manually, so nothing was reaching ghcr.io.

    `mode=max` is the other half: it exports every intermediate layer of a
    Playwright image, which is what pushed the export past the cache quota.
    """
    workflow = _read(WORKFLOW_DIR / "docker_build.yml")
    steps = _run_script_lines(workflow)

    assert "timeout-minutes: 120" in workflow, "the build job has no time cap"
    assert "cache-to: type=gha,mode=max" not in workflow, (
        "mode=max exports every intermediate layer and overruns the cache quota"
    )
    # A single amd64 image needs no emulation; QEMU is setup cost only.
    assert not [line for line in steps if "setup-qemu-action" in line], (
        "QEMU is only needed for a multi-platform build, and this one is single-arch"
    )


def test_database_workflows_notify_on_failure():
    """A database outage has to reach someone.

    Both Oracle workflows ran without a notify step, so
    `oci_live_verification` failed every Sunday from 2026-08-23 onward
    without an alert. The condition the job exists to detect is the one
    that was going unreported, which is what let it run six weeks.
    """
    for name in ("oci_live_verification.yml", "oci_connection_probe.yml"):
        workflow = _read(WORKFLOW_DIR / name)
        steps = _run_script_lines(workflow)
        notify = [i for i, line in enumerate(steps) if line.endswith("uses: ./.github/actions/notify")]

        assert notify, f"{name} cannot report a failure"
        # The `if:` belongs to the notify step, so it is written after the
        # `uses:` line. Reading the pair rather than searching the whole file
        # keeps a stray `if: always()` elsewhere from standing in for it.
        after = steps[notify[0] + 1 : notify[0] + 3]
        assert any(line == "if: always()" for line in after), (
            f"{name}: notify is skipped on failure, so only green runs would report"
        )


def test_security_audit_installs_the_locked_runtime():
    """The audit has to see the dependency set every other job installs.

    A plain `pip install` re-resolves the version ranges, so this job could pass
    on a set nothing else ever used. It installed soupsieve 2.10 while the
    lockfile pinned 2.8.4, which hid two ReDoS advisories that `pip-audit
    --local` reports against the locked version.
    """
    workflow = _read(WORKFLOW_DIR / "security_audit.yml")
    steps = _run_script_lines(workflow)

    assert not [line for line in steps if line.startswith("pip install .")], (
        "a plain pip install re-resolves the ranges and audits a set nothing else installs"
    )
    sync = [line for line in steps if line.startswith("uv sync")]
    assert sync, "expected the locked install"
    # The dev extras would make the report cover tools the runtime never ships.
    assert all("--no-dev" in line for line in sync), "the audit must cover the runtime set only"


def test_security_audit_fails_on_unallowlisted_vulnerabilities():
    """A dependency finding must fail the job, including on pull requests."""
    workflow = _read(WORKFLOW_DIR / "security_audit.yml")

    # The old workflow set `continue-on-error: true`, which let a known-vulnerable
    # dependency report a green run. The allowlist file is now the only escape hatch.
    assert "continue-on-error" not in workflow
    assert "pull_request" in workflow
    assert "pyproject.toml" in workflow
    assert ".github/security-audit-allowlist.txt" in workflow
    assert "--ignore-vuln" in workflow


def test_security_audit_allowlist_is_documented():
    allowlist = (ROOT / ".github" / "security-audit-allowlist.txt").read_text()

    assert "advisory id per line" in allowlist.lower()
    # Every non-comment entry must carry a reason and a review date.
    for line in allowlist.splitlines():
        entry = line.split("#", 1)[0].strip()
        if not entry:
            continue
        assert "#" in line, f"allowlist entry without a reason: {line}"
        assert any(ch.isdigit() for ch in line), f"allowlist entry without a review date: {line}"


def test_test_suite_runs_lint_and_test_matrix():
    workflow = _read(WORKFLOW_DIR / "test_suite.yml")

    assert "ruff check --output-format=github src/ tests/ scripts/ tools/ 2>&1" in workflow
    assert "ruff format --check src/ tests/ scripts/ tools/ 2>&1" in workflow
    assert "scripts/lint_bare_except.py" in workflow
    assert "scripts/lint_unreachable_code.py" in workflow
    assert "scripts/lint_notification_layering.py" in workflow
    assert "pytest --tb=short -v --durations=10" in workflow
    assert "if line_rate < 75:" in workflow
    assert "migration-apply" in workflow
    assert "apply_postgres_migrations" in workflow
    assert "integration-test-postgres" in workflow
    assert "image: postgres:16" in workflow
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
    assert "migration-apply" in jobs
    assert "integration-test" in jobs
    assert "integration-test-postgres" in jobs
    assert "OCI_DB_URL" not in jobs["migration-apply"]
    assert "needs: test" in jobs["integration-test"]
    assert "needs: test" in jobs["integration-test-postgres"]
    assert workflow.index("  lint:\n") < workflow.index("  test:\n")

    migration_job = jobs["migration-apply"]
    assert "Initialize PostgreSQL ORM baseline schema" in migration_job
    assert "from src.db.engine import init_db; init_db()" in migration_job
    assert "Verify PostgreSQL ORM baseline views" in migration_job
    assert "vw_player_season_batting_recalc" in migration_job
    assert migration_job.index("Initialize PostgreSQL ORM baseline schema") < migration_job.index(
        "Verify PostgreSQL ORM baseline views"
    )
    assert migration_job.index("Verify PostgreSQL ORM baseline views") < migration_job.index(
        "Apply PostgreSQL incremental migrations",
    )
    assert migration_job.index("Apply PostgreSQL incremental migrations") < migration_job.index(
        "Reapply PostgreSQL migrations",
    )
    assert migration_job.index("Reapply PostgreSQL migrations") < migration_job.index(
        "Check PostgreSQL migrations are current",
    )


def test_oci_live_verification_uses_dedicated_target_and_smoke_gates():
    workflow = _read(WORKFLOW_DIR / "oci_live_verification.yml")

    assert "OCI_DB_URL: ${{ secrets.OCI_DB_URL }}" in workflow
    assert "DATABASE_URL:" not in workflow
    assert "init-db: 'false'" in workflow
    assert "--include-safety-gated" in workflow
    assert "scripts/verification/audit_oracle_schema.py" in workflow
    assert "pytest tests/test_oracle_smoke.py -m oci -q -o addopts=''" in workflow
    assert workflow.index("Apply Oracle migrations") < workflow.index("Reapply Oracle migrations")
    assert workflow.index("Reapply Oracle migrations") < workflow.index("Check Oracle migrations")
    assert workflow.index("Check Oracle migrations") < workflow.index("Run Oracle repository smoke tests")
    assert workflow.index("Run Oracle repository smoke tests") < workflow.index("Run Live Oracle E2E Verification")


def test_docker_build_has_full_build_chain():
    workflow = _read(WORKFLOW_DIR / "docker_build.yml")

    assert "docker/setup-buildx-action@v4" in workflow
    assert "docker/login-action@v4" in workflow
    assert "docker/metadata-action@v6" in workflow
    assert "docker/build-push-action@v7" in workflow
    assert "ghcr.io" in workflow
    assert "secrets.GITHUB_TOKEN" in workflow
    assert "packages: write" in workflow
    assert "type=gha" in workflow

    # QEMU used to be first in this list. It is dropped now: the build declares
    # no `platforms:`, so it produces a single amd64 image and the emulator is
    # setup cost with nothing to emulate.
    step_order = [
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
    assert "--sync" not in workflow
    assert "YOUTUBE_API_KEY" in workflow
    assert "NAVER_CLIENT_ID" in workflow
    assert "NAVER_CLIENT_SECRET" in workflow
    assert "OCI_DB_URL" not in workflow
    assert workflow.index("Run Weekly Maintenance") < workflow.index("uses: ./.github/actions/notify")


def test_periodic_extras_runs_unified_audit_twice():
    workflow = _read(WORKFLOW_DIR / "periodic_extras.yml")

    assert "python3 -m src.cli.run_periodic_extras" in workflow
    assert "--year" in workflow
    assert "--sync" not in workflow
    assert 'python3 -m src.cli.monthly_unified_audit --year "$PREV_YEAR"' in workflow
    assert 'python3 -m src.cli.monthly_unified_audit --year "$YEAR"' in workflow
    assert workflow.index("Run Periodic Extras") < workflow.index("Monthly Unified Audit")


def test_full_recalculation_full_pipeline():
    workflow = _read(WORKFLOW_DIR / "full_recalculation.yml")

    assert "python3 -m src.cli.recalc_season_stats" in workflow
    assert "--year ${{ github.event.inputs.year }}" in workflow
    assert "--series ${{ github.event.inputs.series }}" in workflow
    assert "--save" in workflow
    assert "python3 -m src.cli.recalc_player_game_stats" in workflow
    assert "--season ${{ github.event.inputs.year }}" in workflow
    assert "python3 -m src.cli.sync_oci" not in workflow
    assert "github.event.inputs.sync" not in workflow
    assert "python3 -m scripts.verification.verify_player_game_stats --exit-code" in workflow
    assert "if: always()" in workflow
    assert "concurrency:" in workflow


def test_advanced_daily_is_not_run_by_any_workflow():
    """The advanced daily work belongs to the scheduled pipeline, not to a workflow.

    GitHub Actions ran ``run_advanced_daily`` while the canonical scheduler did not,
    which is how ``team_season_*`` froze for months and the quality gate failed on every
    run. Re-adding a workflow invocation reintroduces two divergent pipelines; the
    scheduler step is covered by the daily-update DAG tests.
    """
    for path in _workflow_files():
        assert "run_advanced_daily" not in _read(path)


def test_daily_extras_keeps_the_work_the_scheduler_does_not_cover():
    """Guard against deleting this job as "redundant" — these steps have no other path."""
    jobs = dict(_job_blocks(_read(WORKFLOW_DIR / "daily_kbo_sync.yml")))
    job_block = jobs["daily-extras"]

    for command in (
        "src.cli.crawl_milestones",
        "src.cli.crawl_player_splits",
        "src.cli.crawl_player_drafts",
        "src.cli.send_today_pregame_alerts",
        "src.cli.send_milestone_daily_summary",
    ):
        assert command in job_block

    assert "src.cli.run_advanced_daily" not in job_block


def test_daily_sync_has_no_legacy_oracle_step_or_wallet_secrets():
    """The Oracle-era sync step had no target URL and the wallet vars had no reader."""
    workflow = _read(WORKFLOW_DIR / "daily_kbo_sync.yml")

    assert "kbo sync --apply" not in workflow
    assert "sync_sqlite_to_oci" not in workflow
    assert "ORACLE_WALLET_B64" not in workflow
    assert "OCI_WALLET_PASSWORD" not in workflow
