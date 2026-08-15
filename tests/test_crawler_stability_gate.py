from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "verification" / "crawler_stability_gate.sh"


def _usable_bash() -> bool:
    bash_bin = shutil.which("bash")
    if not bash_bin:
        return False
    try:
        return subprocess.run([bash_bin, "-c", "exit 0"], check=False).returncode == 0
    except OSError:
        return False


def test_crawler_stability_gate_prints_expected_targets():
    if not _usable_bash():
        pytest.skip("requires a usable bash executable")

    result = subprocess.run(
        ["bash", str(SCRIPT), "--print-targets"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    targets = result.stdout.strip().splitlines()

    assert targets == [
        "tests/test_schedule_crawler_stability.py",
        "tests/test_schedule_collection_service.py",
        "tests/test_schedule_season_id_mapping.py",
        "tests/test_game_detail_crawler_stability.py",
        "tests/test_game_detail_crawler_roster_fallback.py",
        "tests/test_request_throttle.py",
        "tests/test_playwright_retry.py",
        "tests/test_naver_relay_resolver.py",
        "tests/test_relay_recovery_service.py",
        "tests/test_relay_recovery.py",
        "tests/test_run_daily_update.py",
        "tests/test_p0_readiness.py",
        "tests/test_broadcast_crawler.py",
        "tests/test_roster_transaction_crawler.py",
        "tests/test_retry_daily_failures.py",
        "tests/test_crawler_live_smoke.py",
        "tests/test_crawler_release_check.py",
        "tests/test_refresh_manifest.py",
        "tests/test_scheduler_alerting.py",
        "tests/test_game_collection_service.py",
        "tests/test_fixture_ingest_clis.py",
        "tests/test_game_id_normalization.py",
    ]
    assert len(targets) == len(set(targets))


def test_crawler_stability_gate_propagates_test_runner_failure():
    false_bin = shutil.which("false")
    if not false_bin or not _usable_bash():
        pytest.skip("requires usable bash and false commands")

    env = {**os.environ, "CRAWLER_STABILITY_PYTHON": false_bin}
    result = subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert f"Python: {false_bin}" in result.stdout
