from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "verification" / "crawler_stability_gate.sh"


def test_crawler_stability_gate_prints_expected_targets():
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
        "tests/test_oci_sync_dirty_detection.py",
        "tests/test_run_daily_update.py",
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
    assert false_bin

    env = {**os.environ, "CRAWLER_STABILITY_PYTHON": false_bin}
    result = subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert f"Python: {false_bin}" in result.stdout
